from dataclasses import dataclass, field
import asyncio, typing as t, shutil, duckdb, time

from . import _sources as so, _utils as u, _constants as c, _connection_set as cs, _manifest as m


@dataclass
class ModelBuilder:
    _filepath: str
    _settings: m.Settings
    _conn_set: cs.ConnectionSet
    _sources: so.Sources
    _logger: u.Logger = field(default_factory=lambda: u.Logger(""))

    def _run_duckdb_stmt(self, duckdb_conn: duckdb.DuckDBPyConnection, stmt: str, *, params: dict[str, t.Any] | None = None) -> duckdb.DuckDBPyConnection:
        self._logger.info(f"Running statement: {stmt}", extra={"data": {"params": params}})
        try:
            return duckdb_conn.execute(stmt, params)
        except duckdb.ParserException as e:
            self._logger.error(f"Failed to run statement: {stmt}", exc_info=e)
            raise e
    
    def _attach_connections(self, duckdb_conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
        dialect_by_conn_name: dict[str, str] = {}
        for conn_name, conn_props in self._conn_set.get_connections_as_dict().items():
            dialect = conn_props.dialect
            attach_uri = conn_props.attach_uri_for_duckdb
            if attach_uri is None:
                continue # skip unsupported dialects
            attach_stmt = f"ATTACH IF NOT EXISTS '{attach_uri}' AS db_{conn_name} (TYPE {dialect}, READ_ONLY)"
            self._run_duckdb_stmt(duckdb_conn, attach_stmt)
            dialect_by_conn_name[conn_name] = dialect
        return dialect_by_conn_name
    
    def _process_source(self, duckdb_conn: duckdb.DuckDBPyConnection, source: so.Source, dialect_by_conn_name: dict[str, str], full_refresh: bool) -> None:
        local_conn = duckdb_conn.cursor()

        conn_name = source.get_connection(self._settings)
        dialect = dialect_by_conn_name[conn_name]
        result = self._run_duckdb_stmt(local_conn, f"FROM (SHOW DATABASES) WHERE database_name = 'db_{conn_name}'").fetchone()
        if result is None:
            return # skip this source if connection is not attached
        
        table_name = source.get_table()
        new_table_name = source.name

        if len(source.columns) == 0:
            stmt = f"CREATE OR REPLACE TABLE {new_table_name} AS SELECT * FROM db_{conn_name}.{table_name}"
            self._run_duckdb_stmt(local_conn, stmt)
            return
        
        increasing_column = source.update_hints.increasing_column
        recreate_table = full_refresh or increasing_column is None
        if recreate_table:
            self._run_duckdb_stmt(local_conn, f"DROP TABLE IF EXISTS {new_table_name}")

        create_table_cols_clause = source.get_cols_for_create_table_stmt()
        stmt = f"CREATE TABLE IF NOT EXISTS {new_table_name} ({create_table_cols_clause})"
        self._run_duckdb_stmt(local_conn, stmt)
    
        if not recreate_table:
            if source.update_hints.selective_overwrite_value is not None:
                stmt = f"DELETE FROM {new_table_name} WHERE {increasing_column} >= $value"
                self._run_duckdb_stmt(local_conn, stmt, params={"value": source.update_hints.selective_overwrite_value})
            elif not source.update_hints.strictly_increasing:
                stmt = f"DELETE FROM {new_table_name} WHERE {increasing_column} = ({source.get_max_incr_col_query()})"
                self._run_duckdb_stmt(local_conn, stmt)
        
        max_val_of_incr_col = None
        if increasing_column is not None:
            max_val_of_incr_col_tuple = self._run_duckdb_stmt(local_conn, source.get_max_incr_col_query()).fetchone()
            max_val_of_incr_col = max_val_of_incr_col_tuple[0] if isinstance(max_val_of_incr_col_tuple, tuple) else None
            if max_val_of_incr_col is None:
                recreate_table = True

        insert_cols_clause = source.get_cols_for_insert_stmt()
        insert_on_conflict_clause = source.get_insert_on_conflict_clause()
        query = source.get_query_for_insert(dialect, conn_name, table_name, max_val_of_incr_col, full_refresh=recreate_table)
        stmt = f"INSERT INTO {new_table_name} ({insert_cols_clause}) {query} {insert_on_conflict_clause}"
        self._run_duckdb_stmt(local_conn, stmt)

    async def _build_sources(self, duckdb_conn: duckdb.DuckDBPyConnection, full_refresh: bool) -> None:
        """
        Creates the source tables as DuckDB tables for supported source connections.
        Supported connections: sqlite, postgres, mysql
        """
        dialect_by_conn_name = self._attach_connections(duckdb_conn)

        # Create tasks for all sources and run them concurrently
        tasks = [asyncio.to_thread(self._process_source, duckdb_conn, source, dialect_by_conn_name, full_refresh) for source in self._sources.sources]
        await asyncio.gather(*tasks)

    async def build(self, *, full_refresh: bool = False, stage_file: bool = False) -> None:
        start = time.time()

        # Create target folder if it doesn't exist
        target_path = u.Path(self._filepath, c.TARGET_FOLDER)
        target_path.mkdir(parents=True, exist_ok=True)

        # Delete any existing DuckDB file if full refresh is requested
        duckdb_dev_path = u.Path(target_path, c.DUCKDB_DEV_FILE)
        duckdb_stg_path = u.Path(target_path, c.DUCKDB_STG_FILE)
        duckdb_path = u.Path(target_path, c.DUCKDB_VENV_FILE)
        if not full_refresh:
            if duckdb_stg_path.exists():
                duckdb_stg_path.replace(duckdb_dev_path)
            elif duckdb_path.exists():
                shutil.copy(duckdb_path, duckdb_dev_path)
        
        try:
            # Connect to DuckDB file
            duckdb_conn = duckdb.connect(duckdb_dev_path)
            duckdb_conn.execute("SET enable_progress_bar = true")
            try:
                # Build sources
                await self._build_sources(duckdb_conn, full_refresh)
            finally:
                duckdb_conn.close()
        
            # Rename duckdb_dev_path to duckdb_path (or duckdb_stg_path if stage_file is True)
            if stage_file:
                duckdb_dev_path.replace(duckdb_stg_path)
            else:
                duckdb_dev_path.replace(duckdb_path)
    
        except Exception as e:
            # Remove the dev file if there was an error
            if duckdb_dev_path.exists():
                duckdb_dev_path.unlink()
            raise e
        
        self._logger.log_activity_time("building virtual data environment", start)
