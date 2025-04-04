from dataclasses import dataclass, field
import asyncio, shutil, duckdb, time

from . import _utils as u, _connection_set as cs, _models as m
from ._exceptions import InvalidInputError


@dataclass
class ModelBuilder:
    _duckdb_venv_path: str
    _conn_set: cs.ConnectionSet
    _static_models: dict[str, m.StaticModel]
    _conn_args: cs.ConnectionsArgs = field(default_factory=lambda: cs.ConnectionsArgs(".", {}, {}))
    _logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    
    def _attach_connections(self, duckdb_conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
        dialect_by_conn_name: dict[str, str] = {}
        for conn_name, conn_props in self._conn_set.get_connections_as_dict().items():
            if not isinstance(conn_props, m.ConnectionProperties):
                continue
            dialect = conn_props.dialect
            attach_uri = conn_props.attach_uri_for_duckdb
            if attach_uri is None:
                continue # skip unsupported dialects
            attach_stmt = f"ATTACH IF NOT EXISTS '{attach_uri}' AS db_{conn_name} (TYPE {dialect}, READ_ONLY)"
            u.run_duckdb_stmt(self._logger, duckdb_conn, attach_stmt, redacted_values=[attach_uri])
            dialect_by_conn_name[conn_name] = dialect
        return dialect_by_conn_name

    async def _build_models(self, duckdb_conn: duckdb.DuckDBPyConnection, select: str | None, full_refresh: bool) -> None:
        """
        Compile and construct the build models as DuckDB tables.
        """
        # Compile the build models
        models_list = self._static_models.values() if select is None else [self._static_models[select]]
        for model in models_list:
            model.compile_for_build(self._conn_args, self._static_models)

        # Find all terminal nodes
        terminal_nodes = set()
        if select is None:
            for model in models_list:
                terminal_nodes.update(model.get_terminal_nodes_for_build(set()))
            for model in models_list:
                model.confirmed_no_cycles = False
        else:
            terminal_nodes.add(select)

        # Run the build models
        coroutines = []
        for model_name in terminal_nodes:
            model = self._static_models[model_name]
            coro = model.build_model(duckdb_conn, full_refresh)
            coroutines.append(coro)
        await u.asyncio_gather(coroutines)

    async def build(self, full_refresh: bool, select: str | None, stage_file: bool) -> None:
        start = time.time()

        # Create target folder if it doesn't exist
        duckdb_path = u.Path(self._duckdb_venv_path)
        duckdb_path.parent.mkdir(parents=True, exist_ok=True)

        # Delete any existing DuckDB file if full refresh is requested
        duckdb_dev_path = u.Path(self._duckdb_venv_path + ".dev")
        duckdb_stg_path = u.Path(self._duckdb_venv_path + ".stg")

        # If the development copy is already in use, a concurrent build is not allowed
        duckdb_dev_lock_path = u.Path(self._duckdb_venv_path + ".dev.lock")
        if duckdb_dev_lock_path.exists():
            raise InvalidInputError(60, "An existing build process is already running and a concurrent build is not allowed")
        duckdb_dev_lock_path.touch(exist_ok=False)
        
        # Ensure the lock file is deleted even if an exception is raised
        try:
            # If not full refresh, create a development copy of the existing virtual data environment
            if not full_refresh:
                if duckdb_stg_path.exists():
                    duckdb_stg_path.replace(duckdb_dev_path)
                elif duckdb_path.exists():
                    shutil.copy(duckdb_path, duckdb_dev_path)
                
            self._logger.log_activity_time("creating development copy of virtual data environment", start)
        
            # Connect to DuckDB file
            duckdb_conn = u.create_duckdb_connection(duckdb_dev_path)
            
        except Exception:
            duckdb_dev_lock_path.unlink()
            raise
        
        # Sometimes code after conn.close() doesn't run (as if the python process is killed but no error is raised)
        # Using a new try block to ensure the lock file is removed before closing the connection
        try:
            # Attach connections
            self._attach_connections(duckdb_conn)

            # Construct build models
            await self._build_models(duckdb_conn, select, full_refresh)
        
        finally:
            duckdb_dev_lock_path.unlink()
            duckdb_conn.close()

        # Rename duckdb_dev_path to duckdb_path (or duckdb_stg_path if stage_file is True)
        if stage_file:
            duckdb_dev_path.replace(duckdb_stg_path)
        else:
            duckdb_dev_path.replace(duckdb_path)

        self._logger.log_activity_time("TOTAL TIME to build virtual data environment", start)
