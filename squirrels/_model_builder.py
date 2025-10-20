from dataclasses import dataclass, field
import duckdb, time

from . import _utils as u, _connection_set as cs, _models as m


@dataclass
class ModelBuilder:
    _datalake_db_path: str
    _conn_set: cs.ConnectionSet
    _static_models: dict[str, m.StaticModel]
    _conn_args: cs.ConnectionsArgs = field(default_factory=lambda: cs.ConnectionsArgs(".", {}, {}))
    _logger: u.Logger = field(default_factory=lambda: u.Logger(""))
    
    def _attach_connections(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        for conn_name, conn_props in self._conn_set.get_connections_as_dict().items():
            if not isinstance(conn_props, m.ConnectionProperties):
                continue
            attach_uri = conn_props.attach_uri_for_duckdb
            if attach_uri is None:
                continue # skip unsupported dialects
            attach_stmt = f"ATTACH IF NOT EXISTS '{attach_uri}' AS db_{conn_name} (READ_ONLY)"
            u.run_duckdb_stmt(self._logger, duckdb_conn, attach_stmt, redacted_values=[attach_uri])

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
            # await model.build_model(duckdb_conn, full_refresh)
            coro = model.build_model(duckdb_conn, full_refresh)
            coroutines.append(coro)
        await u.asyncio_gather(coroutines)

    async def build(self, full_refresh: bool, select: str | None) -> None:
        start = time.time()

        # Connect directly to DuckLake instead of attaching (supports concurrent connections)
        duckdb_conn = u.create_duckdb_connection(self._datalake_db_path)
        
        try:
            # Attach connections
            self._attach_connections(duckdb_conn)

            # Construct build models
            await self._build_models(duckdb_conn, select, full_refresh)

        finally:
            duckdb_conn.close()

        self._logger.log_activity_time("TOTAL TIME to build the Virtual Data Lake (VDL)", start)
