from dotenv import dotenv_values, load_dotenv
from pathlib import Path
import asyncio, typing as t, functools as ft, shutil, json, os
import sqlglot, sqlglot.expressions, duckdb, polars as pl

from ._auth import Authenticator, AuthProviderArgs, ProviderFunctionType
from ._schemas.auth_models import CustomUserFields, AbstractUser, GuestUser, RegisteredUser
from ._schemas import response_models as rm
from ._model_builder import ModelBuilder
from ._env_vars import SquirrelsEnvVars
from ._exceptions import InvalidInputError, ConfigurationError
from ._py_module import PyModule
from . import _dashboards as d, _utils as u, _constants as c, _manifest as mf, _connection_set as cs
from . import _seeds as s, _models as m, _model_configs as mc, _model_queries as mq, _sources as so
from . import _parameter_sets as ps, _dataset_types as dr, _logging as l

T = t.TypeVar("T", bound=d.Dashboard)
M = t.TypeVar("M", bound=m.DataModel)


class SquirrelsProject:
    """
    Initiate an instance of this class to interact with a Squirrels project through Python code. For example this can be handy to experiment with the datasets produced by Squirrels in a Jupyter notebook.
    """
    
    def __init__(
        self, *, project_path: str = ".", load_dotenv_globally: bool = False,
        log_to_file: bool = False, log_level: str | None = None, log_format: str | None = None,
    ) -> None:
        """
        Constructor for SquirrelsProject class. Loads the file contents of the Squirrels project into memory as member fields.

        Arguments:
            project_path: The path to the Squirrels project file. Defaults to the current working directory.
            log_level: The logging level to use. Options are "DEBUG", "INFO", and "WARNING". Default is from SQRL_LOGGING__LOG_LEVEL environment variable or "INFO".
            log_to_file: Whether to enable logging to file(s) in the "logs/" folder with rotation and retention policies. Default is False.
            log_format: The format of the log records. Options are "text" and "json". Default is from SQRL_LOGGING__LOG_FORMAT environment variable or "text".
        """
        self._project_path = project_path

        self._env_vars_unformatted = self._load_env_vars(project_path, load_dotenv_globally)
        self._env_vars = SquirrelsEnvVars(project_path=project_path, **self._env_vars_unformatted)
        self._vdl_catalog_db_path = self._env_vars.vdl_catalog_db_path
        
        self._logger = self._get_logger(project_path, self._env_vars, log_to_file, log_level, log_format)
        self._ensure_virtual_datalake_exists(project_path, self._vdl_catalog_db_path, self._env_vars.vdl_data_path)
    
    @staticmethod
    def _load_env_vars(project_path: str, load_dotenv_globally: bool) -> dict[str, str]:
        dotenv_files = [c.DOTENV_FILE, c.DOTENV_LOCAL_FILE]
        dotenv_vars = {}
        for file in dotenv_files:
            full_path = u.Path(project_path, file)
            if load_dotenv_globally:
                load_dotenv(full_path)
            dotenv_vars.update({k: v for k, v in dotenv_values(full_path).items() if v is not None})
        return {**os.environ, **dotenv_vars}

    @staticmethod
    def _get_logger(
        filepath: str, env_vars: SquirrelsEnvVars, log_to_file: bool, log_level: str | None, log_format: str | None
    ) -> u.Logger:
        # CLI arguments take precedence over environment variables
        log_level = log_level if log_level is not None else env_vars.logging_log_level
        log_format = log_format if log_format is not None else env_vars.logging_log_format
        log_to_file = log_to_file or u.to_bool(env_vars.logging_log_to_file)
        log_file_size_mb = int(env_vars.logging_log_file_size_mb)
        log_file_backup_count = int(env_vars.logging_log_file_backup_count)
        return l.get_logger(filepath, log_to_file, log_level, log_format, log_file_size_mb, log_file_backup_count)

    @staticmethod
    def _ensure_virtual_datalake_exists(project_path: str, vdl_catalog_db_path: str, vdl_data_path: str) -> None:
        target_path = u.Path(project_path, c.TARGET_FOLDER)
        target_path.mkdir(parents=True, exist_ok=True)

        # Attempt to set up the virtual data lake with DATA_PATH if possible
        try:
            is_ducklake = vdl_catalog_db_path.startswith("ducklake:")
            
            options = f"(DATA_PATH '{vdl_data_path}')" if is_ducklake else ""
            attach_stmt = f"ATTACH '{vdl_catalog_db_path}' AS vdl {options}"
            with duckdb.connect() as conn:
                conn.execute(attach_stmt)
                # TODO: support incremental loads for build models and avoid cleaning up old files all the time
                conn.execute("CALL ducklake_expire_snapshots('vdl', older_than => now())")
                conn.execute("CALL ducklake_cleanup_old_files('vdl', cleanup_all => true)")
        
        except Exception as e:
            if "DATA_PATH parameter" in str(e):
                first_line = str(e).split("\n")[0]
                note = "NOTE: Squirrels does not allow changing the data path for an existing Virtual Data Lake (VDL)"
                raise u.ConfigurationError(f"{first_line}\n\n{note}")
            
            if is_ducklake and not any(x in vdl_catalog_db_path for x in [":sqlite:", ":postgres:", ":mysql:"]):
                extended_error = "\n- Note: if you're using DuckDB for the metadata database, only one process can connect to the VDL at a time."
            else:
                extended_error = ""
            
            raise u.ConfigurationError(f"Failed to attach Virtual Data Lake (VDL).{extended_error}") from e
    
    @ft.cached_property
    def _manifest_cfg(self) -> mf.ManifestConfig:
        return mf.ManifestIO.load_from_file(self._logger, self._project_path, self._env_vars_unformatted)
    
    @ft.cached_property
    def _seeds(self) -> s.Seeds:
        return s.SeedsIO.load_files(self._logger, self._env_vars)
    
    @ft.cached_property
    def _sources(self) -> so.Sources:
        return so.SourcesIO.load_file(self._logger, self._env_vars, self._env_vars_unformatted)
    
    @ft.cached_property
    def _build_model_files(self) -> dict[str, mq.QueryFileWithConfig]:
        return m.ModelsIO.load_build_files(self._logger, self._env_vars)
    
    @ft.cached_property
    def _dbview_model_files(self) -> dict[str, mq.QueryFileWithConfig]:
        return m.ModelsIO.load_dbview_files(self._logger, self._env_vars)
    
    @ft.cached_property
    def _federate_model_files(self) -> dict[str, mq.QueryFileWithConfig]:
        return m.ModelsIO.load_federate_files(self._logger, self._env_vars)
    
    @ft.cached_property
    def _context_func(self) -> m.ContextFunc:
        return m.ModelsIO.load_context_func(self._logger, self._project_path)
    
    @ft.cached_property
    def _dashboards(self) -> dict[str, d.DashboardDefinition]:
        return d.DashboardsIO.load_files(
            self._logger, self._project_path, self._manifest_cfg.project_variables.auth_type, self._manifest_cfg.configurables
        )
    
    @ft.cached_property
    def _conn_args(self) -> cs.ConnectionsArgs:
        proj_vars = self._manifest_cfg.project_variables.model_dump()
        conn_args = cs.ConnectionsArgs(self._project_path, proj_vars, self._env_vars_unformatted)
        return conn_args
    
    @ft.cached_property
    def _conn_set(self) -> cs.ConnectionSet:
        return cs.ConnectionSetIO.load_from_file(self._logger, self._project_path, self._manifest_cfg, self._conn_args)
    
    @ft.cached_property
    def _custom_user_fields_cls_and_provider_functions(self) -> tuple[type[CustomUserFields], list[ProviderFunctionType]]:
        user_module_path = u.Path(self._project_path, c.PYCONFIGS_FOLDER, c.USER_FILE)
        user_module = PyModule(user_module_path)
        
        # Load CustomUserFields class (adds to Authenticator.providers as side effect)
        CustomUserFieldsCls = user_module.get_func_or_class("CustomUserFields", default_attr=CustomUserFields)
        provider_functions = Authenticator.providers
        Authenticator.providers = []
        
        if not issubclass(CustomUserFieldsCls, CustomUserFields):
            raise ConfigurationError(f"CustomUserFields class in '{c.USER_FILE}' must inherit from CustomUserFields")
        
        return CustomUserFieldsCls, provider_functions
    
    @ft.cached_property
    def _auth(self) -> Authenticator:
        auth_args = AuthProviderArgs(**self._conn_args.__dict__)
        CustomUserFieldsCls, provider_functions = self._custom_user_fields_cls_and_provider_functions
        # external_only = (self._manifest_cfg.authentication.type == mf.AuthenticationType.EXTERNAL)
        return Authenticator(
            self._logger, self._env_vars, auth_args, provider_functions, 
            custom_user_fields_cls=CustomUserFieldsCls, # external_only=external_only
        )
    
    @ft.cached_property
    def _guest_user(self) -> AbstractUser:
        custom_fields = self._auth.CustomUserFields()
        return GuestUser(username="", custom_fields=custom_fields)

    @ft.cached_property
    def _admin_user(self) -> AbstractUser:
        custom_fields = self._auth.CustomUserFields()
        return RegisteredUser(username="", access_level="admin", custom_fields=custom_fields)
    
    @ft.cached_property
    def _param_args(self) -> ps.ParametersArgs:
        conn_args = self._conn_args
        return ps.ParametersArgs(**conn_args.__dict__)
    
    @ft.cached_property
    def _param_cfg_set(self) -> ps.ParameterConfigsSet:
        return ps.ParameterConfigsSetIO.load_from_file(
            self._logger, self._env_vars, self._manifest_cfg, self._seeds, self._conn_set, self._param_args
        )
    
    @ft.cached_property
    def _j2_env(self) -> u.EnvironmentWithMacros:
        env = u.EnvironmentWithMacros(self._logger, loader=u.j2.FileSystemLoader(self._project_path))

        def value_to_str(value: t.Any, attribute: str | None = None) -> str:
            if attribute is None:
                return str(value)
            else:
                return str(getattr(value, attribute))

        def join(value: list[t.Any], d: str = ", ", attribute: str | None = None) -> str:
            return d.join(map(lambda x: value_to_str(x, attribute), value))

        def quote(value: t.Any, q: str = "'", attribute: str | None = None) -> str:
            return q + value_to_str(value, attribute) + q
        
        def quote_and_join(value: list[t.Any], q: str = "'", d: str = ", ", attribute: str | None = None) -> str:
            return d.join(map(lambda x: quote(x, q, attribute), value))
        
        env.filters["join"] = join
        env.filters["quote"] = quote
        env.filters["quote_and_join"] = quote_and_join
        return env

    def close(self) -> None:
        """
        Deliberately close any open resources within the Squirrels project, such as database connections (instead of relying on the garbage collector).
        """
        self._conn_set.dispose()
        self._auth.close()

    def __exit__(self, exc_type, exc_val, traceback):
        self.close()

    
    def _add_model(self, models_dict: dict[str, M], model: M) -> None:
        if model.name in models_dict:
            raise ConfigurationError(f"Names across all models must be unique. Model '{model.name}' is duplicated")
        models_dict[model.name] = model
    

    def _get_static_models(self) -> dict[str, m.StaticModel]:
        models_dict: dict[str, m.StaticModel] = {}

        seeds_dict = self._seeds.get_dataframes()
        for key, seed in seeds_dict.items():
            self._add_model(models_dict, m.Seed(key, seed.config, seed.df, logger=self._logger, conn_set=self._conn_set))

        for source_name, source_config in self._sources.sources.items():
            self._add_model(models_dict, m.SourceModel(source_name, source_config, logger=self._logger, conn_set=self._conn_set))

        for name, val in self._build_model_files.items():
            model = m.BuildModel(name, val.config, val.query_file, logger=self._logger, conn_set=self._conn_set, j2_env=self._j2_env) 
            self._add_model(models_dict, model)

        return models_dict


    async def build(self, *, full_refresh: bool = False, select: str | None = None) -> None:
        """
        Build the Virtual Data Lake (VDL) for the Squirrels project

        Arguments:
            full_refresh: Whether to drop all tables and rebuild the VDL from scratch. Default is False.
            select: The name of a specific model to build. If None, all models are built. Default is None.
        """
        models_dict: dict[str, m.StaticModel] = self._get_static_models()
        builder = ModelBuilder(self._vdl_catalog_db_path, self._conn_set, models_dict, self._conn_args, self._logger)
        await builder.build(full_refresh, select)

    def _get_models_dict(self, always_python_df: bool) -> dict[str, m.DataModel]:
        models_dict: dict[str, m.DataModel] = self._get_static_models()
        
        for name, val in self._dbview_model_files.items():
            self._add_model(models_dict, m.DbviewModel(
                name, val.config, val.query_file, logger=self._logger, conn_set=self._conn_set, j2_env=self._j2_env
            ))
            models_dict[name].needs_python_df = always_python_df
        
        for name, val in self._federate_model_files.items():
            self._add_model(models_dict, m.FederateModel(
                name, val.config, val.query_file, logger=self._logger, conn_set=self._conn_set, j2_env=self._j2_env
            ))
            models_dict[name].needs_python_df = always_python_df
        
        return models_dict
    
    def _generate_dag(self, dataset: str) -> m.DAG:
        models_dict = self._get_models_dict(always_python_df=False)
        
        dataset_config = self._manifest_cfg.datasets[dataset]
        target_model = models_dict[dataset_config.model]
        target_model.is_target = True
        dag = m.DAG(dataset_config, target_model, models_dict, self._vdl_catalog_db_path, self._logger)
        
        return dag
    
    def _generate_dag_with_fake_target(self, sql_query: str | None, *, always_python_df: bool = False) -> m.DAG:
        models_dict = self._get_models_dict(always_python_df=always_python_df)

        if sql_query is None:
            dependencies = set(models_dict.keys())
        else:
            dependencies, parsed = u.parse_dependent_tables(sql_query, models_dict.keys())

            substitutions = {}
            for model_name in dependencies:
                model = models_dict[model_name]
                if isinstance(model, m.SourceModel) and not model.is_queryable:
                    raise InvalidInputError(400, "cannot_query_source_model", f"Source model '{model_name}' cannot be queried with DuckDB")
                if isinstance(model, m.BuildModel):
                    substitutions[model_name] = f"vdl.{model_name}"
                elif isinstance(model, m.SourceModel):
                    if model.model_config.load_to_vdl:
                        substitutions[model_name] = f"vdl.{model_name}"
                    else:
                        # DuckDB connection without load_to_vdl - reference via attached database
                        conn_name = model.model_config.get_connection()
                        table_name = model.model_config.get_table()
                        substitutions[model_name] = f"db_{conn_name}.{table_name}"
            
            sql_query = parsed.transform(
                lambda node: sqlglot.expressions.Table(this=substitutions[node.name], alias=node.alias)
                if isinstance(node, sqlglot.expressions.Table) and node.name in substitutions
                else node
            ).sql()
        
        model_config = mc.FederateModelConfig(depends_on=dependencies)
        query_file = mq.SqlQueryFile("", sql_query or "SELECT 1")
        fake_target_model = m.FederateModel(
            "__fake_target", model_config, query_file, logger=self._logger, conn_set=self._conn_set, j2_env=self._j2_env
        )
        fake_target_model.is_target = True
        dag = m.DAG(None, fake_target_model, models_dict, self._vdl_catalog_db_path, self._logger)
        return dag
    
    async def _get_compiled_dag(
        self, user: AbstractUser, *, sql_query: str | None = None, selections: dict[str, t.Any] = {}, configurables: dict[str, str] = {}, 
        always_python_df: bool = False
    ) -> m.DAG:
        dag = self._generate_dag_with_fake_target(sql_query, always_python_df=always_python_df)
        
        configurables = {**self._manifest_cfg.get_default_configurables(), **configurables}
        await dag.execute(
            self._param_args, self._param_cfg_set, self._context_func, user, selections,
            runquery=False, configurables=configurables
        )
        return dag
    
    def _get_all_connections(self) -> list[rm.ConnectionItemModel]:
        connections = []
        for conn_name, conn_props in self._conn_set.get_connections_as_dict().items():
            if isinstance(conn_props, mf.ConnectionProperties):
                label = conn_props.label if conn_props.label is not None else conn_name
                connections.append(rm.ConnectionItemModel(name=conn_name, label=label))
        return connections
    
    def _get_all_data_models(self, compiled_dag: m.DAG) -> list[rm.DataModelItem]:
        return compiled_dag.get_all_data_models()
    
    async def get_all_data_models(self) -> list[rm.DataModelItem]:
        """
        Get all data models in the project

        Returns:
            A list of DataModelItem objects
        """
        compiled_dag = await self._get_compiled_dag(self._admin_user)
        return self._get_all_data_models(compiled_dag)

    def _get_all_data_lineage(self, compiled_dag: m.DAG) -> list[rm.LineageRelation]:
        all_lineage = compiled_dag.get_all_model_lineage()

        # Add dataset nodes to the lineage
        for dataset in self._manifest_cfg.datasets.values():
            target_dataset = rm.LineageNode(name=dataset.name, type="dataset")
            source_model = rm.LineageNode(name=dataset.model, type="model")
            all_lineage.append(rm.LineageRelation(type="runtime", source=source_model, target=target_dataset))

        # Add dashboard nodes to the lineage
        for dashboard in self._dashboards.values():
            target_dashboard = rm.LineageNode(name=dashboard.dashboard_name, type="dashboard")
            datasets = set(x.dataset for x in dashboard.config.depends_on)
            for dataset in datasets:
                source_dataset = rm.LineageNode(name=dataset, type="dataset")
                all_lineage.append(rm.LineageRelation(type="runtime", source=source_dataset, target=target_dashboard))

        return all_lineage

    async def get_all_data_lineage(self) -> list[rm.LineageRelation]:
        """
        Get all data lineage in the project

        Returns:
            A list of LineageRelation objects
        """
        compiled_dag = await self._get_compiled_dag(self._admin_user)
        return self._get_all_data_lineage(compiled_dag)

    async def compile(
        self, *, selected_model: str | None = None, test_set: str | None = None, do_all_test_sets: bool = False,
        runquery: bool = False, clear: bool = False, buildtime_only: bool = False, runtime_only: bool = False
    ) -> None:
        """
        Compile models into the "target/compile" folder.

        Behavior:
        - Buildtime outputs: target/compile/buildtime/*.sql (for SQL build models) and dag.png
        - Runtime outputs: target/compile/runtime/[test_set]/dbviews/*.sql, federates/*.sql, dag.png
          If runquery=True, also write CSVs for runtime models.
        - Options: clear entire compile folder first; compile only buildtime or only runtime.

        Arguments:
            selected_model: The name of the model to compile. If specified, the compiled SQL query is also printed in the terminal. If None, all models for the selected dataset are compiled. Default is None.
            test_set: The name of the test set to compile with. If None, the default test set is used (which can vary by dataset). Ignored if `do_all_test_sets` argument is True. Default is None.
            do_all_test_sets: Whether to compile all applicable test sets for the selected dataset(s). If True, the `test_set` argument is ignored. Default is False.
            runquery: Whether to run all compiled queries and save each result as a CSV file. If True and `selected_model` is specified, all upstream models of the selected model is compiled as well. Default is False.
            clear: Whether to clear the "target/compile/" folder before compiling. Default is False.
            buildtime_only: Whether to compile only buildtime models. Default is False.
            runtime_only: Whether to compile only runtime models. Default is False.
        """
        border = "=" * 80
        underlines = "-" * len(border)

        compile_root = Path(self._project_path, c.TARGET_FOLDER, c.COMPILE_FOLDER)
        if clear and compile_root.exists():
            shutil.rmtree(compile_root)

        models_dict = self._get_models_dict(always_python_df=False)

        if selected_model is not None:
            selected_model = u.normalize_name(selected_model)
            if selected_model not in models_dict:
                print(f"No such model found: {selected_model}")
                return
            if not isinstance(models_dict[selected_model], m.QueryModel):
                print(f"Model '{selected_model}' is not a query model. Nothing to do.")
                return
        
        model_to_compile = None

        # Buildtime compilation
        if not runtime_only:
            print(underlines)
            print(f"Compiling buildtime models")
            print(underlines)

            buildtime_folder = Path(compile_root, c.COMPILE_BUILDTIME_FOLDER)
            buildtime_folder.mkdir(parents=True, exist_ok=True)

            def write_buildtime_model(model: m.DataModel, static_models: dict[str, m.StaticModel]) -> None:
                if not isinstance(model, m.BuildModel):
                    return
                
                model.compile_for_build(self._conn_args, static_models)

                if isinstance(model.compiled_query, mq.SqlModelQuery):
                    out_path = Path(buildtime_folder, f"{model.name}.sql")
                    with open(out_path, 'w') as f:
                        f.write(model.compiled_query.query)
                    print(f"Successfully compiled build model: {model.name}")
                elif isinstance(model.compiled_query, mq.PyModelQuery):
                    print(f"The build model '{model.name}' is in Python. Compilation for Python is not supported yet.")

            static_models = self._get_static_models()
            if selected_model is not None:
                model_to_compile = models_dict[selected_model]
                write_buildtime_model(model_to_compile, static_models)
            else:
                coros = [asyncio.to_thread(write_buildtime_model, m, static_models) for m in static_models.values()]
                await u.asyncio_gather(coros)
            
            print(underlines)
            print()
        
        # Runtime compilation
        if not buildtime_only:
            if do_all_test_sets:
                test_set_names_set = set(self._manifest_cfg.selection_test_sets.keys())
                test_set_names_set.add(c.DEFAULT_TEST_SET_NAME)
                test_set_names = list(test_set_names_set)
            else:
                test_set_names = [test_set or c.DEFAULT_TEST_SET_NAME]

            for ts_name in test_set_names:
                print(underlines)
                print(f"Compiling runtime models (test set '{ts_name}')")
                print(underlines)

                # Build user and selections from test set config if present
                ts_conf = self._manifest_cfg.selection_test_sets.get(ts_name, self._manifest_cfg.get_default_test_set())
                # Separate base fields from custom fields
                access_level = ts_conf.user.access_level
                custom_fields = self._auth.CustomUserFields(**ts_conf.user.custom_fields)
                if access_level == "guest":
                    user = GuestUser(username="", custom_fields=custom_fields)
                else:
                    user = RegisteredUser(username="", access_level=access_level, custom_fields=custom_fields)

                # Generate DAG across all models. When runquery=True, force models to produce Python dataframes so CSVs can be written.
                dag = await self._get_compiled_dag(
                    user=user, selections=ts_conf.parameters, configurables=ts_conf.configurables, always_python_df=runquery,
                )
                if runquery:
                    await dag._run_models()

                # Prepare output folders
                runtime_folder = Path(compile_root, c.COMPILE_RUNTIME_FOLDER, ts_name)
                dbviews_folder = Path(runtime_folder, c.DBVIEWS_FOLDER)
                federates_folder = Path(runtime_folder, c.FEDERATES_FOLDER)
                dbviews_folder.mkdir(parents=True, exist_ok=True)
                federates_folder.mkdir(parents=True, exist_ok=True)
                with open(Path(runtime_folder, "placeholders.json"), "w") as f:
                    json.dump(dag.placeholders, f)

                # Function to write runtime models
                def write_runtime_model(model: m.DataModel) -> None:
                    if not isinstance(model, m.QueryModel):
                        return
                    
                    if model.model_type not in (m.ModelType.DBVIEW, m.ModelType.FEDERATE):
                        return
                    
                    subfolder = dbviews_folder if model.model_type == m.ModelType.DBVIEW else federates_folder
                    model_type = "dbview" if model.model_type == m.ModelType.DBVIEW else "federate"

                    if isinstance(model.compiled_query, mq.SqlModelQuery):
                        out_sql = Path(subfolder, f"{model.name}.sql")
                        with open(out_sql, 'w') as f:
                            f.write(model.compiled_query.query)
                        print(f"Successfully compiled {model_type} model: {model.name}")
                    elif isinstance(model.compiled_query, mq.PyModelQuery):
                        print(f"The {model_type} model '{model.name}' is in Python. Compilation for Python is not supported yet.")
                    
                    if runquery and isinstance(model.result, pl.LazyFrame):
                        out_csv = Path(subfolder, f"{model.name}.csv")
                        model.result.collect().write_csv(out_csv)
                        print(f"Successfully created CSV for {model_type} model: {model.name}")

                # If selected_model is provided for runtime, only emit that model's outputs
                if selected_model is not None:
                    model_to_compile = dag.models_dict[selected_model]
                    write_runtime_model(model_to_compile)
                else:
                    coros = [asyncio.to_thread(write_runtime_model, model) for model in dag.models_dict.values()]
                    await u.asyncio_gather(coros)

                print(underlines)
                print()

        print(f"All compilations complete! See the '{c.TARGET_FOLDER}/{c.COMPILE_FOLDER}/' folder for results.")
        if model_to_compile and isinstance(model_to_compile, m.QueryModel) and isinstance(model_to_compile.compiled_query, mq.SqlModelQuery):
            print()
            print(border)
            print(f"Compiled SQL query for model '{model_to_compile.name}':")
            print(underlines)
            print(model_to_compile.compiled_query.query)
            print(border)
            print()

    def _permission_error(self, user: AbstractUser, data_type: str, data_name: str, scope: str) -> InvalidInputError:
        return InvalidInputError(403, f"unauthorized_access_to_{data_type}", f"User '{user}' does not have permission to access {scope} {data_type}: {data_name}")
    
    def seed(self, name: str) -> pl.LazyFrame:
        """
        Method to retrieve a seed as a polars LazyFrame given a seed name.

        Arguments:
            name: The name of the seed to retrieve

        Returns:
            The seed as a polars LazyFrame
        """
        seeds_dict = self._seeds.get_dataframes()
        try:
            return seeds_dict[name].df
        except KeyError:
            available_seeds = list(seeds_dict.keys())
            raise KeyError(f"Seed '{name}' not found. Available seeds are: {available_seeds}")
    
    def dataset_metadata(self, name: str) -> dr.DatasetMetadata:
        """
        Method to retrieve the metadata of a dataset given a dataset name.

        Arguments:
            name: The name of the dataset to retrieve.
        
        Returns:
            A DatasetMetadata object containing the dataset description and column details.
        """
        dag = self._generate_dag(name)
        dag.target_model.process_pass_through_columns(dag.models_dict)
        return dr.DatasetMetadata(
            target_model_config=dag.target_model.model_config
        )
    
    def _enforce_max_result_rows(self, lazy_df: pl.LazyFrame, error_type: str) -> pl.DataFrame:
        """
        Collect at most max_rows + 1 rows from a LazyFrame to detect overflow.
        Raises InvalidInputError if the result exceeds the maximum allowed rows.
        
        Arguments:
            lazy_df: The LazyFrame to collect and check
            error_type: Either "dataset" or "query" to customize the error message
        
        Returns:
            A DataFrame with at most max_rows rows (or raises if exceeded)
        """
        max_rows = self._env_vars.datasets_max_rows_output
        # Collect max_rows + 1 to detect overflow without loading unbounded results
        collected = lazy_df.limit(max_rows + 1).collect()
        row_count = collected.select(pl.len()).item()
        
        if row_count > max_rows:
            raise InvalidInputError(
                413, f"{error_type}_result_too_large",
                f"The {error_type} result contains {row_count} rows, which exceeds the maximum allowed of {max_rows} rows."
            )
        
        return collected
    
    async def _dataset_result(
        self, name: str, *, selections: dict[str, t.Any] = {}, user: AbstractUser | None = None,
        configurables: dict[str, str] = {}, check_user_access: bool = True
    ) -> dr.DatasetResult:
        if user is None:
            user = self._guest_user
        
        scope = self._manifest_cfg.datasets[name].scope
        if check_user_access and not self._auth.can_user_access_scope(user, scope):
            raise self._permission_error(user, "dataset", name, scope.name)
        
        dataset_config = self._manifest_cfg.datasets[name]
        configurables = {**self._manifest_cfg.get_default_configurables(overrides=dataset_config.configurables), **configurables}
        
        dag = self._generate_dag(name)
        await dag.execute(
            self._param_args, self._param_cfg_set, self._context_func, user, dict(selections), configurables=configurables
        )
        assert isinstance(dag.target_model.result, pl.LazyFrame)
        df = self._enforce_max_result_rows(dag.target_model.result, "dataset")
        return dr.DatasetResult(
            target_model_config=dag.target_model.model_config, 
            df=df.with_row_index("_row_num", offset=1)
        )
    
    async def dataset_result(
        self, name: str, *, selections: dict[str, t.Any] = {}, user: AbstractUser | None = None, configurables: dict[str, str] = {}
    ) -> dr.DatasetResult:
        """
        Async method to retrieve a dataset as a DatasetResult object (with metadata) given parameter selections.

        Arguments:
            name: The name of the dataset to retrieve.
            selections: A dictionary of parameter selections to apply to the dataset. Optional, default is empty dictionary.
            user: The user to use for authentication. If None, no user is used. Optional, default is None.
            configurables: A dictionary of configurables to apply to the dataset. Optional, default is empty dictionary.
        
        Returns:
            A DatasetResult object containing the dataset result (as a polars DataFrame), its description, and the column details.
        """
        result = await self._dataset_result(name, selections=selections, user=user, configurables=configurables, check_user_access=False)
        return result

    async def dashboard(
        self, name: str, *, selections: dict[str, t.Any] = {}, user: AbstractUser | None = None, dashboard_type: t.Type[T] = d.PngDashboard,
        configurables: dict[str, str] = {}
    ) -> T:
        """
        Async method to retrieve a dashboard given parameter selections.

        Arguments:
            name: The name of the dashboard to retrieve.
            selections: A dictionary of parameter selections to apply to the dashboard. Optional, default is empty dictionary.
            user: The user to use for authentication. If None, no user is used. Optional, default is None.
            dashboard_type: Return type of the method (mainly used for type hints). For instance, provide PngDashboard if you want the return type to be a PngDashboard. Optional, default is squirrels.Dashboard.
        
        Returns:
            The dashboard type specified by the "dashboard_type" argument.
        """
        if user is None:
            user = self._guest_user
        
        scope = self._dashboards[name].config.scope
        if not self._auth.can_user_access_scope(user, scope):
            raise self._permission_error(user, "dashboard", name, scope.name)
        
        async def get_dataset_df(dataset_name: str, fixed_params: dict[str, t.Any]) -> pl.DataFrame:
            final_selections = {**selections, **fixed_params}
            result = await self.dataset_result(
                dataset_name, selections=final_selections, user=user, configurables=configurables
            )
            return result.df
        
        dashboard_config = self._dashboards[name].config
        parameter_set = self._param_cfg_set.apply_selections(dashboard_config.parameters, selections, user)
        prms = parameter_set.get_parameters_as_dict()
        
        configurables = {**self._manifest_cfg.get_default_configurables(overrides=dashboard_config.configurables), **configurables}
        context = {}
        ctx_args = m.ContextArgs(
            **self._param_args.__dict__, user=user, prms=prms, configurables=configurables, _conn_args=self._conn_args
        )
        self._context_func(context, ctx_args)
        
        args = d.DashboardArgs(
            **ctx_args.__dict__, ctx=context, _get_dataset=get_dataset_df
        )
        try:
            return await self._dashboards[name].get_dashboard(args, dashboard_type=dashboard_type)
        except KeyError:
            raise KeyError(f"No dashboard file found for: {name}")
    
    async def query_models(
        self, sql_query: str, *, user: AbstractUser | None = None, selections: dict[str, t.Any] = {}, configurables: dict[str, str] = {}
    ) -> dr.DatasetResult:
        if user is None:
            user = self._guest_user
        
        dag = await self._get_compiled_dag(user=user, sql_query=sql_query, selections=selections, configurables=configurables)
        await dag._run_models()
        assert isinstance(dag.target_model.result, pl.LazyFrame)
        df = self._enforce_max_result_rows(dag.target_model.result, "query")
        return dr.DatasetResult(
            target_model_config=dag.target_model.model_config, 
            df=df.with_row_index("_row_num", offset=1)
        )

    async def get_compiled_model_query(
        self, model_name: str, *, user: AbstractUser | None = None, selections: dict[str, t.Any] = {}, configurables: dict[str, str] = {}
    ) -> rm.CompiledQueryModel:
        """
        Compile the specified data model and return its language and compiled definition.
        """
        if user is None:
            user = self._guest_user
        
        name = u.normalize_name(model_name)
        models_dict = self._get_models_dict(always_python_df=False)
        if name not in models_dict:
            raise InvalidInputError(404, "model_not_found", f"No data model found with name: {model_name}")

        model = models_dict[name]
        # Only build, dbview, and federate models support runtime compiled definition in this context
        if not isinstance(model, (m.BuildModel, m.DbviewModel, m.FederateModel)):
            raise InvalidInputError(400, "unsupported_model_type", "Only build, dbview, and federate models currently support compiled definition via this endpoint")

        # Build a DAG with this model as the target, without a dataset context
        model.is_target = True
        dag = m.DAG(None, model, models_dict, self._vdl_catalog_db_path, self._logger)

        cfg = {**self._manifest_cfg.get_default_configurables(), **configurables}
        await dag.execute(
            self._param_args, self._param_cfg_set, self._context_func, user, selections, runquery=False, configurables=cfg
        )

        language = "sql" if isinstance(model.query_file, mq.SqlQueryFile) else "python"
        if isinstance(model, m.BuildModel):
            # Compile SQL build models; Python build models not yet supported
            if isinstance(model.query_file, mq.SqlQueryFile):
                static_models = self._get_static_models()
                compiled = model._compile_sql_model(model.query_file, self._conn_args, static_models)
                definition = compiled.query
            else:
                definition = "# Compiling Python build models is currently not supported. This will be available in a future version of Squirrels..."
        elif isinstance(model.compiled_query, mq.SqlModelQuery):
            definition = model.compiled_query.query 
        elif isinstance(model.compiled_query, mq.PyModelQuery): 
            definition = "# Compiling Python data models is currently not supported. This will be available in a future version of Squirrels..."
        else:
            raise NotImplementedError(f"Query type not supported: {model.compiled_query.__class__.__name__}")
        
        return rm.CompiledQueryModel(language=language, definition=definition, placeholders=dag.placeholders)
    