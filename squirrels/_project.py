from dotenv import dotenv_values
from uuid import uuid4
import asyncio, typing as t, functools as ft, shutil, json, os
import logging as l, matplotlib.pyplot as plt, networkx as nx, polars as pl
import sqlglot, sqlglot.expressions

from ._auth import Authenticator, BaseUser
from ._model_builder import ModelBuilder
from ._exceptions import InvalidInputError, ConfigurationError
from . import _utils as u, _constants as c, _manifest as mf, _connection_set as cs, _api_response_models as arm
from . import _seeds as s, _models as m, _model_configs as mc, _model_queries as mq, _sources as so
from . import _parameter_sets as ps, _dashboards_io as d, dashboards as dash, dataset_result as dr

T = t.TypeVar("T", bound=dash.Dashboard)
M = t.TypeVar("M", bound=m.DataModel)


class _CustomJsonFormatter(l.Formatter):
    def format(self, record: l.LogRecord) -> str:
        super().format(record)
        info = {
            "timestamp": self.formatTime(record),
            "project_id": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
            "thread": record.thread,
            "thread_name": record.threadName,
            "process": record.process,
            **record.__dict__.get("info", {})
        }
        output = {
            "data": record.__dict__.get("data", {}),
            "info": info
        }
        return json.dumps(output)


class SquirrelsProject:
    """
    Initiate an instance of this class to interact with a Squirrels project through Python code. For example this can be handy to experiment with the datasets produced by Squirrels in a Jupyter notebook.
    """
    
    def __init__(self, *, filepath: str = ".", log_file: str | None = c.LOGS_FILE, log_level: str = "INFO", log_format: str = "text") -> None:
        """
        Constructor for SquirrelsProject class. Loads the file contents of the Squirrels project into memory as member fields.

        Arguments:
            filepath: The path to the Squirrels project file. Defaults to the current working directory.
            log_level: The logging level to use. Options are "DEBUG", "INFO", and "WARNING". Default is "INFO".
            log_file: The name of the log file to write to from the "logs/" subfolder. If None or empty string, then file logging is disabled. Default is "squirrels.log".
            log_format: The format of the log records. Options are "text" and "json". Default is "text".
        """
        self._filepath = filepath
        self._logger = self._get_logger(self._filepath, log_file, log_level, log_format)

    def _get_logger(self, base_path: str, log_file: str | None, log_level: str, log_format: str) -> u.Logger:
        logger = u.Logger(name=uuid4().hex)
        logger.setLevel(log_level.upper())

        handler = l.StreamHandler()
        handler.setLevel("WARNING")
        handler.setFormatter(l.Formatter("%(levelname)s:   %(asctime)s - %(message)s"))
        logger.addHandler(handler)
        
        if log_format.lower() == "json":
            formatter = _CustomJsonFormatter()
        elif log_format.lower() == "text":
            formatter = l.Formatter("[%(name)s] %(asctime)s - %(levelname)s - %(message)s")
        else:
            raise ValueError("log_format must be either 'text' or 'json'")
            
        if log_file:
            path = u.Path(base_path, c.LOGS_FOLDER, log_file)
            path.parent.mkdir(parents=True, exist_ok=True)

            handler = l.FileHandler(path)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    @ft.cached_property
    def _env_vars(self) -> dict[str, str]:
        dotenv_files = [c.DOTENV_FILE, c.DOTENV_LOCAL_FILE]
        dotenv_vars = {}
        for file in dotenv_files:
            dotenv_vars.update({k: v for k, v in dotenv_values(f"{self._filepath}/{file}").items() if v is not None})
        return {**os.environ, **dotenv_vars}

    @ft.cached_property
    def _manifest_cfg(self) -> mf.ManifestConfig:
        return mf.ManifestIO.load_from_file(self._logger, self._filepath, self._env_vars)
    
    @ft.cached_property
    def _seeds(self) -> s.Seeds:
        return s.SeedsIO.load_files(self._logger, self._filepath, self._env_vars)
    
    @ft.cached_property
    def _sources(self) -> so.Sources:
        return so.SourcesIO.load_file(self._logger, self._filepath, self._env_vars)
    
    @ft.cached_property
    def _build_model_files(self) -> dict[str, mq.QueryFileWithConfig]:
        return m.ModelsIO.load_build_files(self._logger, self._filepath)
    
    @ft.cached_property
    def _dbview_model_files(self) -> dict[str, mq.QueryFileWithConfig]:
        return m.ModelsIO.load_dbview_files(self._logger, self._filepath, self._env_vars)
    
    @ft.cached_property
    def _federate_model_files(self) -> dict[str, mq.QueryFileWithConfig]:
        return m.ModelsIO.load_federate_files(self._logger, self._filepath)
    
    @ft.cached_property
    def _context_func(self) -> m.ContextFunc:
        return m.ModelsIO.load_context_func(self._logger, self._filepath)
    
    @ft.cached_property
    def _dashboards(self) -> dict[str, d.DashboardDefinition]:
        return d.DashboardsIO.load_files(self._logger, self._filepath)
    
    @ft.cached_property
    def _conn_args(self) -> cs.ConnectionsArgs:
        return cs.ConnectionSetIO.load_conn_py_args(self._logger, self._filepath, self._env_vars, self._manifest_cfg)
    
    @ft.cached_property
    def _conn_set(self) -> cs.ConnectionSet:
        return cs.ConnectionSetIO.load_from_file(self._logger, self._filepath, self._manifest_cfg, self._conn_args)
    
    @ft.cached_property
    def _auth(self) -> Authenticator:
        return Authenticator(self._logger, self._filepath, self._env_vars)
    
    @ft.cached_property
    def _param_args(self) -> ps.ParametersArgs:
        return ps.ParameterConfigsSetIO.get_param_args(self._conn_args)
    
    @ft.cached_property
    def _param_cfg_set(self) -> ps.ParameterConfigsSet:
        return ps.ParameterConfigsSetIO.load_from_file(
            self._logger, self._filepath, self._manifest_cfg, self._seeds, self._conn_set, self._param_args
        )
    
    @ft.cached_property
    def _j2_env(self) -> u.EnvironmentWithMacros:
        return u.EnvironmentWithMacros(self._logger, loader=u.j2.FileSystemLoader(self._filepath))

    @ft.cached_property
    def _duckdb_venv_path(self) -> str:
        duckdb_filepath_setting_val = self._env_vars.get(c.SQRL_DUCKDB_VENV_DB_FILE_PATH, f"{c.TARGET_FOLDER}/{c.DUCKDB_VENV_FILE}")
        return str(u.Path(self._filepath, duckdb_filepath_setting_val))
    
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
            self._add_model(models_dict, m.Seed(key, seed.config, seed.df, logger=self._logger, env_vars=self._env_vars, conn_set=self._conn_set))

        for source_name, source_config in self._sources.sources.items():
            self._add_model(models_dict, m.SourceModel(source_name, source_config, logger=self._logger, env_vars=self._env_vars, conn_set=self._conn_set))

        for name, val in self._build_model_files.items():
            model = m.BuildModel(name, val.config, val.query_file, logger=self._logger, env_vars=self._env_vars, conn_set=self._conn_set, j2_env=self._j2_env) 
            self._add_model(models_dict, model)

        return models_dict


    async def build(self, *, full_refresh: bool = False, select: str | None = None, stage_file: bool = False) -> None:
        """
        Build the virtual data environment for the Squirrels project

        Arguments:
            full_refresh: Whether to drop all tables and rebuild the virtual data environment from scratch. Default is False.
            stage_file: Whether to stage the DuckDB file to overwrite the existing one later if the virtual data environment is in use. Default is False.
        """
        models_dict: dict[str, m.StaticModel] = self._get_static_models()
        builder = ModelBuilder(self._duckdb_venv_path, self._conn_set, models_dict, self._conn_args, self._logger)
        await builder.build(full_refresh, select, stage_file)

    def _get_models_dict(self, always_python_df: bool) -> dict[str, m.DataModel]:
        models_dict: dict[str, m.DataModel] = dict(self._get_static_models())
        
        for name, val in self._dbview_model_files.items():
            self._add_model(models_dict, m.DbviewModel(
                name, val.config, val.query_file, logger=self._logger, env_vars=self._env_vars, conn_set=self._conn_set, j2_env=self._j2_env
            ))
            models_dict[name].needs_python_df = always_python_df
        
        for name, val in self._federate_model_files.items():
            self._add_model(models_dict, m.FederateModel(
                name, val.config, val.query_file, logger=self._logger, env_vars=self._env_vars, conn_set=self._conn_set, j2_env=self._j2_env
            ))
            models_dict[name].needs_python_df = always_python_df
        
        return models_dict
    
    def _generate_dag(self, dataset: str, *, target_model_name: str | None = None, always_python_df: bool = False) -> m.DAG:
        models_dict = self._get_models_dict(always_python_df)
        
        dataset_config = self._manifest_cfg.datasets[dataset]
        target_model_name = dataset_config.model if target_model_name is None else target_model_name
        target_model = models_dict[target_model_name]
        target_model.is_target = True
        dag = m.DAG(dataset_config, target_model, models_dict, self._duckdb_venv_path, self._logger)
        
        return dag
    
    def _generate_dag_with_fake_target(self, sql_query: str | None) -> m.DAG:
        models_dict = self._get_models_dict(always_python_df=False)

        if sql_query is None:
            dependencies = set(models_dict.keys())
        else:
            dependencies, parsed = u.parse_dependent_tables(sql_query, models_dict.keys())

            substitutions = {}
            for model_name in dependencies:
                model = models_dict[model_name]
                if isinstance(model, m.SourceModel) and not model.model_config.load_to_duckdb:
                    raise InvalidInputError(203, f"Source model '{model_name}' cannot be queried with DuckDB")
                if isinstance(model, (m.SourceModel, m.BuildModel)):
                    substitutions[model_name] = f"venv.{model_name}"
            
            sql_query = parsed.transform(
                lambda node: sqlglot.expressions.Table(this=substitutions[node.name])
                if isinstance(node, sqlglot.expressions.Table) and node.name in substitutions
                else node
            ).sql()
        
        model_config = mc.FederateModelConfig(depends_on=dependencies)
        query_file = mq.SqlQueryFile("", sql_query or "")
        fake_target_model = m.FederateModel(
            "__fake_target", model_config, query_file, logger=self._logger, env_vars=self._env_vars, conn_set=self._conn_set, j2_env=self._j2_env
        )
        fake_target_model.is_target = True
        dag = m.DAG(None, fake_target_model, models_dict, self._duckdb_venv_path, self._logger)
        return dag
    
    def _draw_dag(self, dag: m.DAG, output_folder: u.Path) -> None:
        color_map = {
            m.ModelType.SEED: "green", m.ModelType.DBVIEW: "red", m.ModelType.FEDERATE: "skyblue",
            m.ModelType.BUILD: "purple", m.ModelType.SOURCE: "orange"
        }

        G = dag.to_networkx_graph()
        
        fig, _ = plt.subplots()
        pos = nx.multipartite_layout(G, subset_key="layer")
        colors = [color_map[node[1]] for node in G.nodes(data="model_type")] # type: ignore
        nx.draw(G, pos=pos, node_shape='^', node_size=1000, node_color=colors, arrowsize=20)
        
        y_values = [val[1] for val in pos.values()]
        scale = max(y_values) - min(y_values) if len(y_values) > 0 else 0
        label_pos = {key: (val[0], val[1]-0.002-0.1*scale) for key, val in pos.items()}
        nx.draw_networkx_labels(G, pos=label_pos, font_size=8)
        
        fig.tight_layout()
        plt.margins(x=0.1, y=0.1)
        fig.savefig(u.Path(output_folder, "dag.png"))
        plt.close(fig)
    
    async def _get_compiled_dag(self, *, sql_query: str | None = None, selections: dict[str, t.Any] = {}, user: BaseUser | None = None) -> m.DAG:
        dag = self._generate_dag_with_fake_target(sql_query)
        
        default_traits = self._manifest_cfg.get_default_traits()
        await dag.execute(self._param_args, self._param_cfg_set, self._context_func, user, selections, runquery=False, default_traits=default_traits)
        return dag
    
    def _get_all_connections(self) -> list[arm.ConnectionItemModel]:
        connections = []
        for conn_name, conn_props in self._conn_set.get_connections_as_dict().items():
            if isinstance(conn_props, mf.ConnectionProperties):
                label = conn_props.label if conn_props.label is not None else conn_name
                connections.append(arm.ConnectionItemModel(name=conn_name, label=label))
        return connections
    
    def _get_all_data_models(self, compiled_dag: m.DAG) -> list[arm.DataModelItem]:
        return compiled_dag.get_all_data_models()
    
    async def get_all_data_models(self) -> list[arm.DataModelItem]:
        """
        Get all data models in the project

        Returns:
            A list of DataModelItem objects
        """
        compiled_dag = await self._get_compiled_dag()
        return self._get_all_data_models(compiled_dag)

    def _get_all_data_lineage(self, compiled_dag: m.DAG) -> list[arm.LineageRelation]:
        all_lineage = compiled_dag.get_all_model_lineage()

        # Add dataset nodes to the lineage
        for dataset in self._manifest_cfg.datasets.values():
            target_dataset = arm.LineageNode(name=dataset.name, type="dataset")
            source_model = arm.LineageNode(name=dataset.model, type="model")
            all_lineage.append(arm.LineageRelation(type="runtime", source=source_model, target=target_dataset))

        # Add dashboard nodes to the lineage
        for dashboard in self._dashboards.values():
            target_dashboard = arm.LineageNode(name=dashboard.dashboard_name, type="dashboard")
            datasets = set(x.dataset for x in dashboard.config.depends_on)
            for dataset in datasets:
                source_dataset = arm.LineageNode(name=dataset, type="dataset")
                all_lineage.append(arm.LineageRelation(type="runtime", source=source_dataset, target=target_dashboard))

        return all_lineage

    async def get_all_data_lineage(self) -> list[arm.LineageRelation]:
        """
        Get all data lineage in the project

        Returns:
            A list of LineageRelation objects
        """
        compiled_dag = await self._get_compiled_dag()
        return self._get_all_data_lineage(compiled_dag)

    async def _write_dataset_outputs_given_test_set(
        self, dataset: str, select: str, test_set: str | None, runquery: bool, recurse: bool
    ) -> t.Any | None:
        dataset_conf = self._manifest_cfg.datasets[dataset]
        default_test_set_conf = self._manifest_cfg.get_default_test_set(dataset)
        if test_set in self._manifest_cfg.selection_test_sets:
            test_set_conf = self._manifest_cfg.selection_test_sets[test_set]
        elif test_set is None or test_set == default_test_set_conf.name:
            test_set, test_set_conf = default_test_set_conf.name, default_test_set_conf
        else:
            raise ConfigurationError(f"No test set named '{test_set}' was found when compiling dataset '{dataset}'. The test set must be defined if not default for dataset.")
        
        error_msg_intro = f"Cannot compile dataset '{dataset}' with test set '{test_set}'."
        if test_set_conf.datasets is not None and dataset not in test_set_conf.datasets:
            raise ConfigurationError(f"{error_msg_intro}\n Applicable datasets for test set '{test_set}' does not include dataset '{dataset}'.")
        
        user_attributes = test_set_conf.user_attributes.copy() if test_set_conf.user_attributes is not None else {}
        selections = test_set_conf.parameters.copy()
        username, is_admin = user_attributes.pop("username", ""), user_attributes.pop("is_admin", False)
        if test_set_conf.is_authenticated:
            user = self._auth.User(username=username, is_admin=is_admin, **user_attributes)
        elif dataset_conf.scope == mf.PermissionScope.PUBLIC:
            user = None
        else:
            raise ConfigurationError(f"{error_msg_intro}\n Non-public datasets require a test set with 'user_attributes' section defined")
        
        if dataset_conf.scope == mf.PermissionScope.PRIVATE and not is_admin:
            raise ConfigurationError(f"{error_msg_intro}\n Private datasets require a test set with user_attribute 'is_admin' set to true")

        # always_python_df is set to True for creating CSV files from results (when runquery is True)
        dag = self._generate_dag(dataset, target_model_name=select, always_python_df=runquery)
        await dag.execute(
            self._param_args, self._param_cfg_set, self._context_func, user, selections, 
            runquery=runquery, recurse=recurse, default_traits=self._manifest_cfg.get_default_traits()
        )
        
        output_folder = u.Path(self._filepath, c.TARGET_FOLDER, c.COMPILE_FOLDER, dataset, test_set)
        if output_folder.exists():
            shutil.rmtree(output_folder)
        output_folder.mkdir(parents=True, exist_ok=True)
        
        def write_placeholders() -> None:
            output_filepath = u.Path(output_folder, "placeholders.json")
            with open(output_filepath, 'w') as f:
                json.dump(dag.placeholders, f, indent=4)
        
        def write_model_outputs(model: m.DataModel) -> None:
            assert isinstance(model, m.QueryModel)
            subfolder = c.DBVIEWS_FOLDER if model.model_type == m.ModelType.DBVIEW else c.FEDERATES_FOLDER
            subpath = u.Path(output_folder, subfolder)
            subpath.mkdir(parents=True, exist_ok=True)
            if isinstance(model.compiled_query, mq.SqlModelQuery):
                output_filepath = u.Path(subpath, model.name+'.sql')
                query = model.compiled_query.query
                with open(output_filepath, 'w') as f:
                    f.write(query)
            if runquery and isinstance(model.result, pl.LazyFrame):
                output_filepath = u.Path(subpath, model.name+'.csv')
                model.result.collect().write_csv(output_filepath)

        write_placeholders()
        all_model_names = dag.get_all_query_models()
        coroutines = [asyncio.to_thread(write_model_outputs, dag.models_dict[name]) for name in all_model_names]
        await u.asyncio_gather(coroutines)

        if recurse:
            self._draw_dag(dag, output_folder)
        
        if isinstance(dag.target_model, m.QueryModel) and dag.target_model.compiled_query is not None:
            return dag.target_model.compiled_query.query
    
    async def compile(
        self, *, dataset: str | None = None, do_all_datasets: bool = False, selected_model: str | None = None, test_set: str | None = None, 
        do_all_test_sets: bool = False, runquery: bool = False
    ) -> None:
        """
        Async method to compile the SQL templates into files in the "target/" folder. Same functionality as the "sqrl compile" CLI.

        Although all arguments are "optional", the "dataset" argument is required if "do_all_datasets" argument is False.

        Arguments:
            dataset: The name of the dataset to compile. Ignored if "do_all_datasets" argument is True, but required (i.e., cannot be None) if "do_all_datasets" is False. Default is None.
            do_all_datasets: If True, compile all datasets and ignore the "dataset" argument. Default is False.
            selected_model: The name of the model to compile. If specified, the compiled SQL query is also printed in the terminal. If None, all models for the selected dataset are compiled. Default is None.
            test_set: The name of the test set to compile with. If None, the default test set is used (which can vary by dataset). Ignored if `do_all_test_sets` argument is True. Default is None.
            do_all_test_sets: Whether to compile all applicable test sets for the selected dataset(s). If True, the `test_set` argument is ignored. Default is False.
            runquery**: Whether to run all compiled queries and save each result as a CSV file. If True and `selected_model` is specified, all upstream models of the selected model is compiled as well. Default is False.
        """
        recurse = True
        if do_all_datasets:
            selected_models = [(dataset.name, dataset.model) for dataset in self._manifest_cfg.datasets.values()]
        else:
            assert isinstance(dataset, str), "argument 'dataset' must be provided a string value if argument 'do_all_datasets' is False"
            assert dataset in self._manifest_cfg.datasets, f"dataset '{dataset}' not found in {c.MANIFEST_FILE}"
            if selected_model is None:
                selected_model = self._manifest_cfg.datasets[dataset].model
            else:
                recurse = False
            selected_models = [(dataset, selected_model)]
        
        coroutines: list[t.Coroutine] = []
        for dataset, selected_model in selected_models:
            if do_all_test_sets:
                for test_set_name in self._manifest_cfg.get_applicable_test_sets(dataset):
                    coroutine = self._write_dataset_outputs_given_test_set(dataset, selected_model, test_set_name, runquery, recurse)
                    coroutines.append(coroutine)
            
            coroutine = self._write_dataset_outputs_given_test_set(dataset, selected_model, test_set, runquery, recurse)
            coroutines.append(coroutine)
        
        queries = await u.asyncio_gather(coroutines)
        
        print(f"Compiled successfully! See the '{c.TARGET_FOLDER}/' folder for results.")
        print()
        if not recurse and len(queries) == 1 and isinstance(queries[0], str):
            print(queries[0])
            print()

    def _permission_error(self, user: BaseUser | None, data_type: str, data_name: str, scope: str) -> InvalidInputError:
        username = "" if user is None else f" '{user.username}'"
        return InvalidInputError(25, f"User{username} does not have permission to access {scope} {data_type}: {data_name}")
    
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
    
    async def dataset(
        self, name: str, *, selections: dict[str, t.Any] = {}, user: BaseUser | None = None, require_auth: bool = True
    ) -> dr.DatasetResult:
        """
        Async method to retrieve a dataset as a DatasetResult object (with metadata) given parameter selections.

        Arguments:
            name: The name of the dataset to retrieve.
            selections: A dictionary of parameter selections to apply to the dataset. Optional, default is empty dictionary.
            user: The user to use for authentication. If None, no user is used. Optional, default is None.
        
        Returns:
            A DatasetResult object containing the dataset result (as a polars DataFrame), its description, and the column details.
        """
        scope = self._manifest_cfg.datasets[name].scope
        if require_auth and not self._auth.can_user_access_scope(user, scope):
            raise self._permission_error(user, "dataset", name, scope.name)
        
        dag = self._generate_dag(name)
        await dag.execute(
            self._param_args, self._param_cfg_set, self._context_func, user, dict(selections), 
            default_traits=self._manifest_cfg.get_default_traits()
        )
        assert isinstance(dag.target_model.result, pl.LazyFrame)
        return dr.DatasetResult(
            target_model_config=dag.target_model.model_config, 
            df=dag.target_model.result.collect().with_row_index("_row_num", offset=1)
        )
    
    async def dashboard(
        self, name: str, *, selections: dict[str, t.Any] = {}, user: BaseUser | None = None, dashboard_type: t.Type[T] = dash.Dashboard
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
        scope = self._dashboards[name].config.scope
        if not self._auth.can_user_access_scope(user, scope):
            raise self._permission_error(user, "dashboard", name, scope.name)
        
        async def get_dataset_df(dataset_name: str, fixed_params: dict[str, t.Any]) -> pl.DataFrame:
            final_selections = {**selections, **fixed_params}
            result = await self.dataset(dataset_name, selections=final_selections, user=user, require_auth=False)
            return result.df
        
        args = d.DashboardArgs(self._param_args, get_dataset_df)
        try:
            return await self._dashboards[name].get_dashboard(args, dashboard_type=dashboard_type)
        except KeyError:
            raise KeyError(f"No dashboard file found for: {name}")
    
    async def query_models(
        self, sql_query: str, *, selections: dict[str, t.Any] = {}, user: BaseUser | None = None
    ) -> dr.DatasetResult:
        dag = await self._get_compiled_dag(sql_query=sql_query, selections=selections, user=user)
        await dag._run_models()
        assert isinstance(dag.target_model.result, pl.LazyFrame)
        return dr.DatasetResult(
            target_model_config=dag.target_model.model_config, 
            df=dag.target_model.result.collect().with_row_index("_row_num", offset=1)
        )
