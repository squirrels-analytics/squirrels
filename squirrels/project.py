import typing as _t, functools as _ft, asyncio as _aio, os as _os, shutil as _shutil, json as _json
import logging as _l, uuid as _uu, matplotlib.pyplot as _plt, networkx as _nx, polars as _pl

from . import _utils as _u, _constants as _c, _environcfg as _ec, _manifest as _mf, _authenticator as _auth
from . import _seeds as _s, _connection_set as _cs, _models as _m, _dashboards_io as _d, _parameter_sets as _ps
from . import _model_queries as _mq, dashboards as _dash

T = _t.TypeVar('T', bound=_dash.Dashboard)


class _CustomJsonFormatter(_l.Formatter):
    def format(self, record: _l.LogRecord) -> str:
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
        return _json.dumps(output)


class SquirrelsProject:
    """
    Initiate an instance of this class to interact with a Squirrels project through Python code. For example this can be handy to experiment with the datasets produced by Squirrels in a Jupyter notebook.
    """
    
    def __init__(self, *, filepath: str = ".", log_file: str | None = _c.LOGS_FILE, log_level: str = "INFO", log_format: str = "text") -> None:
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
    
    def _get_logger(self, base_path: str, log_file: str | None, log_level: str, log_format: str) -> _u.Logger:
        logger = _u.Logger(name=_uu.uuid4().hex)
        logger.setLevel(log_level.upper())
        
        if log_file:
            path = _u.Path(base_path, _c.LOGS_FOLDER, log_file)
            path.parent.mkdir(parents=True, exist_ok=True)

            handler = _l.FileHandler(path)
            if log_format.lower() == "json":
                handler.setFormatter(_CustomJsonFormatter())
            elif log_format.lower() == "text":
                formatter = _l.Formatter("[%(name)s] %(asctime)s - %(levelname)s - %(message)s")
                handler.setFormatter(formatter)
            else:
                raise ValueError("log_format must be either 'text' or 'json'")
            logger.addHandler(handler)
        else:
            logger.disabled = True
        
        return logger
    
    @property
    @_ft.cache
    def _env_cfg(self) -> _ec.EnvironConfig:
        return _ec.EnvironConfigIO.load_from_file(self._logger, self._filepath)

    @property
    @_ft.cache
    def _manifest_cfg(self) -> _mf.ManifestConfig:
        return _mf.ManifestIO.load_from_file(self._logger, self._filepath, self._env_cfg)
    
    @property
    @_ft.cache
    def _seeds(self) -> _s.Seeds:
        return _s.SeedsIO.load_files(self._logger, self._filepath, self._manifest_cfg)
    
    @property
    @_ft.cache
    def _model_files(self) -> dict[_m.ModelType, dict[str, _mq.QueryFileWithConfig]]:
        return _m.ModelsIO.load_files(self._logger, self._filepath)
    
    @property
    @_ft.cache
    def _context_func(self) -> _m.ContextFunc:
        return _m.ModelsIO.load_context_func(self._logger, self._filepath)
    
    @property
    @_ft.cache
    def _dashboards(self) -> dict[str, _d.DashboardDefinition]:
        return _d.DashboardsIO.load_files(self._logger, self._filepath)
    
    @property
    @_ft.cache
    def _conn_args(self) -> _cs.ConnectionsArgs:
        return _cs.ConnectionSetIO.load_conn_py_args(self._logger, self._env_cfg, self._manifest_cfg)
    
    @property
    def _conn_set(self) -> _cs.ConnectionSet:
        if not hasattr(self, "__conn_set") or self.__conn_set is None:
            self.__conn_set = _cs.ConnectionSetIO.load_from_file(self._logger, self._filepath, self._manifest_cfg, self._conn_args)
        return self.__conn_set
    
    @property
    @_ft.cache
    def _authenticator(self) -> _auth.Authenticator:
        token_expiry_minutes = self._manifest_cfg.settings.get(_c.AUTH_TOKEN_EXPIRE_SETTING, 30)
        return _auth.Authenticator(self._filepath, self._env_cfg, self._conn_args, self._conn_set, token_expiry_minutes)
    
    @property
    @_ft.cache
    def _param_args(self) -> _ps.ParametersArgs:
        return _ps.ParameterConfigsSetIO.get_param_args(self._conn_args)
    
    @property
    @_ft.cache
    def _param_cfg_set(self) -> _ps.ParameterConfigsSet:
        return _ps.ParameterConfigsSetIO.load_from_file(
            self._logger, self._filepath, self._manifest_cfg, self._seeds, self._conn_set, self._param_args
        )
    
    @property
    @_ft.cache
    def _j2_env(self) -> _u.EnvironmentWithMacros:
        return _u.EnvironmentWithMacros(self._logger, loader=_u.j2.FileSystemLoader(self._filepath))
    
    @property
    @_ft.cache
    def User(self) -> type[_auth.User]:
        """
        A direct reference to the User class in the `auth.py` file (if applicable). If `auth.py` does not exist, then this returns the `squirrels.User` class.
        """
        return self._authenticator.user_cls
    
    def close(self) -> None:
        """
        Deliberately close any open resources within the Squirrels project, such as database connections (instead of relying on the garbage collector).
        """
        if hasattr(self, "__conn_set") and self.__conn_set is not None:
            self.__conn_set.dispose()
            self.__conn_set = None

    def __exit__(self, exc_type, exc_val, traceback):
        self.close()
    
    def _generate_dag(self, dataset: str, *, target_model_name: str | None = None, always_python_df: bool = False) -> _m.DAG:
        seeds_dict = self._seeds.get_dataframes()

        models_dict: dict[str, _m.Referable] = {key: _m.Seed(key, seed.config, seed.df) for key, seed in seeds_dict.items()}

        for name, val in self._model_files[_m.ModelType.DBVIEW].items():
            models_dict[name] = _m.DbviewModel(name, val.config, val.query_file, self._manifest_cfg, self._conn_set, logger=self._logger, j2_env=self._j2_env)
            models_dict[name].needs_python_df = always_python_df
        
        for name, val in self._model_files[_m.ModelType.FEDERATE].items():
            models_dict[name] = _m.FederateModel(name, val.config, val.query_file, self._manifest_cfg, self._conn_set, logger=self._logger, j2_env=self._j2_env)
            models_dict[name].needs_python_df = always_python_df
        
        dataset_config = self._manifest_cfg.datasets[dataset]
        target_model_name = dataset_config.model if target_model_name is None else target_model_name
        target_model = models_dict[target_model_name]
        target_model.is_target = True
        
        return _m.DAG(self._manifest_cfg, dataset_config, target_model, models_dict, self._logger)
    
    def _draw_dag(self, dag: _m.DAG, output_folder: _u.Path) -> None:
        color_map = {_m.ModelType.SEED: "green", _m.ModelType.DBVIEW: "red", _m.ModelType.FEDERATE: "skyblue"}

        G = dag.to_networkx_graph()
        
        fig, _ = _plt.subplots()
        pos = _nx.multipartite_layout(G, subset_key="layer")
        colors = [color_map[node[1]] for node in G.nodes(data="model_type")] # type: ignore
        _nx.draw(G, pos=pos, node_shape='^', node_size=1000, node_color=colors, arrowsize=20)
        
        y_values = [val[1] for val in pos.values()]
        scale = max(y_values) - min(y_values) if len(y_values) > 0 else 0
        label_pos = {key: (val[0], val[1]-0.002-0.1*scale) for key, val in pos.items()}
        _nx.draw_networkx_labels(G, pos=label_pos, font_size=8)
        
        fig.tight_layout()
        _plt.margins(x=0.1, y=0.1)
        fig.savefig(_u.Path(output_folder, "dag.png"))
        _plt.close(fig)

    async def _write_dataset_outputs_given_test_set(
        self, dataset: str, select: str, test_set: str | None, runquery: bool, recurse: bool
    ) -> _t.Any | None:
        dataset_conf = self._manifest_cfg.datasets[dataset]
        default_test_set_conf = self._manifest_cfg.get_default_test_set(dataset)
        if test_set in self._manifest_cfg.selection_test_sets:
            test_set_conf = self._manifest_cfg.selection_test_sets[test_set]
        elif test_set is None or test_set == default_test_set_conf.name:
            test_set, test_set_conf = default_test_set_conf.name, default_test_set_conf
        else:
            raise _u.ConfigurationError(f"No test set named '{test_set}' was found when compiling dataset '{dataset}'. The test set must be defined if not default for dataset.")
        
        error_msg_intro = f"Cannot compile dataset '{dataset}' with test set '{test_set}'."
        if test_set_conf.datasets is not None and dataset not in test_set_conf.datasets:
            raise _u.ConfigurationError(f"{error_msg_intro}\n Applicable datasets for test set '{test_set}' does not include dataset '{dataset}'.")
        
        user_attributes = test_set_conf.user_attributes.copy()
        selections = test_set_conf.parameters.copy()
        username, is_internal = user_attributes.pop("username", ""), user_attributes.pop("is_internal", False)
        if test_set_conf.is_authenticated:
            user = self.User.Create(username, is_internal=is_internal, **user_attributes)
        elif dataset_conf.scope == _mf.DatasetScope.PUBLIC:
            user = None
        else:
            raise _u.ConfigurationError(f"{error_msg_intro}\n Non-public datasets require a test set with 'user_attributes' section defined")
        
        if dataset_conf.scope == _mf.DatasetScope.PRIVATE and not is_internal:
            raise _u.ConfigurationError(f"{error_msg_intro}\n Private datasets require a test set with user_attribute 'is_internal' set to true")

        # always_python_df is set to True for creating CSV files from results (when runquery is True)
        dag = self._generate_dag(dataset, target_model_name=select, always_python_df=runquery)
        placeholders = await dag.execute(self._param_args, self._param_cfg_set, self._context_func, user, selections, runquery=runquery, recurse=recurse)
        
        output_folder = _u.Path(self._filepath, _c.TARGET_FOLDER, _c.COMPILE_FOLDER, dataset, test_set)
        if _os.path.exists(output_folder):
            _shutil.rmtree(output_folder)
        _os.makedirs(output_folder, exist_ok=True)
        
        def write_placeholders() -> None:
            output_filepath = _u.Path(output_folder, "placeholders.json")
            with open(output_filepath, 'w') as f:
                _json.dump(placeholders, f, indent=4)
        
        def write_model_outputs(model: _m.Referable) -> None:
            assert isinstance(model, _m.QueryModel)
            subfolder = _c.DBVIEWS_FOLDER if model.model_type == _m.ModelType.DBVIEW else _c.FEDERATES_FOLDER
            subpath = _u.Path(output_folder, subfolder)
            _os.makedirs(subpath, exist_ok=True)
            if isinstance(model.compiled_query, _mq.SqlModelQuery):
                output_filepath = _u.Path(subpath, model.name+'.sql')
                query = model.compiled_query.query
                with open(output_filepath, 'w') as f:
                    f.write(query)
            if runquery and isinstance(model.result, _pl.LazyFrame):
                output_filepath = _u.Path(subpath, model.name+'.csv')
                model.result.collect().write_csv(output_filepath)

        write_placeholders()
        all_model_names = dag.get_all_query_models()
        coroutines = [_aio.to_thread(write_model_outputs, dag.models_dict[name]) for name in all_model_names]
        await _aio.gather(*coroutines)

        if recurse:
            self._draw_dag(dag, output_folder)
        
        if isinstance(dag.target_model, _m.QueryModel) and dag.target_model.compiled_query is not None:
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
            assert dataset in self._manifest_cfg.datasets, f"dataset '{dataset}' not found in {_c.MANIFEST_FILE}"
            if selected_model is None:
                selected_model = self._manifest_cfg.datasets[dataset].model
            else:
                recurse = False
            selected_models = [(dataset, selected_model)]
        
        coroutines: list[_t.Coroutine] = []
        for dataset, selected_model in selected_models:
            if do_all_test_sets:
                for test_set_name in self._manifest_cfg.get_applicable_test_sets(dataset):
                    coroutine = self._write_dataset_outputs_given_test_set(dataset, selected_model, test_set_name, runquery, recurse)
                    coroutines.append(coroutine)
            
            coroutine = self._write_dataset_outputs_given_test_set(dataset, selected_model, test_set, runquery, recurse)
            coroutines.append(coroutine)
        
        queries = await _aio.gather(*coroutines)
        
        print(f"Compiled successfully! See the '{_c.TARGET_FOLDER}/' folder for results.")
        print()
        if not recurse and len(queries) == 1 and isinstance(queries[0], str):
            print(queries[0])
            print()

    def _permission_error(self, user: _auth.User | None, data_type: str, data_name: str, scope: str) -> PermissionError:
        username = None if user is None else user.username
        return PermissionError(f"User '{username}' does not have permission to access {scope} {data_type}: {data_name}")
    
    def seed(self, name: str) -> _pl.DataFrame:
        """
        Method to retrieve a seed as a pandas DataFrame given a seed name.

        Arguments:
            name: The name of the seed to retrieve

        Returns:
            The seed as a pandas DataFrame
        """
        seeds_dict = self._seeds.get_dataframes()
        try:
            return seeds_dict[name].df
        except KeyError:
            available_seeds = list(seeds_dict.keys())
            raise KeyError(f"Seed '{name}' not found. Available seeds are: {available_seeds}")
    
    async def _dataset_helper(
        self, name: str, selections: dict[str, _t.Any], user: _auth.User | None
    ) -> _pl.DataFrame:
        dag = self._generate_dag(name)
        await dag.execute(self._param_args, self._param_cfg_set, self._context_func, user, dict(selections))
        assert isinstance(dag.target_model.result, _pl.LazyFrame)
        return dag.target_model.result.collect()
    
    async def dataset(
        self, name: str, *, selections: dict[str, _t.Any] = {}, user: _auth.User | None = None
    ) -> _pl.DataFrame:
        """
        Async method to retrieve a dataset as a pandas DataFrame given parameter selections.

        Arguments:
            name: The name of the dataset to retrieve.
            selections: A dictionary of parameter selections to apply to the dataset. Optional, default is empty dictionary.
            user: The user to use for authentication. If None, no user is used. Optional, default is None.
        
        Returns:
            A pandas DataFrame containing the dataset.
        """
        scope = self._manifest_cfg.datasets[name].scope
        if not self._authenticator.can_user_access_scope(user, scope):
            raise self._permission_error(user, "dataset", name, scope.name)
        return await self._dataset_helper(name, selections, user)
    
    async def dashboard(
        self, name: str, *, selections: dict[str, _t.Any] = {}, user: _auth.User | None = None, dashboard_type: _t.Type[T] = _dash.Dashboard
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
        if not self._authenticator.can_user_access_scope(user, scope):
            raise self._permission_error(user, "dashboard", name, scope.name)
        
        async def get_dataset(dataset_name: str, fixed_params: dict[str, _t.Any]) -> _pl.DataFrame:
            final_selections = {**selections, **fixed_params}
            return await self._dataset_helper(dataset_name, final_selections, user)
        
        args = _d.DashboardArgs(self._param_args.proj_vars, self._param_args.env_vars, get_dataset)
        try:
            return await self._dashboards[name].get_dashboard(args, dashboard_type=dashboard_type)
        except KeyError:
            raise KeyError(f"No dashboard file found for: {name}")
