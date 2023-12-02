from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
from pathlib import Path
import yaml

from . import _constants as c, _utils as u
from ._environcfg import EnvironConfigIO
from ._timer import timer, time


@dataclass
class _Manifest:
    _config: Dict
    
    def get_proj_vars(self) -> Dict[str, Any]:
        return self._config.get(c.PROJ_VARS_KEY, dict())
    
    def get_modules(self) -> List[str]:
        return self._config.get(c.MODULES_KEY, list())
    
    def _get_required_field(self, key: str) -> Any:
        try:
            return self._config[key]
        except KeyError as e:
            raise u.ConfigurationError(f'Field "{key}" not found in squirrels.yaml') from e
    
    def _get_required_proj_var(self, key) -> str:
        project_vars = self.get_proj_vars()
        try:
            product_name = project_vars[key]
        except KeyError as e:
            raise u.ConfigurationError(f"The '{key}' must be specified in project variables") from e
        return product_name

    def get_product_name(self) -> str:
        return self._get_required_proj_var(c.PRODUCT_NAME_KEY)
    
    def get_product_label(self) -> str:
        product_name = self.get_product_name()
        project_vars = self.get_proj_vars()
        return project_vars.get(c.PRODUCT_LABEL_KEY, product_name)
    
    def get_major_version(self) -> str:
        return self._get_required_proj_var(c.MAJOR_VERSION_KEY)
    
    def get_minor_version(self) -> str:
        return self._get_required_proj_var(c.MINOR_VERSION_KEY)
    
    def get_db_connections(self) -> Dict[str, Dict[str, str]]:
        return self._config.get(c.DB_CONNECTIONS_KEY, {})
    
    def get_parameters(self) -> List[Dict]:
        return self._config.get(c.PARAMETERS_KEY, [])

    def _get_dataset_parms(self, dataset: str) -> Dict[str, Any]:
        try:
            return self._get_required_field(c.DATASETS_KEY)[dataset]
        except KeyError as e:
            raise u.InvalidInputError(f'No such dataset named "{dataset}" exists') from e
        
    def _get_required_field_from_dataset_parms(self, dataset: str, key: str):
        try:
            return self._get_dataset_parms(dataset)[key]
        except KeyError as e:
            raise u.ConfigurationError(f'The "{key}" field is not defined for dataset "{dataset}"') from e
    
    def _get_all_database_view_parms(self, dataset: str) -> Dict[str, Dict[str, str]]:
        return self._get_required_field_from_dataset_parms(dataset, c.DATABASE_VIEWS_KEY)
    
    def get_all_dataset_names(self) -> str:
        datasets: Dict[str, Any] = self._get_required_field(c.DATASETS_KEY)
        return list(datasets.keys())
    
    def get_dataset_folder(self, dataset: str) -> Path:
        return u.join_paths(c.DATASETS_FOLDER, dataset)
        
    def get_dataset_args(self, dataset: str) -> Dict[str, Any]:
        dataset_args = self._get_dataset_parms(dataset).get("args", {})
        full_args = {**self.get_proj_vars(), **dataset_args}
        return full_args
    
    def get_all_database_view_names(self, dataset: str) -> List[str]:
        all_database_views = self._get_all_database_view_parms(dataset)
        return list(all_database_views.keys())
    
    def get_database_view_file(self, dataset: str, database_view: str) -> Path:
        database_view_parms = self._get_all_database_view_parms(dataset)[database_view]
        if isinstance(database_view_parms, str):
            db_view_file = database_view_parms
        else:
            try:
                db_view_file = database_view_parms[c.FILE_KEY]
            except KeyError as e:
                raise u.ConfigurationError(f'The "{c.FILE_KEY}" field is not defined for "{database_view}" in dataset "{dataset}"') from e
        dataset_folder = self.get_dataset_folder(dataset)
        return u.join_paths(dataset_folder, db_view_file)
    
    def get_view_args(self, dataset: str, database_view: str = None) -> Dict[str, Any]:
        dataset_args = self.get_dataset_args(dataset)
        if database_view is None:
            view_parms: Dict[str, Any] = self._get_required_field_from_dataset_parms(dataset, c.FINAL_VIEW_KEY)
        else:
            view_parms: Dict[str, Any] = self._get_all_database_view_parms(dataset)[database_view]
        view_args: Dict[str, Any] = {} if isinstance(view_parms, str) else view_parms.get("args", {})
        full_args = {**dataset_args, **view_args}
        return full_args

    def get_database_view_db_connection(self, dataset: str, database_view: str) -> Optional[str]:
        database_view_parms = self._get_all_database_view_parms(dataset)[database_view]
        if isinstance(database_view_parms, str):
            db_connection = c.DEFAULT_DB_CONN 
        else: 
            db_connection = database_view_parms.get(c.DB_CONNECTION_KEY, c.DEFAULT_DB_CONN)
        return db_connection
    
    def get_dataset_label(self, dataset: str) -> str:
        return self._get_required_field_from_dataset_parms(dataset, c.DATASET_LABEL_KEY)
    
    def get_dataset_scope(self, dataset: str) -> str:
        scope: str = self._get_dataset_parms(dataset).get(c.SCOPE_KEY, c.PUBLIC_SCOPE)
        return scope.strip().lower()
    
    def get_dataset_parameters(self, dataset: str) -> Optional[List[str]]:
        dataset_params = self._get_dataset_parms(dataset).get(c.DATASET_PARAMETERS_KEY)
        return dataset_params
    
    def get_dataset_final_view_file(self, dataset: str) -> Union[str, Path]:
        final_view_parms: Dict[str, Any] = self._get_required_field_from_dataset_parms(dataset, c.FINAL_VIEW_KEY)
        if isinstance(final_view_parms, str):
            final_view_file = final_view_parms
        else:
            try:
                final_view_file = final_view_parms[c.FILE_KEY]
            except KeyError as e:
                raise u.ConfigurationError(f'The "{c.FILE_KEY}" field is not defined for the final view') from e
        
        database_views = self.get_all_database_view_names(dataset)
        if final_view_file in database_views:
            return final_view_file
        else:
            dataset_path = self.get_dataset_folder(dataset)
            return u.join_paths(dataset_path, final_view_file)

    def get_setting(self, key: str, default: Any) -> Any:
        settings: Dict[str, Any] = self._config.get(c.SETTINGS_KEY, dict())
        return settings.get(key, default)


class ManifestIO:
    obj: _Manifest

    @classmethod
    def LoadFromFile(cls) -> None:
        EnvironConfigIO.LoadFromFile()
        
        start = time.time()
        with open(c.MANIFEST_FILE, 'r') as f:
            raw_content = f.read()
        
        env_config = EnvironConfigIO.obj.get_all_env_vars()
        content = u.render_string(raw_content, env_config)
        proj_config = yaml.safe_load(content)
        cls.obj = _Manifest(proj_config)
        timer.add_activity_time("loading squirrels.yaml file", start)
