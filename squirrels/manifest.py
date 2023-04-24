from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import yaml

from squirrels import constants as c, utils
from squirrels.utils import ConfigurationError, InvalidInputError
from squirrels.timed_imports import jinja2 as j2


class Manifest:
    def __init__(self, parms: Dict, proj_vars: Dict[str, str] = {}) -> None:
        self._parms = parms
        self._proj_vars = proj_vars
    
    @classmethod
    def from_yaml_str(cls, parms_str: str, proj_vars_str: str = ''):
        proj_vars_str = proj_vars_str.rstrip()
        if proj_vars_str != '':
            proj_vars = yaml.safe_load(proj_vars_str)
            template = utils.j2_env.from_string(parms_str)
            rendered = template.render(**proj_vars)
        else:
            proj_vars = {}
            rendered = parms_str
        parms = yaml.safe_load(rendered)
        return cls(parms, proj_vars)

    @classmethod
    def from_file(cls, manifest_path: str, project_vars_path: str):
        try:
            with open(project_vars_path, 'r') as f:
                proj_vars_str = f.read()
        except FileNotFoundError:
            proj_vars_str = ''
            
        with open(manifest_path, 'r') as f:
            parms_str = f.read()
        
        return Manifest.from_yaml_str(parms_str, proj_vars_str)
    
    def get_parms(self):
        return self._parms
    
    def get_proj_vars(self):
        return self._proj_vars
    
    def get_modules(self):
        return self._parms.get(c.MODULES_KEY, list())
    
    def _get_required_field(self, key: str):
        try:
            return self._parms[key]
        except KeyError as e:
            raise ConfigurationError(f'Field "{key}" not found in squirrels.yaml') from e
    
    def get_base_path(self) -> str:
        return self._get_required_field(c.BASE_PATH_KEY)
    
    def get_default_db_connection(self) -> Optional[str]:
        return self._parms.get(c.DB_CONNECTION_KEY, None)

    def _get_dataset_parms(self, dataset: str) -> Dict[str, Any]:
        try:
            return self._get_required_field(c.DATASETS_KEY)[dataset]
        except KeyError as e:
            raise InvalidInputError(f'No such dataset named "{dataset}" exists') from e
        
    def _get_required_field_from_dataset_parms(self, dataset: str, key: str):
        try:
            return self._get_dataset_parms(dataset)[key]
        except KeyError as e:
            raise ConfigurationError(f'The "{key}" field is not defined for dataset "{dataset}"')
    
    def _get_all_database_view_parms(self, dataset: str) -> Dict[str, Dict[str, str]]:
        return self._get_required_field_from_dataset_parms(dataset, c.DATABASE_VIEWS_KEY)
    
    def get_all_dataset_names(self) -> str:
        datasets: Dict[str, Any] = self._get_required_field(c.DATASETS_KEY)
        return list(datasets.keys())
    
    def get_dataset_folder(self, dataset: str) -> Path:
        return utils.join_paths(c.DATASETS_FOLDER, dataset)
    
    def get_all_database_view_names(self, dataset: str) -> List[str]:
        all_database_views = self._get_all_database_view_parms(dataset)
        return list(all_database_views.keys())
    
    def get_database_view_file(self, dataset: str, database_view: str) -> Path:
        database_view_parms = self._get_all_database_view_parms(dataset)[database_view]
        try:
            db_view_file = database_view_parms[c.DB_VIEW_FILE_KEY]
        except KeyError as e:
            raise ConfigurationError(f'The "{c.DB_VIEW_FILE_KEY}" field is not defined for "{database_view}" in dataset "{dataset}"') from e
        dataset_folder = self.get_dataset_folder(dataset)
        return utils.join_paths(dataset_folder, db_view_file)

    def get_database_view_db_connection(self, dataset: str, database_view: str) -> str:
        database_view_parms = self._get_all_database_view_parms(dataset)[database_view]
        try:
            db_connection = database_view_parms.get(c.DB_CONNECTION_KEY, None)
            return db_connection if db_connection is not None else self._parms[c.DB_CONNECTION_KEY]
        except KeyError as e:
            raise ConfigurationError(f'Undefined database profile for "{database_view}" in dataset "{dataset}"') from e
    
    def get_dataset_label(self, dataset: str) -> str:
        return self._get_required_field_from_dataset_parms(dataset, c.DATASET_LABEL_KEY)
    
    def get_dataset_final_view(self, dataset: str) -> Union[str, Path]:
        final_view = self._get_required_field_from_dataset_parms(dataset, c.FINAL_VIEW_KEY)
        database_views = self.get_all_database_view_names(dataset)
        if final_view in database_views:
            return final_view
        else:
            dataset_path = self.get_dataset_folder(dataset)
            return utils.join_paths(dataset_path, final_view)

    def get_setting(self, key: str, default: Any) -> Any:
        settings: Dict[str, Any] = self._parms.get(c.SETTINGS_KEY, dict())
        return settings.get(key, default)


def from_file():
    return Manifest.from_file(c.MANIFEST_FILE, c.PROJ_VARS_FILE)
