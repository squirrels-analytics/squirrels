from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from sqlalchemy import Engine, create_engine
import yaml

from squirrels import _constants as c, _utils
from squirrels._credentials_manager import Credential, squirrels_config_io
from squirrels._utils import ConfigurationError, InvalidInputError


class Manifest:
    def __init__(self, parms: Dict) -> None:
        self._parms = parms
    
    @classmethod
    def from_yaml_str(cls, parms_str: str):
        parms = yaml.safe_load(parms_str)
        return cls(parms)

    @classmethod
    def from_file(cls, manifest_path: str):
        with open(manifest_path, 'r') as f:
            parms_str = f.read()
        
        return Manifest.from_yaml_str(parms_str)
    
    def get_proj_vars(self) -> Dict[str, Any]:
        return self._parms.get(c.PROJ_VARS_KEY, dict())
    
    def get_modules(self) -> List[str]:
        return self._parms.get(c.MODULES_KEY, list())
    
    def _get_required_field(self, key: str) -> Any:
        try:
            return self._parms[key]
        except KeyError as e:
            raise ConfigurationError(f'Field "{key}" not found in squirrels.yaml') from e
    
    def get_base_path(self) -> str:
        project_vars = self.get_proj_vars()
        try:
            product = project_vars[c.PRODUCT_KEY]
            major_version = project_vars[c.MAJOR_VERSION_KEY]
        except KeyError as e:
            raise ConfigurationError("Could not construct API endpoint as 'product' and 'major_version' \
                                     were not specified in project variables") from e
        base_path = f"/{product}/v{major_version}"
        return base_path
    
    def get_db_connections(self, test_creds: Dict[str, Credential] = None) -> Dict[str, Engine]:
        configs: Dict[str, Dict[str, str]] = self._parms.get(c.DB_CONNECTIONS_KEY, {})
        output = {}
        for key, config in configs.items():
            cred_key = config.get(c.DB_CREDENTIALS_KEY)
            if cred_key is None:
                cred = Credential("", "")
            elif test_creds is not None:
                cred = test_creds[cred_key]
            else:
                cred = squirrels_config_io.get_credential(cred_key)
            url = config[c.URL_KEY].replace("${username}", cred.username).replace("${password}", cred.password)
            output[key] = create_engine(url)
        return output

    def _get_dataset_parms(self, dataset: str) -> Dict[str, Any]:
        try:
            return self._get_required_field(c.DATASETS_KEY)[dataset]
        except KeyError as e:
            raise InvalidInputError(f'No such dataset named "{dataset}" exists') from e
        
    def _get_required_field_from_dataset_parms(self, dataset: str, key: str):
        try:
            return self._get_dataset_parms(dataset)[key]
        except KeyError as e:
            raise ConfigurationError(f'The "{key}" field is not defined for dataset "{dataset}"') from e
    
    def _get_all_database_view_parms(self, dataset: str) -> Dict[str, Dict[str, str]]:
        return self._get_required_field_from_dataset_parms(dataset, c.DATABASE_VIEWS_KEY)
    
    def get_all_dataset_names(self) -> str:
        datasets: Dict[str, Any] = self._get_required_field(c.DATASETS_KEY)
        return list(datasets.keys())
    
    def get_dataset_folder(self, dataset: str) -> Path:
        return _utils.join_paths(c.DATASETS_FOLDER, dataset)
        
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
                raise ConfigurationError(f'The "{c.FILE_KEY}" field is not defined for "{database_view}" in dataset "{dataset}"') from e
        dataset_folder = self.get_dataset_folder(dataset)
        return _utils.join_paths(dataset_folder, db_view_file)
    
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
    
    def get_dataset_final_view_file(self, dataset: str) -> Union[str, Path]:
        final_view_parms: Dict[str, Any] = self._get_required_field_from_dataset_parms(dataset, c.FINAL_VIEW_KEY)
        if isinstance(final_view_parms, str):
            final_view_file = final_view_parms
        else:
            try:
                final_view_file = final_view_parms[c.FILE_KEY]
            except KeyError as e:
                raise ConfigurationError(f'The "{c.FILE_KEY}" field is not defined for the final view') from e
        
        database_views = self.get_all_database_view_names(dataset)
        if final_view_file in database_views:
            return final_view_file
        else:
            dataset_path = self.get_dataset_folder(dataset)
            return _utils.join_paths(dataset_path, final_view_file)

    def get_setting(self, key: str, default: Any) -> Any:
        settings: Dict[str, Any] = self._parms.get(c.SETTINGS_KEY, dict())
        return settings.get(key, default)
    
    def get_catalog(self, parameters_path: str, results_path: str) -> Any:
        """
        Gets the component of the catalog API response that's generated by this manifest

        Parameters:
            parameters_path: The path to the parameters API endpoint
            results_path: The path to the results API endpoint
        
        Returns:
            A JSON response for the catalog API
        """
        datasets_info = []
        for dataset in self.get_all_dataset_names():
            dataset_normalized = _utils.normalize_name_for_api(dataset)
            datasets_info.append({
                'name': dataset,
                'label': self.get_dataset_label(dataset),
                'parameters_path': parameters_path.format(dataset=dataset_normalized),
                'result_path': results_path.format(dataset=dataset_normalized),
                'minor_version_ranges': [0, None]
            })
        
        project_vars = self.get_proj_vars()
        return {
            'response_version': 0,
            'products': [{
                'name': project_vars[c.PRODUCT_KEY],
                'versions': [{
                    'major_version': project_vars[c.MAJOR_VERSION_KEY],
                    'datasets': datasets_info
                }]
            }]
        }


def _from_file():
    return Manifest.from_file(c.MANIFEST_FILE)
