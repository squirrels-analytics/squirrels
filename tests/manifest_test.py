from typing import List, Dict, Any
from pathlib import Path
import pytest

from squirrels.credentials_manager import Credential
from squirrels.manifest import Manifest
from squirrels.utils import ConfigurationError, InvalidInputError


@pytest.fixture
def empty_manifest() -> Manifest:
    return Manifest({})


@pytest.fixture
def empty_db_view_manifest() -> Manifest:
    parms = {
        'modules': [],
        'project_variables': {},
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'database_views': {'db_view_1': {}},
                'final_view': 'db_view1'
            }
        }
    }
    return Manifest(parms)


@pytest.fixture
def minimal_manifest() -> Manifest:
    parms = {
        'modules': [],
        'project_variables': {
            'product': 'my_product',
            'major_version': 1,
            'minor_version': 0
        },
        'db_connections': {
            'default': {'url': 'sqlite://'}
        },
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'database_views': {
                    'db_view_1': 'db_view1.py'
                },
                'final_view': {'file': 'db_view_1'}
            }
        }
    }
    return Manifest(parms)


@pytest.fixture
def basic_manifest() -> Manifest:
    parms = {
        'modules': ['module1'],
        'project_variables': {
            'product': 'my_product',
            'major_version': 1,
            'minor_version': 0
        },
        'db_connections': {
            'default': {'url': 'sqlite://'},
            'my_other_db': {'url': 'sqlite:////${username}/${password}.db', 'credential_key': 'test_cred_key'}
        },
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'database_views': {
                    'db_view_1': {'file': 'db_view1.sql.j2', 'db_connection': 'my_other_db'},
                    'db_view_2': {'file': 'db_view2.sql.j2', 'args': {'arg1': 'val1'}}
                },
                'final_view': 'final_view.sql.j2',
                'args': {'arg2': 'val2'}
            }
        },
        'settings': {
            'results.cache.size': 128
        }
    }
    return Manifest(parms)


def test_invalid_configurations(empty_manifest: Manifest, empty_db_view_manifest: Manifest, 
                                minimal_manifest: Manifest):
    with pytest.raises(ConfigurationError):
        empty_manifest.get_base_path()
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_base_path()
    with pytest.raises(ConfigurationError):
        empty_manifest.get_all_dataset_names()
    with pytest.raises(ConfigurationError):
        empty_manifest.get_dataset_label('dataset_test')
    with pytest.raises(InvalidInputError):
        minimal_manifest.get_dataset_label('wrong_name')
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_database_view_file('dataset1', 'db_view_1')


@pytest.mark.parametrize('manifest_name,expected', [
    ('empty_manifest', {}),
    ('minimal_manifest', {'product': 'my_product', 'major_version': 1, 'minor_version': 0}),
    ('basic_manifest', {'product': 'my_product', 'major_version': 1, 'minor_version': 0})
])
def test_get_proj_vars(manifest_name: str, expected: Dict[str, Any], request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_proj_vars() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('empty_manifest', []),
    ('minimal_manifest', []),
    ('basic_manifest', ['module1'])
])
def test_get_modules(manifest_name: str, expected: List[str], request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_modules() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('minimal_manifest', '/my_product/v1')
])
def test_get_base_path(manifest_name: str, expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_base_path() == expected


def test_get_db_connections(basic_manifest: Manifest):
    db_connections = basic_manifest.get_db_connections({'test_cred_key': Credential('user1', 'pass1')})
    assert str(db_connections['default'].url) == 'sqlite://'
    assert str(db_connections['my_other_db'].url) == 'sqlite:////user1/pass1.db'


def test_get_all_dataset_names():
    parms = {'datasets': {'dataset1': {}, 'dataset2': {}}}
    manifest = Manifest(parms)
    assert manifest.get_all_dataset_names() == ['dataset1', 'dataset2']


@pytest.mark.parametrize('manifest_name,dataset,expected',[
    ('minimal_manifest', 'dataset1', ['db_view_1']),
    ('basic_manifest', 'dataset1', ['db_view_1', 'db_view_2'])
])
def test_get_all_database_view_names(manifest_name: str, dataset: str, expected: str, 
                                     request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_all_database_view_names(dataset) == expected


@pytest.mark.parametrize('manifest_name,dataset,database_view,expected', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'datasets/dataset1/db_view1.py'),
    ('basic_manifest', 'dataset1', 'db_view_2', 'datasets/dataset1/db_view2.sql.j2')
])
def test_get_database_view_file(manifest_name: str, dataset: str, database_view: str, expected: str, 
                                request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_database_view_file(dataset, database_view) == Path(expected)


@pytest.mark.parametrize('manifest_name,dataset,database_view,expected', [
    ('empty_db_view_manifest', 'dataset1', 'db_view_1', {}),
    ('minimal_manifest', 'dataset1', 'db_view_1', 
     {'product': 'my_product', 'major_version': 1, 'minor_version': 0}),
    ('basic_manifest', 'dataset1', 'db_view_2', 
     {'product': 'my_product', 'major_version': 1, 'minor_version': 0, 'arg2': 'val2', 'arg1': 'val1'})
])
def test_get_database_view_args(manifest_name: str, dataset: str, database_view: str, 
                                expected: Dict[str, Any], request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_view_args(dataset, database_view) == expected


@pytest.mark.parametrize('manifest_name,dataset,database_view,expected', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'default'),
    ('basic_manifest', 'dataset1', 'db_view_1', 'my_other_db'),
    ('basic_manifest', 'dataset1', 'db_view_2', 'default')
])
def test_get_database_view_db_connection(manifest_name: str, dataset: str, database_view: str, 
                                         expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_database_view_db_connection(dataset, database_view) == expected


@pytest.mark.parametrize('manifest_name,dataset,expected,type', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'str'),
    ('basic_manifest', 'dataset1', 'datasets/dataset1/final_view.sql.j2', 'path')
])
def test_get_dataset_final_view_file(manifest_name: str, dataset: str, expected: str, 
                                     type: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    if type == 'path':
        expected = Path(expected)
    assert manifest.get_dataset_final_view_file(dataset) == expected


@pytest.mark.parametrize('manifest_name,key,expected', [
    ('minimal_manifest', 'results.cache.size', 1000),
    ('basic_manifest', 'results.cache.size', 128),
    ('basic_manifest', 'parameters.cache.size', 1000)
])
def test_get_setting(manifest_name: str, key: str, expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_setting(key, 1000) == expected


def test_get_catalog(basic_manifest: Manifest):
    expected = {"response_version": 0, "products": [
        {"name": "my_product", "versions":[
            {"major_version": 1, "datasets": [{
                "name": "dataset1", 
                "label": "Dataset", 
                "parameters_path": "/parameters", 
                "result_path": "/results",
                "minor_version_ranges": [0, None]
            }]}
        ]}
    ]}
    assert basic_manifest.get_catalog("/parameters", "/results") == expected
