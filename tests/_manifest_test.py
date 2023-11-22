from typing import List, Dict, Any
from pathlib import Path
import pytest

from squirrels._manifest import _Manifest
from squirrels._utils import ConfigurationError, InvalidInputError
from squirrels import _constants as c


@pytest.fixture(scope="module")
def empty_manifest() -> _Manifest:
    return _Manifest({})


@pytest.fixture(scope="module")
def empty_db_view_manifest() -> _Manifest:
    parms = {
        'modules': [],
        'project_variables': {},
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'database_views': {'db_view_1': {}},
                'final_view': 'db_view_1'
            }
        }
    }
    return _Manifest(parms)


@pytest.fixture(scope="module")
def minimal_manifest() -> _Manifest:
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
    return _Manifest(parms)


@pytest.fixture(scope="module")
def basic_manifest() -> _Manifest:
    parms = {
        'modules': ['module1'],
        'project_variables': {
            'product': 'my_product',
            'product_label': 'My Product',
            'major_version': 1,
            'minor_version': 0
        },
        'db_connections': {
            'default': {'url':  'sqlite://', 'credential_key': 'test_cred_key'}
        },
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'scope': 'PrOtectEd ',
                'parameters': ['param1', 'param2'],
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
    return _Manifest(parms)


def test_invalid_configurations(empty_manifest: _Manifest, empty_db_view_manifest: _Manifest, 
                                minimal_manifest: _Manifest):
    with pytest.raises(ConfigurationError):
        empty_manifest.get_product()
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_product()
    with pytest.raises(ConfigurationError):
        empty_manifest.get_major_version()
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_major_version()
    with pytest.raises(ConfigurationError):
        empty_manifest.get_dataset_label('dataset_test')
    with pytest.raises(InvalidInputError):
        minimal_manifest.get_dataset_label('wrong_name')
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_database_view_file('dataset1', 'db_view_1')


@pytest.mark.parametrize('manifest_name,expected', [
    ('empty_manifest', {}),
    ('minimal_manifest', {'product': 'my_product', 'major_version': 1, 'minor_version': 0}),
    ('basic_manifest', {'product': 'my_product', 'product_label': 'My Product', 'major_version': 1, 'minor_version': 0})
])
def test_get_proj_vars(manifest_name: str, expected: Dict[str, Any], request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_proj_vars() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('empty_manifest', []),
    ('minimal_manifest', []),
    ('basic_manifest', ['module1'])
])
def test_get_modules(manifest_name: str, expected: List[str], request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_modules() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('minimal_manifest', 'my_product')
])
def test_get_product(manifest_name: str, expected: str, request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_product() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('minimal_manifest', 1)
])
def test_get_major_version(manifest_name: str, expected: str, request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_major_version() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('empty_manifest', {}),
    ('minimal_manifest', {'default': {'url': 'sqlite://'}}),
    ('basic_manifest', {'default': {'url':  'sqlite://', 'credential_key': 'test_cred_key'}})
])
def test_get_db_connections(manifest_name: str, expected: str, request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_db_connections() == expected


def test_get_all_dataset_names():
    parms = {'datasets': {'dataset1': {}, 'dataset2': {}}}
    manifest = _Manifest(parms)
    assert manifest.get_all_dataset_names() == ['dataset1', 'dataset2']
    
    manifest2 = _Manifest({})
    with pytest.raises(ConfigurationError):
        manifest2.get_all_dataset_names()


@pytest.mark.parametrize('manifest_name,dataset,expected',[
    ('minimal_manifest', 'dataset1', ['db_view_1']),
    ('basic_manifest', 'dataset1', ['db_view_1', 'db_view_2'])
])
def test_get_all_database_view_names(manifest_name: str, dataset: str, expected: str, 
                                     request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_all_database_view_names(dataset) == expected


@pytest.mark.parametrize('manifest_name,dataset,database_view,expected', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'datasets/dataset1/db_view1.py'),
    ('basic_manifest', 'dataset1', 'db_view_2', 'datasets/dataset1/db_view2.sql.j2')
])
def test_get_database_view_file(manifest_name: str, dataset: str, database_view: str, expected: str, 
                                request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_database_view_file(dataset, database_view) == Path(expected)


@pytest.mark.parametrize('manifest_name,dataset,database_view,expected', [
    ('empty_db_view_manifest', 'dataset1', 'db_view_1', {}),
    ('minimal_manifest', 'dataset1', 'db_view_1', 
     {'product': 'my_product', 'major_version': 1, 'minor_version': 0}),
    ('basic_manifest', 'dataset1', 'db_view_2', 
     {'product': 'my_product', 'product_label': 'My Product', 'major_version': 1, 'minor_version': 0, 'arg2': 'val2', 'arg1': 'val1'}),
    ('basic_manifest', 'dataset1', None, 
     {'product': 'my_product', 'product_label': 'My Product', 'major_version': 1, 'minor_version': 0, 'arg2': 'val2'})
])
def test_get_view_args(manifest_name: str, dataset: str, database_view: str, expected: Dict[str, Any], 
                       request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_view_args(dataset, database_view) == expected


@pytest.mark.parametrize('manifest_name,dataset,database_view,expected', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'default'),
    ('basic_manifest', 'dataset1', 'db_view_1', 'my_other_db'),
    ('basic_manifest', 'dataset1', 'db_view_2', 'default')
])
def test_get_database_view_db_connection(manifest_name: str, dataset: str, database_view: str, 
                                         expected: str, request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_database_view_db_connection(dataset, database_view) == expected


@pytest.mark.parametrize('manifest_name,dataset,expected', [
    ('minimal_manifest', 'dataset1', c.PUBLIC_SCOPE),
    ('basic_manifest', 'dataset1', c.PROTECTED_SCOPE)
])
def test_get_dataset_scope(manifest_name: str, dataset: str, expected: str, 
                           request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_dataset_scope(dataset) == expected


@pytest.mark.parametrize('manifest_name,dataset,expected', [
    ('minimal_manifest', 'dataset1', None),
    ('basic_manifest', 'dataset1', ['param1', 'param2'])
])
def test_get_dataset_parameters(manifest_name: str, dataset: str, expected: str, 
                           request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_dataset_parameters(dataset) == expected


@pytest.mark.parametrize('manifest_name,dataset,expected,type', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'str'),
    ('basic_manifest', 'dataset1', 'datasets/dataset1/final_view.sql.j2', 'path')
])
def test_get_dataset_final_view_file(manifest_name: str, dataset: str, expected: str, 
                                     type: str, request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    if type == 'path':
        expected = Path(expected)
    assert manifest.get_dataset_final_view_file(dataset) == expected


@pytest.mark.parametrize('manifest_name,key,expected', [
    ('minimal_manifest', 'results.cache.size', 1000),
    ('basic_manifest', 'results.cache.size', 128),
    ('basic_manifest', 'parameters.cache.size', 1000)
])
def test_get_setting(manifest_name: str, key: str, expected: str, request: pytest.FixtureRequest):
    manifest: _Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_setting(key, 1000) == expected
