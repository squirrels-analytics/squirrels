from typing import List, Any
from pathlib import Path
import pytest, textwrap

from squirrels.manifest import Manifest
from squirrels.utils import ConfigurationError, InvalidInputError


@pytest.fixture
def empty_manifest() -> Manifest:
    return Manifest({})


@pytest.fixture
def empty_db_view_manifest() -> Manifest:
    parms = {
        'base_path': '/base/path',
        'modules': [],
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
    proj_vars = {}
    parms = {
        'base_path': '/base/path',
        'modules': [],
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'database_views': {
                    'db_view_1': {'file': 'db_view1.py', 'db_connection': 'some_db'}
                },
                'final_view': 'db_view_1'
            }
        }
    }
    return Manifest(parms, proj_vars)


@pytest.fixture
def basic_manifest() -> Manifest:
    proj_vars = {
        'product': 'my_product',
        'major_version': 0,
        'minor_version': 1
    }
    parms = {
        'base_path': '/my_product/v0',
        'modules': ['module1'],
        'db_connection': 'my_db',
        'datasets': {
            'dataset1': {
                'label': 'Dataset',
                'database_views': {
                    'db_view_1': {'file': 'db_view1.sql.j2', 'db_connection': 'my_other_db'},
                    'db_view_2': {'file': 'db_view2.sql.j2'}
                },
                'final_view': 'final_view.sql.j2'
            }
        },
        'settings': {
            'results.cache.size': 128
        }
    }
    return Manifest(parms, proj_vars)


def test_from_yaml_str():
    empty_proj_vars_str = """


    """
    proj_vars_str = textwrap.dedent(
        """
        product: my_product
        major_version: 0
        """
    )
    parms_str = textwrap.dedent(
        """
        base_path: /{{product}}/v{{major_version}}
        """
    )
    manifest1 = Manifest.from_yaml_str(parms_str, proj_vars_str)
    manifest2 = Manifest.from_yaml_str(parms_str)
    manifest3 = Manifest.from_yaml_str(parms_str, empty_proj_vars_str)

    assert manifest1.get_parms() == {'base_path': '/my_product/v0'}
    assert manifest2.get_parms() == {'base_path': '/{{product}}/v{{major_version}}'}
    assert manifest3.get_parms() == {'base_path': '/{{product}}/v{{major_version}}'}

    assert manifest1.get_proj_vars() == {'product': 'my_product', 'major_version': 0}
    assert manifest2.get_proj_vars() == {}
    assert manifest3.get_proj_vars() == {}


def test_invalid_configurations(empty_manifest: Manifest, empty_db_view_manifest: Manifest, 
                                minimal_manifest: Manifest):
    with pytest.raises(ConfigurationError):
        empty_manifest.get_base_path()
    with pytest.raises(ConfigurationError):
        empty_manifest.get_all_dataset_names()
    with pytest.raises(ConfigurationError):
        empty_manifest.get_dataset_label('dataset_test')
    with pytest.raises(InvalidInputError):
        minimal_manifest.get_dataset_label('wrong_name')
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_database_view_file('dataset1', 'db_view_1')
    with pytest.raises(ConfigurationError):
        empty_db_view_manifest.get_database_view_db_connection('dataset1', 'db_view_1')


@pytest.mark.parametrize('manifest_name,expected', [
    ('empty_manifest', []),
    ('minimal_manifest', []),
    ('basic_manifest', ['module1'])
])
def test_get_modules(manifest_name: str, expected: List[str], request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_modules() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('minimal_manifest', '/base/path')
])
def test_get_base_path(manifest_name: str, expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_base_path() == expected


@pytest.mark.parametrize('manifest_name,expected', [
    ('minimal_manifest', None),
    ('basic_manifest', 'my_db')
])
def test_get_default_db_connection(manifest_name: str, expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_default_db_connection() == expected


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
    ('minimal_manifest', 'dataset1', 'db_view_1', 'some_db'),
    ('basic_manifest', 'dataset1', 'db_view_1', 'my_other_db'),
    ('basic_manifest', 'dataset1', 'db_view_2', 'my_db')
])
def test_get_database_view_db_connection(manifest_name: str, dataset: str, database_view: str, 
                                         expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_database_view_db_connection(dataset, database_view) == expected


@pytest.mark.parametrize('manifest_name,dataset,expected,type', [
    ('minimal_manifest', 'dataset1', 'db_view_1', 'str'),
    ('basic_manifest', 'dataset1', 'datasets/dataset1/final_view.sql.j2', 'path')
])
def test_get_dataset_final_view(manifest_name: str, dataset: str, expected: str, 
                                type: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    if type == 'path':
        expected = Path(expected)
    assert manifest.get_dataset_final_view(dataset) == expected


@pytest.mark.parametrize('manifest_name,key,expected', [
    ('minimal_manifest', 'results.cache.size', 1000),
    ('basic_manifest', 'results.cache.size', 128),
    ('basic_manifest', 'parameters.cache.size', 1000)
])
def test_get_setting(manifest_name: str, key: str, expected: str, request: pytest.FixtureRequest):
    manifest: Manifest = request.getfixturevalue(manifest_name)
    assert manifest.get_setting(key, 1000) == expected
