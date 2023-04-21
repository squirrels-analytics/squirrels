import sys, os
from squirrels import profile_manager as pm, manifest as mf, constants as c


def pytest_sessionstart(session):
    sys.path.append('sample_project')
    mf.initialize('sample_project/squirrels.yaml')
    database_path = os.path.abspath('./database/test.db')
    pm.Profile('product_profile').set('sqlite', f'/{database_path}', '', '')
