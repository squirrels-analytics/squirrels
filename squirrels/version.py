import os

_directory = os.path.dirname(__file__)
with open(os.path.join(_directory, 'version.txt'), 'r') as f:
    __version__ = f.read()

major_version, minor_version, patch_version = __version__.split('.')
