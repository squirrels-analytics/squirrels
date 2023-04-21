from setuptools import setup
from os import path

with open('squirrels/version.txt') as f:
    __version__ = f.read()

# The directory containing this file
HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='squirrels',
    version=__version__,
    packages=['squirrels'],
    description='Python Package for Configuring SQL Generating APIs',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Tim Huang',
    author_email='tim.yuting@hotmail.com',
    license='MIT',
    install_requires=[
        'inquirer', 'pwinput', 'cachetools', 'fastapi', 'uvicorn', 'Jinja2', 
        'GitPython', 'pandasql', 'pandas', 'sqlalchemy<2', 'pyyaml'
    ],
    setup_requires=['pytest-runner==6.0.0'],
    tests_require=['pytest==7.2.0'],
    test_suite='tests',
    package_data= {
        'squirrels': ['static/*', 'templates/*', 'base_project/database/*', 'base_project/datasets/sample_dataset/*', 
                      'base_project/.gitignore', 'base_project/*']
    },
    entry_points= {
        'console_scripts': ['squirrels=squirrels.command_line:main']
    }
)
