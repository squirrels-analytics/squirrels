from setuptools import setup, find_packages
import os

# The directory containing this file
HERE = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Recursively get package data
def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths

extra_files = package_files(os.path.join('squirrels', 'package_data'))

setup(
    name='squirrels',
    version='0.2.0',
    packages=find_packages(),
    description='Squirrels - Create REST APIs for BI Analytics',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Tim Huang',
    author_email='tim.yuting@hotmail.com',
    license='MIT',
    install_requires=[
        'python-jose', 'cryptography', 'python-multipart', 'openpyxl', 'inquirer', 'pwinput', 
        'cachetools', 'fastapi', 'uvicorn', 'Jinja2', 'GitPython', 'sqlalchemy', 'pandas', 'pyyaml'
    ],
    setup_requires=['pytest-runner==6.0.0'],
    tests_require=['pytest==7.2.0'],
    test_suite='tests',
    package_data= {'squirrels': extra_files},
    entry_points= {
        'console_scripts': ['squirrels=squirrels._command_line:main']
    }
)
