# Squirrels

Squirrels is an API framework for creating APIs that generate sql queries & dataframes dynamically from query parameters. 

## Setup

First, install the library and all dependencies in a separate virtual environment.

```bash
pip install pipenv
pipenv install -e .
```

Then, to activate your virtual environment, either change the python interpretor to the new virtual environment that was just created in you IDE and restart your terminal (preferred), or run:

```bash
pipenv shell
```

To confirm that the setup worked, run this to show the help page for all squirrels CLI commands:

```bash
squirrels -h
```

## Testing

```
python setup.py pytest
```

## Features Roadmap

- Provide a `squirrels init` CLI to create a squirrels project from scratch including sample files for squirrels.yaml, parameters.py, functions.py, database_view.sql.j2, selections.cfg, sample_lu_data.csv, and .gitignore
- Allow for database views as python files
- Introduce single-select and multi-select widgets for group bys
- Provide a `squirrels unit-test` CLI to perform unit tests from a `tests` folder
