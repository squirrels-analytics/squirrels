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
