# Squirrels

Squirrels is an API framework that lets you create REST APIs for dynamic data analytics!

**Documentation**: <a href="https://squirrels-analytics.github.io/" target="_blank">https://squirrels-analytics.github.io/</a>

**Source Code**: <a href="https://github.com/squirrels-analytics/squirrels" target="_blank">https://github.com/squirrels-analytics/squirrels</a>

## Table of Contents

- [Main Features](#main-features)
- [License](#license)
- [Contributing to squirrels](#contributing-to-squirrels)
    - [Setup](#setup)
    - [Testing](#testing)
    - [Project Structure](#project-structure)

## Main Features

Here are a few of the things that squirrels can do:

- Connect to any database by specifying its SQLAlchemy url (in `squirrels.yml`) or by using its native connector library in python (in `connections.py`).
- Configure API routes for datasets (in `squirrels.yml`) without writing code.
- Configure parameter widgets (types include single-select, multi-select, date, number, etc.) for your datasets (in `parameters.py`).
- Use Jinja SQL templates (just like dbt!) or python functions (that return a Python dataframe such as polars or pandas) to define dynamic query logic based on parameter selections.
- Query multiple databases and join the results together in a final view in one API endpoint/dataset!
- Test your API endpoints with an interactive UI or by a command line that generates rendered sql queries and results (for a given set of parameter selections).
- Define authentication logic (in `auth.py`) and authorize privacy scope per dataset (in `squirrels.yml`). The user's attributes can even be used in your query logic!

## License

Squirrels is released under the Apache 2.0 license.

See the file LICENSE for more details.

## Contributing to squirrels

The sections below describe how to set up your local environment for squirrels development and run unit tests. A high level overview of the project structure is also provided.

### Setup

This project requires python version 3.10 or above to be installed. It also uses the python build tool `poetry`. Information on setting up poetry can be found at: https://python-poetry.org/docs/.

Then, to install all dependencies, run:

```
poetry install
```

And activate the virtual environment created by poetry with:

```
poetry shell
```

To confirm that the setup worked, run the following to show the help page for all squirrels CLI commands:

```bash
sqrl -h
```

You can enter `exit` to exit the virtual environment shell. You can also run `poetry run sqrl -h` to run squirrels commands without activating the virtual environment.

### Testing

In poetry's virtual environment, run `pytest`.

### Project Structure

From the root of the git repo, the source code can be found in the `squirrels` folder and unit tests can be found in the `tests` folder.

To understand what a specific squirrels command is doing, start from the `_command_line.py` file as your entry point.

The library version is maintained in both the `pyproject.toml` and the `squirrels/_version.py` files.

When a user initializes a squirrels project using `sqrl init`, the files are copied from the `squirrels/package_data/base_project` folder. The contents in the `database` subfolder were constructed from the scripts in the `database_elt` folder.

For the Squirrels UI activated by `sqrl run`, the HTML, CSS, and Javascript files can be found in the `static` and `templates` subfolders of `squirrels/package_data`. The CSS and Javascript files are minified and built from the source files in this project: https://github.com/squirrels-analytics/squirrels-testing-ui.
