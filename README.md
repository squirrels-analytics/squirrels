# Squirrels

Squirrels is an API framework that lets you create REST APIs for dynamic data analytics!

**Documentation**: <a href="https://docs.pysquirrels.com" target="_blank">https://docs.pysquirrels.com</a>

**Source Code**: <a href="https://github.com/squirrels-analytics/squirrels" target="_blank">https://github.com/squirrels-analytics/squirrels</a>

## Table of Contents

- [Main Features](#main-features)
- [License](#license)
- [Contributing to Squirrels](#contributing-to-squirrels)
    - [Setup](#setup)
    - [Testing](#testing)
    - [Project Structure](#project-structure)

## Main Features

Here are a few of the things that Squirrels can do:

- Connect to any database by specifying its SQLAlchemy url (in `squirrels.yml`) or by using its native connector library in python (in `connections.py`).
- Configure API routes for datasets (in `squirrels.yml`) without writing code.
- Configure parameter widgets (types include single-select, multi-select, date, number, etc.) for your datasets (in `parameters.py`).
- Use SQL templates (templated with Jinja, like dbt) or python functions (that return a Python dataframe in polars or pandas) to define dynamic query logic based on parameter selections.
- Query multiple databases and join the results together in a final view in one API endpoint/dataset!
- Test your API endpoints with Squirrels Studio or by a command line that generates rendered sql queries and results as files (for a given set of parameter selections).
- Define User model (in `user.py`) and authorize privacy scope per dataset (in `squirrels.yml`). The user's attributes can even be used in your query logic!
- Serve dataset metadata and results to AI agents via MCP (Model Context Protocol)

## Quick Start

In a new virtual environment, install `squirrels`. Then, in your project directory, activate the virtual environment and run the following commands:

```bash
sqrl new --use-defaults --curr-dir
sqrl build
```

To run the API server, simply run:

```bash
sqrl run
```

## License

Squirrels is released under the Apache 2.0 license.

See the file LICENSE for more details.

## Contributing to Squirrels

The sections below describe how to set up your local environment for Squirrels development and run unit tests. A high level overview of the project structure is also provided.

### Setup

This project requires the python package manager `uv` with Python 3.10 or above. Information on setting up uv can be found at: https://docs.astral.sh/uv/getting-started/installation/.

Then, to install all dependencies in a virtual environment, run:

```bash
uv sync -p 3.10
```

And activate the virtual environment with:

```bash
source .venv/bin/activate
```

To confirm that the setup worked, run the following to show the help page for all Squirrels CLI commands:

```bash
sqrl -h
```

### Testing

Run `uv run pytest`. Or if you have the virtual environment activated, simply run `pytest`.

### Project Structure

From the root of the git repo, the source code can be found in the `squirrels` folder and unit tests can be found in the `tests` folder.

To understand what a specific Squirrels command is doing, start from the `_command_line.py` file as your entry point.

The library version is maintained in both the `pyproject.toml` and the `squirrels/_version.py` files.

### Documentation

The contents for the documentation can be found in the `docs` folder.

To test the documentation, use npm to install the mintlify CLI:

```bash
npm i -g mint
```

Then, you will be able to generate a local preview of the documentation site after navigating to the `docs` directory.

```bash
cd docs
mint dev
```
