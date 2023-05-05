# Squirrels

Squirrels is an API framework for creating REST APIs that generate sql queries & dataframes dynamically from query parameters. 

## Setup

First, install the library and all dependencies **in an independent virtual environment**.

```bash
# activate virtual environment...
pip install -e .
```

To confirm that the setup worked, run this to show the help page for all squirrels CLI commands:

```bash
squirrels -h
```

## Testing

```
python setup.py pytest
```

## Documentation

Check the documentation website [here](https://squirrels-nest.github.io/squirrels-docs/) or get started [here](https://squirrels-nest.github.io/squirrels-docs/getting-started/) to learn how to use the squirrels library to make an API project.

## Developer Guide

From the root of the git repo, the source code can be found in the `squirrels` folder and unit tests can be found in the `tests` folder.

To understand what a specific squirrels command line utility is doing, start from the `_command_line.py` file as your entry point.

The library version is maintained in both the `setup.py` file (for the next release or release-candidate version) and the `squirrels/version.py` file (for the next release version only).

When a user initializes a squirrels project using `squirrels init`, the files are copied from the `squirrels/package_data/base_project` folder. The contents in the `database` subfolder were constructed from the scripts in the `database_elt` folder at the top level.

For the Squirrels UI activated by `squirrels run`, the HTML, CSS, and Javascript files can be found in the `static` and `templates` subfolders of `squirrels/package_data`.

## License

Squirrels is released under the MIT license.

See the file LICENSE for more details.
