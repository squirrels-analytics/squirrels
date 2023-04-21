# Getting Started with Squirrels

An introductory tutorial!

---

## Installation

First, in the folder for your squirrels project, create the virtual environment. For example, you can use `pipenv` (install with `pip install pipenv`) to create the virtual environment and install the squirrels library in one command:

```bash
pipenv install squirrels
```

Activate the virtual environment with:

```bash
pipenv shell
```

and deactivate with `exit`.

## Initialize a New Project

You can initialize the project files using:

```bash
squirrels init --core --sample-db seattle-weather
```

For the purpose of this tutorial, we are using the `--sample-db` option to create a sample database to use. All the core files are also created using the `--core` option. 

!!! Note 
    Without using any CLI options (i.e., just `squirrels init`), a set of prompts will be provided instead for the file(s) you need, and the core files would be created by default. However, the prompts go away by specifying any CLI options, and the core files are not created by default if the `--core` option is not included.

Once the command is executed, all core files are created. This includes:

- `.gitignore`, `requirements.txt`, and `squirrels.yaml` at the project root
- `parameters.py` and `database_view.sql.j2` in the `datasets/sample_dataset` subfolder
- `seattle_weather.db` sqlite database in the `database` folder

For more details, see docs for the [init CLI].

## Provide the Database Connection

Next, create a squirrels database profile with a specified profile name of your choice. For the rest of the tutorial, we will assume the profile name is `myprofile`.

```bash
squirrels set-profile myprofile --values sqlite /./database/seattle_weather.db "" ""
```

The `--values` option let you specify the sql dialect/driver, database url, username, and password in the command line all at once instead of using the set of prompts.

!!! Note
    It is better practice to use an absolute path in the database url for the sqlite database. We are using a relative path to `seattle_weather.db` here for simplicity.

In the `squirrels.yaml` file, set the `db_profile` to `myprofile`.

For more details on database profiles, see docs for the [set-profile CLI].

## Configure a Dataset

In the `squirrels.yaml` file, set the `product` property under `project_variables` to `seattle_weather`.

Replace the `sample_dataset` field (under `datasets`) with `weather_by_time`. Specify the following values under the `weather_by_time` field:

- Set `label` to `Weather by Time of Year`
- Set `name` (under `database_views`) to `weather_by_time`
- Set `file` (under `database_views`) to `weather_by_time.sql.j2`
- Set `final_view` to `final_view.sql.j2`

At this point in time, your `squirrels.yaml` file should look something like this:

```yaml
project_variables:
  product: seattle_weather
  major_version: '0'
  minor_version: '1'

modules: []

db_profile: myprofile

base_path: "/{{product}}/v{{major_version}}"

datasets:
  weather_by_time:
    label: Weather by Time of Year
    database_views:
    - name: weather_by_time
      file: weather_by_time.sql.j2
    final_view: weather_by_time

settings: {}
```

Rename the following files/folders to reflect the changes you made in the `squirrels.yaml` file.

- Rename `datasets/sample_dataset` to `datasets/weather_by_time`
- Rename `datasets/weather_by_time/database_view1.sql.j2` to `datasets/weather_by_time/weather_by_time.sql.j2`

For more details on the `squirrels.yaml` file, see the docs for [squirrels.yaml].

## Create the Parameters

In the `datasets/weather_by_time/` folder, there's a `parameters.py` file to specify the parameters for the `weather_by_time` dataset. Replace the contents of the `parameters.py` file with the following.

```python
from typing import Callable, Dict
import squirrels as sq

class GroupByOption(sq.ParameterOption):
    def __init__(self, id, label, dim_col, order_by_col = None):
        super().__init__(id, label)
        self.dim_col = dim_col
        self.order_by_col = order_by_col if order_by_col is not None else dim_col

group_by_options = [
    GroupByOption('0', 'Year', 'year'),
    GroupByOption('1', 'Quarter', 'quarter'),
    GroupByOption('2', 'Month', 'month_name', 'month_order'),
    GroupByOption('3', 'Day of Year', 'day_of_year'),
    GroupByOption('4', 'Condition', 'condition')
]

def main() -> Dict[str, sq.Parameter]:
    return {
        'group_by': sq.SingleSelectParameter('Group By', group_by_options),
    }
```

Classes like `ParameterOption`, `Parameter`, and `SingleSelectParameter` are provided by the squirrels framework. In the code above, we extend from the existing `ParameterOption` class to create our own class with additional attributes. We will be able to use these attributes in the sql query templates we define later. The `parameters.py` file must specify a `main()` function that returns a dictionary of parameter names (as keys) to parameter objects (as value). In the code above, we specified one single-select parameter called `group_by` which will affect the dimension column used for aggregating in the sql query.

For more details on the available classes for parameter configurations, see docs for [parameters.py].

## Create the Dynamic SQL Query

In the `datasets/weather_by_time/` folder, replace the contents of the `weather_by_time.sql.j2` file with the following.

```sql
{% set selected_group_by = prms('group_by').get_selected() -%}
{% set dim_col = selected_group_by.dim_col -%}
{% set order_col = selected_group_by.order_by_col -%}

SELECT {{ dim_col }}
    , avg(temp_max) as temperature_high_C
    , avg(temp_min) as temperature_low_C
    , avg(precipitation) as precipitation_inches
    , avg(wind) as wind_mph
FROM weather
GROUP BY {{ dim_col }}, {{ order_col }}
ORDER BY {{ order_col }}
```

The lines written like `{% set ... -%}` uses Jinja2 syntax to create variables for the templated sql to use. The `prms` function is available to retrieve a Parameter object, and for SingleSelectParameter's, the `.get_selected()` method is available to retrieve the selected ParameterOption, which we extended as a GroupByOption. Thus, the `dim_col` and `order_by_col` attributes are available on the GroupByOption.

The database view file can also be a python file. For more details, see the docs for [database views].

Note that this example only uses one "database view", and the "final view" does not apply any further transformations. For more complex use cases, you can also write Jinja2 templated sql or python files for the final view as well to process on the API server from the results of one or more database views. For more details, see the docs for [final view].

In addition, this framework also lets you define the `dim_col` and `order_col` variables through python instead of through the Jinja template. For more details, see the docs for [context.py].

## Test the Generated Output

You can test the output of the generated SQL query and parameters response for the default parameter selections of the `weather_by_time` dataset by running:

```bash
squirrels test weather_by_time
```

This creates a `outputs/weather_by_time` subfolder with the generated SQL query without running it yet. Confirm all outputs look as expected. You can also run the following to generate all database views and final view results as csv files.

```bash
squirrels test weather_by_time --runquery
```

You can also test on non-default parameter selections. For more details, see docs for the [test CLI].

## Run the API Server

Run the following CLI command to activate the API server in "debug mode":

```bash
squirrels run --debug
```

You should now be able to access the following APIs.

- http://localhost:8000/squirrels0/seattle-weather/v0
    - Catalog of the parameters and results APIs for each dataset
- http://localhost:8000/squirrels0/seattle-weather/v0/weather-by-time/parameters
    - All the parameters information for the dataset
- http://localhost:8000/squirrels0/seattle-weather/v0/weather-by-time
    - The results of the dataset using the default value for each parameter

For a simple UI to test the API interactions, go to `http://localhost:8000/` from your browser.

For more details, see docs for the [run CLI].


[init CLI]: cli-guide/init.md
[set-profile CLI]: cli-guide/set-profile.md
[squirrels.yaml]: user-guide/squirrels-manifest.md
[parameters.py]: user-guide/parameters.md
[context.py]:user-guide/context.md
[database views]: user-guide/database-views.md
[final view]: user-guide/final-view.md
[test CLI]: cli-guide/test.md
[run CLI]: cli-guide/run.md
