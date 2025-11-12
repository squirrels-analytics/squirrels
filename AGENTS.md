# AGENTS.md

## Purpose
Instructions and guardrails for AI coding agents working with this repository. Act autonomously using these rules; ask only when blocked by missing credentials or truly ambiguous requirements.

## Project overview
- Language: Python (requires Python ~=3.10)
- Package: `squirrels` (API framework for dynamic data analytics)
- CLI entry points: `squirrels`, `sqrl` -> `squirrels._command_line:main`
- Docs: `docs/` folder (built with Mintlify)
- Key features:
  - Configure datasets and parameters to expose REST APIs for analytics.
  - Compile and build the Virtual Data Lake (VDL).
  - Jinja SQL and Python model support; multi-source federation.
  - Auth, dashboards, seeding, and macros support.

## Environment & setup
- Python: >= 3.10 required
- Package manager: `uv` (Astral)
- Install `uv`: https://docs.astral.sh/uv/getting-started/installation/

Setup commands:
- Install dependencies and create venv:
  - `uv sync`
- Activate venv:
  - `.venv\Scripts\activate` (Windows)
  - `source .venv/bin/activate` (macOS/Linux)

Confirm CLI:
- `sqrl -h` (or `squirrels -h`)

## Coding standards
- Formatting/linting: follow existing style in repo (PEP8-ish; no enforced tool listed).
- Type hints: Pydantic 2 is used; annotate public functions and models.
- Error handling: raise domain-specific exceptions from `squirrels/_exceptions.py` where suitable; avoid bare `except`.
- Logging: use project logger; avoid printing except where CLI UX intends it.
- Avoid duplication of code.
- Maintain backward compatibility.
- Always use Python type hints. Strict mode is preferred.

## Documentation standards
- "Squirrels" is always spelled with the first letter as capital.
- Prefer lowercase with only the first letter capitalized for title and section headers. Although the first letter is generally capital, there are exceptions such as variable names and CLI commands. Outside of the first letter, names such as "Squirrels" and "Python" always have a capital first letter, even when used in the headers.
- File names for mdx files must only contain lowercase and dashes (no exceptions).
- Update `docs.json` when new pages are added or removed.
- When creating or editing Python reference pages, use `docs/references/python/squirrelsproject.mdx` as the perfect example for styling and format.

## Contribution & change guidelines
- When adding CLI options or routes, update:
  - Help text in `squirrels/_command_line.py`
  - Samples/templates in `_package_data/` if user-facing
  - Tests in `tests/`
  - `README.md` if user-facing behavior changes
- Versioning is tracked in both `pyproject.toml` and `squirrels/_version.py`; keep them in sync for releases.

## Framework architecture & project structure

### High-level architecture
- Core package is `squirrels/`, exposing a CLI and FastAPI server to run analytics APIs powered by DuckDB and/or Python dataframes (Polars/Pandas).
- Documentation contents are in `docs/`, built with Mintlify.
- Projects created by the initializer contain configuration, models, dashboards, and assets, which are compiled/built into a Virtual Data Lake (VDL) and exposed via REST routes.

### Important modules (library internals)
- `_command_line.py`: CLI entry; defines commands: `init`, `get-file`, `deps`, `compile`, `build`, `duckdb`, `run`.
- `_api_server.py`: Starts FastAPI, wires middleware, auth, and routes.
- `_api_routes/`: Route handlers
  - `datasets.py`: Dataset query endpoints
  - `dashboards.py`: Dashboard metadata/routes
  - `auth.py` and `oauth2.py`: Authentication flows
  - `project.py`: Project metadata, health, and tooling endpoints
- `_models.py`: Orchestrates compile/run/build of models; entry for model graph execution.
- `_model_builder.py`, `_model_configs.py`, `_model_queries.py`: Build-time structures and SQL/Python query generation.
- `_manifest.py`: Collects/validates project configuration (datasets, parameters, sources, dashboards) from files.
- `_parameters.py`, `_parameter_sets.py`, `_parameter_options.py`: Parameter definitions, options, and test sets.
- `_sources.py`, `_data_sources.py`, `_connection_set.py`: Data source and connection management (SQLAlchemy URLs or native connectors). 
- `_dashboards.py`: Dashboard definitions and rendering metadata support.
- `_auth.py`: Auth helpers and user model integration.
- `_schemas/`: Pydantic models for request/response, query params, and auth payloads.
- `_initializer.py`: Scaffolds a new project and sample files; also provides `get-file` assets.
- `_package_data/`: Base templates and sample assets copied by the initializer.

### Typical generated project structure

When you run `sqrl new <name>` (or `--curr-dir`), the initializer creates a project directory with:
- `squirrels.yml`: Main manifest configuring datasets, routes, privacy scopes, and dashboards.
- `pyconfigs/`
  - `connections.py` or `connections.yml`: Database connections (SQLAlchemy URLs or native connectors)
  - `parameters.py` or `parameters.yml`: Parameter widgets, defaults, and validation
  - `context.py`: Shared Python utilities/context for Jinja and Python models
  - `user.py`: User model for auth scope and attributes
- `models/`
  - `builds/`: Static tables/materializations (typically SQL or Python)
  - `dbviews/`: Dynamic (based on realtime parameter selections) SQL views that run on an external database
  - `federates/`: Dynamic (based on realtime parameter selections) SQL views or Python data models that run on the server and can act as a federation of multiple other data models
  - `sources.yml`: Details of source tables
- `dashboards/`: Dashboard definitions (`.yml`/`.py`)
- `macros/`: Jinja SQL macros used by models
- `seeds/`: CSVs and seed configs to load lookup/reference data
- `assets/`: Sample databases (e.g., SQLite) and any static assets
- `target/`: Outputs (compiled SQL, logs)
  - `compile/`: Rendered SQL files from `sqrl compile`
  - `duckdb_init.sql`: Initialization SQL used by `sqrl duckdb`

Alternatively, you can run `sqrl init` as an alias for `sqrl new --curr-dir`.

### Model types and execution
- SQL models (Jinja-templated): Compiled into concrete SQL using project parameters and context.
- Python models: Functions returning dataframes (Polars/Pandas) that are materialized or federated.
- Seeds: Load CSV seed data into DuckDB for joins/lookups.

### Build and compile lifecycle
- `sqrl compile`: Render SQL into `target/compile/` without executing. Useful for debugging SQL and verifying parameter expansion.
- `sqrl build`: Execute models to create/update the Virtual Data Lake (VDL) (usually stored using DuckLake), optionally `--full-refresh` to rebuild from scratch. Seeds, static builds, and necessary views are created.
- `sqrl run`: Optionally `--build` first, then start the API server and serve routes.
- `sqrl duckdb` / `--ui`: Open DuckDB shell/UI against the DuckDB venv with project init SQL loaded if present.

## Useful references
- Start reading CLI: `squirrels/_command_line.py`
- API server entry: `squirrels/_api_server.py`
- Routes: `squirrels/_api_routes/`
- Models orchestration: `squirrels/_models.py`
- Docs: https://squirrels-analytics.github.io/