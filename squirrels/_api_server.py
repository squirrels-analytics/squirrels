from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPBearer
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
from contextlib import asynccontextmanager
from argparse import Namespace
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware
from mcp.server.fastmcp import FastMCP
import io, time, mimetypes, traceback, uuid, asyncio, urllib.parse, contextlib

from . import _constants as c, _utils as u
from ._exceptions import InvalidInputError, ConfigurationError, FileExecutionError
from ._version import __version__, sq_major_version
from ._project import SquirrelsProject

# Import route modules
from ._api_routes.auth import AuthRoutes
from ._api_routes.project import ProjectRoutes
from ._api_routes.datasets import DatasetRoutes
from ._api_routes.dashboards import DashboardRoutes
from ._api_routes.data_management import DataManagementRoutes
from ._api_routes.oauth2 import OAuth2Routes

mimetypes.add_type('application/javascript', '.js')


class SmartCORSMiddleware(BaseHTTPMiddleware):
    """
    Custom CORS middleware that allows specific origins to use credentials
    while still allowing all other origins without credentials.
    """
    
    def __init__(self, app, allowed_credential_origins: list[str] | None = None):
        super().__init__(app)
        # Origins that are allowed to send credentials (cookies, auth headers)
        self.allowed_credential_origins = allowed_credential_origins or []
    
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin")
        
        # Call the next middleware/route
        response: StarletteResponse = await call_next(request)
        
        # Handle preflight requests
        if request.method == "OPTIONS":
            response.headers["Access-Control-Allow-Methods"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "Authorization"
        
        # Always expose the Applied-Username header
        response.headers["Access-Control-Expose-Headers"] = "Applied-Username"
        
        if origin:
            scheme = "http" if request.url.hostname in ["localhost", "127.0.0.1"] else "https"
            request_origin = f"{scheme}://{request.url.netloc}"
            # Check if this origin is in the whitelist or if origin matches the host origin
            if origin == request_origin or origin in self.allowed_credential_origins:
                response.headers["Access-Control-Allow-Origin"] = origin
                response.headers["Access-Control-Allow-Credentials"] = "true"
            else:
                # Allow all other origins but without credentials / cookies
                response.headers["Access-Control-Allow-Origin"] = "*"
        else:
            # No origin header (same-origin request or non-browser)
            response.headers["Access-Control-Allow-Origin"] = "*"
        
        return response


class ApiServer:
    def __init__(self, no_cache: bool, project: SquirrelsProject) -> None:
        """
        Constructor for ApiServer

        Arguments:
            no_cache (bool): Whether to disable caching
        """
        self.no_cache = no_cache
        self.project = project
        self.logger = project._logger
        self.env_vars = project._env_vars
        self.j2_env = project._j2_env
        self.manifest_cfg = project._manifest_cfg
        self.seeds = project._seeds
        self.conn_args = project._conn_args
        self.conn_set = project._conn_set
        self.authenticator = project._auth
        self.param_args = project._param_args
        self.param_cfg_set = project._param_cfg_set
        self.context_func = project._context_func
        self.dashboards = project._dashboards
        
        self.mcp = FastMCP(name="Squirrels", stateless_http=True)

        # Initialize route modules
        get_bearer_token = HTTPBearer(auto_error=False)
        self.oauth2_routes = OAuth2Routes(get_bearer_token, project, no_cache)
        self.auth_routes = AuthRoutes(get_bearer_token, project, no_cache)
        self.project_routes = ProjectRoutes(get_bearer_token, project, no_cache)
        self.dataset_routes = DatasetRoutes(get_bearer_token, project, no_cache)
        self.dashboard_routes = DashboardRoutes(get_bearer_token, project, no_cache)
        self.data_management_routes = DataManagementRoutes(get_bearer_token, project, no_cache)
    
    
    async def _monitor_for_staging_file(self) -> None:
        """Background task that monitors for staging file and renames it when present"""
        duckdb_venv_path = self.project._duckdb_venv_path
        staging_file = Path(duckdb_venv_path + ".stg")
        target_file = Path(duckdb_venv_path)
                
        while True:
            try:
                if staging_file.exists():
                    try:
                        staging_file.replace(target_file)
                        self.logger.info("Successfully renamed staging database to virtual environment database")
                    except OSError:
                        # Silently continue if file cannot be renamed (will retry next iteration)
                        pass
                
            except Exception as e:
                # Log any unexpected errors but keep running
                self.logger.error(f"Error in monitoring {c.DUCKDB_VENV_FILE + '.stg'}: {str(e)}")
            
            await asyncio.sleep(1)  # Check every second
    
    @asynccontextmanager
    async def _run_background_tasks(self, app: FastAPI):
        task = asyncio.create_task(self._monitor_for_staging_file())
        
        async with contextlib.AsyncExitStack() as stack:
            await stack.enter_async_context(self.mcp.session_manager.run())
            yield
        
        task.cancel()


    def _get_tags_metadata(self) -> list[dict]:
        tags_metadata = [
            {
                "name": "Project Metadata",
                "description": "Get information on project such as name, version, and other API endpoints",
            },
            {
                "name": "Data Management",
                "description": "Actions to update the data components of the project",
            }
        ]

        for dataset_name in self.manifest_cfg.datasets:
            tags_metadata.append({
                "name": f"Dataset '{dataset_name}'",
                "description": f"Get parameters or results for dataset '{dataset_name}'",
            })
        
        for dashboard_name in self.dashboards:
            tags_metadata.append({
                "name": f"Dashboard '{dashboard_name}'",
                "description": f"Get parameters or results for dashboard '{dashboard_name}'",
            })
        
        tags_metadata.extend([
            {
                "name": "Authentication",
                "description": "Submit authentication credentials and authorize with a session cookie",
            },
            {
                "name": "User Management",
                "description": "Manage users and their attributes",
            },
            {
                "name": "OAuth2",
                "description": "Authorize and get token using the OAuth2 protocol",
            },
        ])
        return tags_metadata
    

    def run(self, uvicorn_args: Namespace) -> None:
        """
        Runs the API server with uvicorn for CLI "squirrels run"

        Arguments:
            uvicorn_args: List of arguments to pass to uvicorn.run. Currently only supports "host" and "port"
        """
        start = time.time()
        
        squirrels_version_path = f'/api/squirrels-v{sq_major_version}'
        project_name = u.normalize_name_for_api(self.manifest_cfg.project_variables.name)
        project_version = f"v{self.manifest_cfg.project_variables.major_version}"
        project_metadata_path = squirrels_version_path + f"/project/{project_name}/{project_version}"
        
        param_fields = self.param_cfg_set.get_all_api_field_info()

        tags_metadata = self._get_tags_metadata()
        
        app = FastAPI(
            title=f"Squirrels APIs for '{self.manifest_cfg.project_variables.label}'", openapi_tags=tags_metadata,
            description="For specifying parameter selections to dataset APIs, you can choose between using query parameters with the GET method or using request body with the POST method",
            lifespan=self._run_background_tasks,
            openapi_url=project_metadata_path+"/openapi.json",
            docs_url=project_metadata_path+"/docs",
            redoc_url=project_metadata_path+"/redoc"
        )

        app.add_middleware(SessionMiddleware, secret_key=self.env_vars.get(c.SQRL_SECRET_KEY, ""), max_age=None)

        async def _log_request_run(request: Request) -> None:
            headers = dict(request.scope["headers"])
            request_id = uuid.uuid4().hex
            headers[b"x-request-id"] = request_id.encode()
            request.scope["headers"] = list(headers.items())

            try:
                body = await request.json()
            except Exception:
                body = None
            
            headers_dict = dict(request.headers)
            path, params = request.url.path, dict(request.query_params)
            path_with_params = f"{path}?{request.query_params}" if len(params) > 0 else path
            data = {"request_method": request.method, "request_path": path, "request_params": params, "request_headers": headers_dict, "request_body": body}
            info = {"request_id": request_id}
            self.logger.info(f'Running request: {request.method} {path_with_params}', extra={"data": data, "info": info})

        @app.middleware("http")
        async def catch_exceptions_middleware(request: Request, call_next):
            buffer = io.StringIO()
            try:
                await _log_request_run(request)
                return await call_next(request)
            except InvalidInputError as exc:
                message = str(exc)
                self.logger.error(message)
                response = JSONResponse(
                    status_code=exc.status_code, content={"error": exc.error, "error_description": exc.error_description}
                )
            except FileExecutionError as exc:
                traceback.print_exception(exc.error, file=buffer)
                buffer.write(str(exc))
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected server error occurred", "blame": "Squirrels project"}
                )
            except ConfigurationError as exc:
                traceback.print_exc(file=buffer)
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected server error occurred", "blame": "Squirrels project"}
                )
            except Exception as exc:
                traceback.print_exc(file=buffer)
                response = JSONResponse(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": f"An unexpected server error occurred", "blame": "Squirrels framework"}
                )
            
            err_msg = buffer.getvalue()
            if err_msg:
                self.logger.error(err_msg)
                print(err_msg)
            return response

        # Configure CORS with smart credential handling
        # Get allowed origins for credentials from environment variable
        credential_origins_env = self.env_vars.get(c.SQRL_AUTH_CREDENTIAL_ORIGINS, "https://squirrels-analytics.github.io")
        allowed_credential_origins = [origin.strip() for origin in credential_origins_env.split(",") if origin.strip()]
        
        app.add_middleware(SmartCORSMiddleware, allowed_credential_origins=allowed_credential_origins)
        
        # Setup route modules
        self.oauth2_routes.setup_routes(app)
        self.auth_routes.setup_routes(app)
        get_parameters_definition = self.project_routes.setup_routes(app, self.mcp, project_metadata_path, project_name, project_version, param_fields)
        self.data_management_routes.setup_routes(app, project_metadata_path, param_fields)
        self.dataset_routes.setup_routes(app, self.mcp, project_metadata_path, project_name, param_fields, get_parameters_definition)
        self.dashboard_routes.setup_routes(app, project_metadata_path, param_fields, get_parameters_definition)
        app.mount(project_metadata_path, self.mcp.streamable_http_app())
    
        # Add Root Path Redirection to Squirrels Studio
        full_hostname = f"http://{uvicorn_args.host}:{uvicorn_args.port}"
        encoded_hostname = urllib.parse.quote(full_hostname, safe="")
        squirrels_studio_params = f"host={encoded_hostname}&projectName={project_name}&projectVersion={project_version}"
        squirrels_studio_url = f"https://squirrels-analytics.github.io/squirrels-studio/#/login?{squirrels_studio_params}"
        
        @app.get("/", include_in_schema=False)
        async def redirect_to_studio():
            return RedirectResponse(url=squirrels_studio_url)

        # Run the API Server
        import uvicorn
        
        print("\nWelcome to the Squirrels Data Application!\n")
        print(f"- Application UI: {squirrels_studio_url}")
        print(f"- API Docs (with ReDoc): {full_hostname}{project_metadata_path}/redoc")
        print(f"- API Docs (with Swagger UI): {full_hostname}{project_metadata_path}/docs")
        print()
        
        self.logger.log_activity_time("creating app server", start)
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port)
    