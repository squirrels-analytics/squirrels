from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.security import HTTPBearer
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
from contextlib import asynccontextmanager
from argparse import Namespace
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware
from mcp.server.fastmcp import FastMCP
import io, time, mimetypes, traceback, uuid, asyncio, contextlib

from . import _constants as c, _utils as u, _parameter_sets as ps
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
    
    def __init__(self, app, allowed_credential_origins: list[str], configurables_as_headers: list[str]):
        super().__init__(app)
        # Origins that are allowed to send credentials (cookies, auth headers)
        self.allowed_credential_origins = allowed_credential_origins
        self.allowed_request_headers = ",".join(["Authorization", "Content-Type"] + configurables_as_headers)
    
    async def dispatch(self, request: Request, call_next):
        origin = request.headers.get("origin")
        
        # Handle preflight requests
        if request.method == "OPTIONS":
            response = StarletteResponse(status_code=200) 
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = self.allowed_request_headers
        
        else:
            # Call the next middleware/route
            response: StarletteResponse = await call_next(request)
            
            # Always expose the Applied-Username header
            response.headers["Access-Control-Expose-Headers"] = "Applied-Username"
        
        if origin:
            scheme = u.get_scheme(request.url.hostname)
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
        
        self.mcp = FastMCP(
            name="Squirrels",
            stateless_http=True
        )

        # Initialize route modules
        get_bearer_token = HTTPBearer(auto_error=False)
        self.oauth2_routes = OAuth2Routes(get_bearer_token, project, no_cache)
        self.auth_routes = AuthRoutes(get_bearer_token, project, no_cache)
        self.project_routes = ProjectRoutes(get_bearer_token, project, no_cache)
        self.dataset_routes = DatasetRoutes(get_bearer_token, project, no_cache)
        self.dashboard_routes = DashboardRoutes(get_bearer_token, project, no_cache)
        self.data_management_routes = DataManagementRoutes(get_bearer_token, project, no_cache)
    
    
    async def _refresh_datasource_params(self) -> None:
        """
        Background task to periodically refresh datasource parameter options.
        Runs every N minutes as configured by SQRL_PARAMETERS__DATASOURCE_REFRESH_MINUTES (default: 60).
        """
        refresh_minutes_str = self.env_vars.get(c.SQRL_PARAMETERS_DATASOURCE_REFRESH_MINUTES, "60")
        try:
            refresh_minutes = int(refresh_minutes_str)
            if refresh_minutes <= 0:
                self.logger.info(f"The value of {c.SQRL_PARAMETERS_DATASOURCE_REFRESH_MINUTES} is: {refresh_minutes_str} minutes")
                self.logger.info(f"Datasource parameter refresh is disabled since the refresh interval is not positive.")
                return
        except ValueError:
            self.logger.warning(f"Invalid value for {c.SQRL_PARAMETERS_DATASOURCE_REFRESH_MINUTES}: {refresh_minutes_str}. Must be an integer. Disabling datasource parameter refresh.")
            return
        
        refresh_seconds = refresh_minutes * 60
        self.logger.info(f"Starting datasource parameter refresh background task (every {refresh_minutes} minutes)")
        
        while True:
            try:
                await asyncio.sleep(refresh_seconds)
                self.logger.info("Refreshing datasource parameter options...")
                
                # Fetch fresh dataframes from datasources in a thread pool to avoid blocking
                loop = asyncio.get_running_loop()
                default_conn_name = self.manifest_cfg.env_vars.get(c.SQRL_CONNECTIONS_DEFAULT_NAME_USED, "default")
                df_dict = await loop.run_in_executor(
                    None,
                    ps.ParameterConfigsSetIO._get_df_dict_from_data_sources,
                    self.param_cfg_set,
                    default_conn_name,
                    self.seeds,
                    self.conn_set,
                    self.project._datalake_db_path
                )
                
                # Re-convert datasource parameters with fresh data
                self.param_cfg_set._post_process_params(df_dict)
                
                self.logger.info("Successfully refreshed datasource parameter options")
            except asyncio.CancelledError:
                self.logger.info("Datasource parameter refresh task cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error refreshing datasource parameter options: {e}", exc_info=True)
                # Continue the loop even if there's an error
    
    @asynccontextmanager
    async def _run_background_tasks(self, app: FastAPI):
        refresh_datasource_task = asyncio.create_task(self._refresh_datasource_params())
        
        async with contextlib.AsyncExitStack() as stack:
            await stack.enter_async_context(self.mcp.session_manager.run())
            yield
        
        refresh_datasource_task.cancel()


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
        
        squirrels_version_path = f'/api/squirrels/v{sq_major_version}'
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

        app.add_middleware(SessionMiddleware, secret_key=self.env_vars.get(c.SQRL_SECRET_KEY, ""), max_age=None, same_site="none", https_only=True)

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
                # print(err_msg)
            return response

        # Configure CORS with smart credential handling
        # Get allowed origins for credentials from environment variable
        credential_origins_env = self.env_vars.get(c.SQRL_AUTH_CREDENTIAL_ORIGINS, "https://squirrels-analytics.github.io")
        allowed_credential_origins = [origin.strip() for origin in credential_origins_env.split(",") if origin.strip()]
        configurables_as_headers = [f"x-config-{u.normalize_name_for_api(name)}" for name in self.manifest_cfg.configurables.keys()]
        
        app.add_middleware(SmartCORSMiddleware, allowed_credential_origins=allowed_credential_origins, configurables_as_headers=configurables_as_headers)
        
        # Setup route modules
        self.oauth2_routes.setup_routes(app, squirrels_version_path)
        self.auth_routes.setup_routes(app, squirrels_version_path)
        get_parameters_definition = self.project_routes.setup_routes(app, self.mcp, project_metadata_path, project_name, project_version, param_fields)
        self.data_management_routes.setup_routes(app, project_metadata_path, param_fields)
        self.dataset_routes.setup_routes(app, self.mcp, project_metadata_path, project_name, project_version, param_fields, get_parameters_definition)
        self.dashboard_routes.setup_routes(app, project_metadata_path, param_fields, get_parameters_definition)
        app.mount(project_metadata_path, self.mcp.streamable_http_app())
    
        # Add Root Path Redirection to Squirrels Studio
        full_hostname = f"http://{uvicorn_args.host}:{uvicorn_args.port}"
        squirrels_studio_path = f"/project/{project_name}/{project_version}/studio"
        templates = Jinja2Templates(directory=str(Path(__file__).parent / "_package_data" / "templates"))

        @app.get(squirrels_studio_path, include_in_schema=False)
        async def squirrels_studio():
            default_studio_path = "https://squirrels-analytics.github.io/squirrels-studio-v1"
            sqrl_studio_base_url = self.env_vars.get(c.SQRL_STUDIO_BASE_URL, default_studio_path)
            context = {
                "sqrl_studio_base_url": sqrl_studio_base_url,
                "project_name": project_name,
                "project_version": project_version,
            }
            return HTMLResponse(content=templates.get_template("squirrels_studio.html").render(context))
        
        @app.get("/", include_in_schema=False)
        async def redirect_to_studio():
            return RedirectResponse(url=squirrels_studio_path)

        self.logger.log_activity_time("creating app server", start)

        # Run the API Server
        import uvicorn
        
        print("\nWelcome to the Squirrels Data Application!\n")
        print(f"- Application UI: {full_hostname}{squirrels_studio_path}")
        print(f"- API Docs (with ReDoc): {full_hostname}{project_metadata_path}/redoc")
        print(f"- API Docs (with Swagger UI): {full_hostname}{project_metadata_path}/docs")
        print(f"- MCP Server URL: {full_hostname}{project_metadata_path}/mcp")
        print()
        
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port, proxy_headers=True, forwarded_allow_ips="*")
