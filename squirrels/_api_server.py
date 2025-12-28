from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.security import HTTPBearer
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
from contextlib import asynccontextmanager
from argparse import Namespace
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware
import io, time, mimetypes, traceback, asyncio

from . import _constants as c, _utils as u, _parameter_sets as ps
from ._exceptions import InvalidInputError, ConfigurationError, FileExecutionError
from ._version import __version__, sq_major_version
from ._project import SquirrelsProject
from ._request_context import set_request_id
from ._mcp_server import McpServerBuilder

# Import route modules
from ._api_routes.auth import AuthRoutes
from ._api_routes.project import ProjectRoutes
from ._api_routes.datasets import DatasetRoutes
from ._api_routes.dashboards import DashboardRoutes
from ._api_routes.data_management import DataManagementRoutes

# # Disabled for now, a 'bring your own OAuth2 server' approach will be provided in the future
# from ._api_routes.oauth2 import OAuth2Routes 

mimetypes.add_type('application/javascript', '.js')


class SmartCORSMiddleware(BaseHTTPMiddleware):
    """
    Custom CORS middleware that allows specific origins to use credentials
    while still allowing all other origins without credentials.
    """
    
    def __init__(self, app, allowed_credential_origins: list[str], configurables_as_headers: list[str]):
        super().__init__(app)

        allowed_predefined_headers = ["Authorization", "Content-Type", "x-api-key"]
        
        self.allowed_credential_origins = allowed_credential_origins
        self.allowed_request_headers = ",".join(allowed_predefined_headers + configurables_as_headers)
    
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
            # No origin header (probably a non-browser request)
            response.headers["Access-Control-Allow-Origin"] = "*"
        
        return response


class ApiServer:
    def __init__(self, no_cache: bool, project: SquirrelsProject) -> None:
        """
        Constructor for ApiServer

        Arguments:
            no_cache (bool): Whether to disable caching
        """
        self.project = project
        self.logger = project._logger
        self.env_vars = project._env_vars
        self.manifest_cfg = project._manifest_cfg
        self.seeds = project._seeds
        self.conn_set = project._conn_set
        self.param_cfg_set = project._param_cfg_set
        self.dashboards = project._dashboards
        
        # Initialize route modules
        get_bearer_token = HTTPBearer(auto_error=False)
        # self.oauth2_routes = OAuth2Routes(get_bearer_token, project, no_cache)
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
        refresh_minutes = self.env_vars.parameters_datasource_refresh_minutes
        if refresh_minutes <= 0:
            self.logger.info(f"The value of {c.SQRL_PARAMETERS_DATASOURCE_REFRESH_MINUTES} is: {refresh_minutes} minutes")
            self.logger.info(f"Datasource parameter refresh is disabled since the refresh interval is not positive.")
            return
        
        refresh_seconds = refresh_minutes * 60
        self.logger.info(f"Starting datasource parameter refresh background task (every {refresh_minutes} minutes)")
        
        default_conn_name = self.env_vars.connections_default_name_used
        while True:
            try:
                await asyncio.sleep(refresh_seconds)
                self.logger.info("Refreshing datasource parameter options...")
                
                # Fetch fresh dataframes from datasources in a thread pool to avoid blocking
                loop = asyncio.get_running_loop()
                df_dict = await loop.run_in_executor(
                    None,
                    ps.ParameterConfigsSetIO._get_df_dict_from_data_sources,
                    self.param_cfg_set,
                    default_conn_name,
                    self.seeds,
                    self.conn_set,
                    self.project._vdl_catalog_db_path
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
            }
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
        project_name = self.manifest_cfg.project_variables.name
        project_name_for_api = u.normalize_name_for_api(project_name)
        project_label = self.manifest_cfg.project_variables.label
        project_version = f"v{self.manifest_cfg.project_variables.major_version}"
        project_name_version_path = f"/project/{project_name_for_api}/{project_version}"
        project_metadata_path = squirrels_version_path + project_name_version_path
        
        param_fields = self.param_cfg_set.get_all_api_field_info()

        tags_metadata = self._get_tags_metadata()
        
        openapi_url = project_name_version_path+"/openapi.json"
        docs_url = project_name_version_path+"/docs"
        redoc_url = project_name_version_path+"/redoc"

        # Container for MCP builder - will be populated after setup_routes
        mcp_container: dict[str, McpServerBuilder] = {}
        
        @asynccontextmanager
        async def app_lifespan(app: FastAPI):
            """App lifespan that includes MCP server lifecycle and background tasks."""
            mcp_builder = mcp_container.get("mcp_builder")
            refresh_datasource_task = asyncio.create_task(self._refresh_datasource_params())
            
            if mcp_builder:
                async with mcp_builder.lifespan():
                    yield
            else:
                yield
            
            refresh_datasource_task.cancel()

        app = FastAPI(
            title=f"Squirrels APIs for '{project_label}'", openapi_tags=tags_metadata,
            description="For specifying parameter selections to dataset APIs, you can choose between using query parameters with the GET method or using request body with the POST method",
            lifespan=app_lifespan,
            openapi_url=openapi_url,
            docs_url=docs_url,
            redoc_url=redoc_url
        )

        app.add_middleware(SessionMiddleware, secret_key=self.env_vars.secret_key, max_age=None, same_site="none", https_only=True)

        async def _log_request_run(request: Request) -> None:
            try:
                body = await request.json()
            except Exception:
                body = None  # Non-JSON payloads may contain sensitive information, so we don't log them

            partial_headers: dict[str, str] = {}
            for header in request.headers.keys():
                if header.startswith("x-") and header not in ["x-api-key"]:
                    partial_headers[header] = request.headers[header]
            
            path, params = request.url.path, dict(request.query_params)
            path_with_params = f"{path}?{request.query_params}" if len(params) > 0 else path
            data = {"request_method": request.method, "request_path": path, "request_params": params, "request_body": body, "partial_headers": partial_headers}
            self.logger.info(f'Running request: {request.method} {path_with_params}', data=data)

        @app.middleware("http")
        async def catch_exceptions_middleware(request: Request, call_next):
            # Generate and set request ID for this request
            request_id = set_request_id()
            
            buffer = io.StringIO()
            try:
                await _log_request_run(request)
                response = await call_next(request)
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
            
            # Add request ID to response header
            response.headers["X-Request-ID"] = request_id
            
            return response

        # Configure CORS with smart credential handling
        # Get allowed origins for credentials from environment variable
        allowed_credential_origins = self.env_vars.auth_credential_origins
        
        # Allow both underscore and dash versions of configurable headers
        configurables_as_headers = []
        for name in self.manifest_cfg.configurables.keys():
            configurables_as_headers.append(f"x-config-{name}")  # underscore version
            configurables_as_headers.append(f"x-config-{u.normalize_name_for_api(name)}")  # dash version
        
        app.add_middleware(SmartCORSMiddleware, allowed_credential_origins=allowed_credential_origins, configurables_as_headers=configurables_as_headers)
        
        # Setup route modules
        # self.oauth2_routes.setup_routes(app, squirrels_version_path)
        self.auth_routes.setup_routes(app, squirrels_version_path)
        get_parameters_definition = self.project_routes.setup_routes(
            app, project_metadata_path, project_name_version_path, project_name, project_version, param_fields
        )
        self.data_management_routes.setup_routes(app, project_metadata_path, param_fields)
        self.dataset_routes.setup_routes(app, project_metadata_path, param_fields, get_parameters_definition)
        self.dashboard_routes.setup_routes(app, project_metadata_path, param_fields, get_parameters_definition)

        # Build the MCP server now that route methods are available
        max_rows_for_ai = self.env_vars.datasets_max_rows_for_ai
        mcp_builder = McpServerBuilder(
            project_name=project_name,
            project_label=project_label,
            max_rows_for_ai=max_rows_for_ai,
            get_user_from_headers=self.project_routes.get_user_from_headers,
            get_data_catalog_for_mcp=self.project_routes._get_data_catalog_for_mcp,
            get_dataset_parameters_for_mcp=self.dataset_routes._get_dataset_parameters_for_mcp,
            get_dataset_results_for_mcp=self.dataset_routes._get_dataset_results_for_mcp,
        )
        mcp_container["mcp_builder"] = mcp_builder
        mcp_app = mcp_builder.get_asgi_app()

        # Mount static files from public directory if it exists
        # This allows users to serve static assets (images, CSS, JS, etc.) from {project_path}/public/
        public_dir = Path(self.project._project_path) / c.PUBLIC_FOLDER
        if public_dir.exists() and public_dir.is_dir():
            app.mount("/public", StaticFiles(directory=str(public_dir)), name="public")
            self.logger.info(f"Mounted static files from: {public_dir}")
    
        # Add Root Path Redirection to Squirrels Studio
        full_hostname = f"http://{uvicorn_args.host}:{uvicorn_args.port}"
        squirrels_studio_path = f"{project_name_version_path}/studio"
        templates = Jinja2Templates(directory=str(Path(__file__).parent / "_package_data" / "templates"))

        @app.get(squirrels_studio_path, include_in_schema=False)
        async def squirrels_studio():
            sqrl_studio_base_url = self.env_vars.studio_base_url
            context = {
                "sqrl_studio_base_url": sqrl_studio_base_url,
                "project_name": project_name_for_api,
                "project_version": project_version,
            }
            return HTMLResponse(content=templates.get_template("squirrels_studio.html").render(context))
        
        @app.get("/", include_in_schema=False)
        async def redirect_to_studio():
            return RedirectResponse(url=squirrels_studio_path)

        # Mount MCP server
        app.add_route(f"{project_name_version_path}/mcp", mcp_app, methods=["GET", "POST"])
        app.add_route("/mcp", mcp_app, methods=["GET", "POST"])
    
        self.logger.log_activity_time("creating app server", start)

        # Run the API Server
        import uvicorn
        
        # Print welcome banner
        banner_width = 80
        print()
        print("═" * banner_width)
        print("👋  WELCOME TO SQUIRRELS!".center(banner_width))
        print("═" * banner_width)
        print()
        print(" 🖥️  Application UI")
        print(f"  └─ Squirrels Studio: {full_hostname}{squirrels_studio_path}")
        print(f"     └─ (The following URL also redirects to studio: {full_hostname})")
        print()
        print(" 🔌 MCP Server URLs")
        print(f"  ├─ Option 1:         {full_hostname}{project_name_version_path}/mcp")
        print(f"  └─ Option 2:         {full_hostname}/mcp")
        print()
        print(" 📖 API Documentation")
        print(f"  ├─ Swagger UI:       {full_hostname}{docs_url}")
        print(f"  ├─ ReDoc UI:         {full_hostname}{redoc_url}")
        print(f"  └─ OpenAPI Spec:     {full_hostname}{openapi_url}")
        print()
        print("─" * banner_width)
        print("✨ Server is running! Press CTRL+C to stop.".center(banner_width))
        print("─" * banner_width)
        print()
        
        uvicorn.run(app, host=uvicorn_args.host, port=uvicorn_args.port, proxy_headers=True, forwarded_allow_ips="*")
