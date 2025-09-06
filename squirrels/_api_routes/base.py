"""
Base utilities and dependencies for API routes
"""
from typing import Any, Mapping, TypeVar, Callable, Coroutine
from fastapi import Request, Response, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.templating import Jinja2Templates
from cachetools import TTLCache
from pydantic import BaseModel, create_model
from mcp.server.fastmcp import Context
from pathlib import Path
from datetime import datetime, timezone

from .. import _utils as u, _constants as c
from .._exceptions import InvalidInputError, ConfigurationError
from .._project import SquirrelsProject

T = TypeVar('T')


class RouteBase:
    """Base class for route modules providing common functionality"""
    
    def __init__(self, get_bearer_token: HTTPBearer, project: SquirrelsProject, no_cache: bool = False):
        self.project = project
        self.no_cache = no_cache
        self.logger = project._logger
        self.env_vars = project._env_vars
        self.manifest_cfg = project._manifest_cfg
        self.authenticator = project._auth
        self.param_cfg_set = project._param_cfg_set
        
        # Setup templates
        template_dir = Path(__file__).parent.parent / "_package_data" / "templates"
        self.templates = Jinja2Templates(directory=str(template_dir))
    
        # Create user models
        fields_without_username = {
            k: (v.annotation, v.default) 
            for k, v in self.authenticator.User.model_fields.items() 
            if k != "username"
        }
        self.UserModel = create_model("UserModel", __base__=BaseModel, **fields_without_username) # type: ignore
        self.UserInfoModel = create_model("UserInfoModel", __base__=self.UserModel, username=str)

        class UserInfoModel(self.UserInfoModel):
            username: str

            def __hash__(self):
                return hash(self.username)
        
        # Authorization dependency for current user
        def get_token_from_session(request: Request) -> str | None:
            expiry = request.session.get("access_token_expiry")
            datetime_now = datetime.now(timezone.utc).timestamp()
            if expiry and expiry > datetime_now:
                return request.session.get("access_token")
            else:
                request.session.pop("access_token", None)
                request.session.pop("access_token_expiry", None)
                return None
        
        async def get_current_user(
            request: Request, response: Response, auth: HTTPAuthorizationCredentials = Depends(get_bearer_token)
        ) -> UserInfoModel | None:
            token = auth.credentials if auth and auth.scheme == "Bearer" else None
            final_token = token if token else get_token_from_session(request)
            user = self.authenticator.get_user_from_token(final_token)
            username = "" if user is None else user.username
            response.headers["Applied-Username"] = username
            return UserInfoModel(**user.model_dump(mode='json')) if user else None

        self.get_current_user = get_current_user
        
    @property
    def _parameters_description(self) -> str:
        """Get the standard parameters description"""
        return "Selections of one parameter may cascade the available options in another parameter. " \
                "For example, if the dataset has parameters for 'country' and 'city', available options for 'city' would " \
                "depend on the selected option 'country'. If a parameter has 'trigger_refresh' as true, provide the parameter " \
                "selection to this endpoint whenever it changes to refresh the parameter options of children parameters."
        
    def _validate_request_params(self, all_request_params: Mapping, params: Mapping) -> None:
        """Validate request parameters"""
        if params.get("x_verify_params", False):
            invalid_params = [
                param for param in all_request_params if param not in params
            ]
            if invalid_params:
                raise InvalidInputError(400, "Invalid query parameters", f"Invalid query parameters: {', '.join(invalid_params)}")
    
    def get_selections_as_immutable(self, params: Mapping, uncached_keys: set[str]) -> tuple[tuple[str, Any], ...]:
        """Convert selections into a cachable tuple of pairs"""
        selections = list()
        for key, val in params.items():
            if key in uncached_keys or val is None:
                continue
            if isinstance(val, (list, tuple)):
                if len(val) == 1:  # for backward compatibility
                    val = val[0]
                else:
                    val = tuple(val)
            selections.append((u.normalize_name(key), val))
        return tuple(selections)

    async def do_cachable_action(self, cache: TTLCache, action: Callable[..., Coroutine[Any, Any, T]], *args) -> T:
        """Execute a cachable action"""
        cache_key = tuple(args)
        result = cache.get(cache_key)
        if result is None:
            result = await action(*args)
            cache[cache_key] = result
        return result
    
    def _get_request_id(self, request: Request) -> str:
        """Get request ID from headers"""
        return request.headers.get("x-request-id", "")
    
    def log_activity_time(self, activity: str, start_time: float, request: Request) -> None:
        """Log activity time"""
        self.logger.log_activity_time(activity, start_time, request_id=self._get_request_id(request))

    def get_name_from_path_section(self, request: Request, section: int) -> str:
        """Extract name from request path section"""
        url_path: str = request.scope['route'].path
        name_raw = url_path.split('/')[section]
        return u.normalize_name(name_raw)

    def _get_access_token_expiry_minutes(self) -> int:
        """Get access token expiry minutes"""
        expiry_mins = self.env_vars.get(c.SQRL_AUTH_TOKEN_EXPIRE_MINUTES, 30)
        try:
            expiry_mins = int(expiry_mins)
        except ValueError:
            raise ConfigurationError(f"Value for environment variable {c.SQRL_AUTH_TOKEN_EXPIRE_MINUTES} is not an integer, got: {expiry_mins}")
        return expiry_mins
    
    def get_headers_from_tool_ctx(self, tool_ctx: Context) -> dict[str, str]:
        request = tool_ctx.request_context.request
        assert request is not None and hasattr(request, "headers")
        headers: dict[str, str] = request.headers
        return headers

    def get_configurables_from_headers(self, headers: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        """Extract configurables from request headers with prefix 'x-config-'."""
        prefix = "x-config-"
        cfg_pairs: list[tuple[str, str]] = []
        for key, value in headers.items():
            key_lower = str(key).lower()
            if key_lower.startswith(prefix):
                cfg_name = key_lower[len(prefix):]
                cfg_pairs.append((u.normalize_name(cfg_name), str(value)))
        
        self.logger.info(f"Configurables: {dict(cfg_pairs)}")
        print(f"Configurables: {dict(cfg_pairs)}")
        return tuple(cfg_pairs)
    
    def get_user_from_tool_headers(self, headers: dict[str, str]):
        authorization_header = headers.get('Authorization')
        if authorization_header:
            parts = authorization_header.split()
            if len(parts) == 2 and parts[0] == 'Bearer':
                access_token = parts[1]
                user = self.authenticator.get_user_from_token(access_token)
                return user
            else:
                raise ValueError("Invalid Authorization header format")
        else:
            return None
