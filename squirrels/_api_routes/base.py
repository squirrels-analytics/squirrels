"""
Base utilities and dependencies for API routes
"""
from typing import Any, Mapping, TypeVar, Callable, Coroutine
from fastapi import Request, Response, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.templating import Jinja2Templates
from cachetools import TTLCache
from mcp.server.fastmcp import Context
from pathlib import Path
from datetime import datetime, timezone

from .. import _utils as u, _constants as c
from .._exceptions import InvalidInputError, ConfigurationError
from .._project import SquirrelsProject
from .._schemas.auth_models import GuestUser, RegisteredUser

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
        ) -> GuestUser | RegisteredUser:
            token = auth.credentials if auth and auth.scheme == "Bearer" else None
            access_token = token if token else get_token_from_session(request)
            api_key = request.headers.get("x-api-key")
            final_token = api_key if api_key else access_token

            user = self.authenticator.get_user_from_token(final_token)
            if user is None:
                user = self.project._guest_user
            
            response.headers["Applied-Username"] = user.username
            return user

        self.get_current_user = get_current_user
        
    @property
    def _parameters_description(self) -> str:
        """Get the standard parameters description"""
        return "Selections of one parameter may cascade the available options in another parameter. " \
                "For example, if the dataset has parameters for 'country' and 'city', available options for 'city' would " \
                "depend on the selected option 'country'. If a parameter has 'trigger_refresh' as true, provide the parameter " \
                "selection to this endpoint whenever it changes to refresh the parameter options of children parameters."
        
    def _validate_request_params(self, all_request_params: Mapping, params: Mapping, headers: Mapping[str, str]) -> None:
        """Validate request parameters
        
        When header 'x-verify-params' is set to a truthy value, ensure that all provided
        query/body parameters are part of the parsed params model. Falls back to legacy
        query param 'x_verify_params' for backward compatibility.
        """
        header_val = headers.get("x-verify-params")
        verify_params = u.to_bool(header_val) or u.to_bool(params.get("x_verify_params", False))
        if verify_params:
            invalid_params = [param for param in all_request_params if param not in params]
            if invalid_params:
                raise InvalidInputError(400, "invalid_query_parameters", f"Invalid query parameters: {', '.join(invalid_params)}")
    
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
        return dict(request.headers)

    def get_configurables_from_headers(self, headers: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        """Extract configurables from request headers with prefix 'x-config-'."""
        prefix = "x-config-"
        cfg_pairs: list[tuple[str, str]] = []
        seen_configurables: dict[str, str] = {}  # normalized_name -> header_name
        
        for key, value in headers.items():
            key_lower = str(key).lower()
            if key_lower.startswith(prefix):
                cfg_name_raw = key_lower[len(prefix):]
                cfg_name_normalized = u.normalize_name(cfg_name_raw)  # Convert to underscore convention
                
                # Check if we've already seen this configurable (with different header format)
                if cfg_name_normalized in seen_configurables:
                    existing_header = seen_configurables[cfg_name_normalized]
                    raise InvalidInputError(
                        400, "duplicate_configurable_header",
                        f"Only one header format is allowed for configurable '{cfg_name_normalized}'. "
                        f"Both '{existing_header}' and '{key_lower}' were provided."
                    )
                
                seen_configurables[cfg_name_normalized] = key_lower
                cfg_pairs.append((cfg_name_normalized, str(value)))
        
        configurables = [k for k, _ in cfg_pairs]
        self.logger.info(f"Configurables specified: {configurables}", data={"configurables_specified": configurables})
        return tuple(cfg_pairs)
    
    def get_user_from_tool_headers(self, headers: dict[str, str]):
        authorization_header = headers.get('Authorization')
        if authorization_header:
            parts = authorization_header.split()
            if len(parts) == 2 and parts[0] == 'Bearer':
                access_token = parts[1]
                user = self.authenticator.get_user_from_token(access_token)
                if user is None:
                    return self.project._guest_user
                return user
            else:
                raise ValueError("Invalid Authorization header format")
        else:
            return self.project._guest_user
