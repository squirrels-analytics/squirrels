from typing import Optional
from datetime import datetime, timedelta, timezone
from jwt.exceptions import InvalidTokenError
import secrets, jwt

from . import _utils as u, _constants as c
from .arguments.run_time_args import AuthLoginArgs, AuthTokenArgs
from ._py_module import PyModule
from ._user_base import User, WrongPassword
from ._environcfg import EnvironConfig
from ._manifest import PermissionScope
from ._connection_set import ConnectionsArgs, ConnectionSet


class Authenticator:

    def __init__(self, base_path: str, env_cfg: EnvironConfig, conn_args: ConnectionsArgs, conn_set: ConnectionSet, token_expiry_minutes: int, *, auth_helper = None) -> None:
        self.env_cfg = env_cfg
        self.conn_args = conn_args
        self.conn_set = conn_set
        self.token_expiry_minutes = token_expiry_minutes
        self.auth_helper = self._get_auth_helper(base_path, default_auth_helper=auth_helper)
        self.secret_key = self._get_secret_key()
        self.algorithm = "HS256"
    
    def _get_auth_helper(self, base_path: str, *, default_auth_helper = None):
        auth_module_path = u.Path(base_path, c.PYCONFIGS_FOLDER, c.AUTH_FILE)
        return PyModule(auth_module_path, default_class=default_auth_helper)

    def _get_secret_key(self) -> str:
        secret_key = self.env_cfg.get_secret(c.JWT_SECRET_KEY, default_factory=lambda: secrets.token_hex(32))
        return str(secret_key)

    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        get_user = self.auth_helper.get_func_or_class(c.GET_USER_FROM_LOGIN_FUNC, is_required=False)
        try:
            sqrl = AuthLoginArgs(self.conn_args, self.conn_set.get_connections_as_dict(), username, password)
            real_user = get_user(sqrl) if get_user is not None else None
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.GET_USER_FROM_LOGIN_FUNC}" in {c.AUTH_FILE}', e) from e
        
        if isinstance(real_user, User):
            return real_user
        
        if not isinstance(real_user, WrongPassword):
            fake_users = self.env_cfg.get_users()
            if username in fake_users and secrets.compare_digest(fake_users[username].password, password):
                fake_user = fake_users[username].model_dump()
                fake_user.pop("username", "")
                fake_user.pop("password", "")
                is_internal = fake_user.pop("is_internal", False)
                try:
                    return User.Create(username, is_internal=is_internal, **fake_user)
                except Exception as e:
                    raise u.FileExecutionError(f'Failed to create user from User model in {c.AUTH_FILE}', e) from e
        
        return None
    
    def create_access_token(self, user: User) -> tuple[str, datetime]:
        expire = datetime.now(timezone.utc) + timedelta(minutes=self.token_expiry_minutes)
        to_encode = {**vars(user), "exp": expire}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt, expire
    
    def get_user_from_token(self, token: Optional[str]) -> Optional[User]:
        if token is None:
            return None
        
        get_user = self.auth_helper.get_func_or_class(c.GET_USER_FROM_TOKEN_FUNC, is_required=False)
        if get_user is not None:
            try:
                sqrl = AuthTokenArgs(self.conn_args, self.conn_set.get_connections_as_dict(), token)
                user = get_user(sqrl)
                if isinstance(user, User):
                    return user
                else:
                    raise u.ConfigurationError(f'The "{c.GET_USER_FROM_TOKEN_FUNC}" function in {c.AUTH_FILE} must return a User instance')
            except Exception as e:
                raise u.FileExecutionError(f'Failed to run "{c.GET_USER_FROM_TOKEN_FUNC}" in {c.AUTH_FILE}', e) from e
        
        try:
            payload: dict = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            payload.pop("exp")
            return User._FromDict(payload)
        except InvalidTokenError:
            return None

    def can_user_access_scope(self, user: Optional[User], scope: PermissionScope) -> bool:
        if user is None:
            user_level = PermissionScope.PUBLIC
        elif not user.is_internal:
            user_level = PermissionScope.PROTECTED
        else:
            user_level = PermissionScope.PRIVATE
        
        return user_level.value >= scope.value
