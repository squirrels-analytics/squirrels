from typing import Optional
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
import secrets

from . import _utils as u, _constants as c
from ._py_module import PyModule
from .user_base import User, WrongPassword
from ._environcfg import EnvironConfigIO
from ._manifest import DatasetScope


class Authenticator:
    
    @classmethod
    def get_auth_helper(cls, default_auth_helper = None):
        auth_module_path = u.join_paths(c.PYCONFIG_FOLDER, c.AUTH_FILE)
        return PyModule(auth_module_path, default_class=default_auth_helper)

    def __init__(self, token_expiry_minutes: int, auth_helper = None) -> None:
        self.token_expiry_minutes = token_expiry_minutes
        self.auth_helper = self.get_auth_helper(auth_helper)
        self.secret_key = self._get_secret_key()
        self.algorithm = "HS256"

    def _get_secret_key(self):
        secret_key = EnvironConfigIO.obj.get_secret(c.JWT_SECRET_KEY, default_factory=lambda: secrets.token_hex(32))
        return secret_key

    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        user_cls = self.auth_helper.get_func_or_class("User", default_attr=User)
        get_user = self.auth_helper.get_func_or_class(c.GET_USER_FUNC, is_required=False)
        try:
            real_user = get_user(username, password) if get_user is not None else None
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.GET_USER_FUNC}" in {c.AUTH_FILE}', e)
        
        if isinstance(real_user, User):
            return real_user
        
        if not isinstance(real_user, WrongPassword):
            fake_users = EnvironConfigIO.obj.get_users()
            if username in fake_users and secrets.compare_digest(fake_users[username][c.USER_PWD_KEY], password):
                is_internal = fake_users[username].get("is_internal", False)
                user: User = user_cls(username, is_internal=is_internal)
                try:
                    return user.with_attributes(fake_users[username])
                except Exception as e:
                    raise u.FileExecutionError(f'Failed to create user from User model in {c.AUTH_FILE}', e)
        
        return None
    
    def create_access_token(self, user: User) -> str:
        expire = datetime.now(timezone.utc) + timedelta(minutes=self.token_expiry_minutes)
        to_encode = {**vars(user), "exp": expire}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt, expire
    
    def get_user_from_token(self, token: Optional[str]) -> Optional[User]:
        if token is not None:
            try:
                payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
                payload.pop("exp")
                user_cls: User = self.auth_helper.get_func_or_class("User", default_attr=User)
                return user_cls._FromDict(payload)
            except JWTError:
                return None

    def can_user_access_scope(self, user: Optional[User], scope: DatasetScope) -> bool:
        if user is None:
            user_level = DatasetScope.PUBLIC
        elif not user.is_internal:
            user_level = DatasetScope.PROTECTED
        else:
            user_level = DatasetScope.PRIVATE
        
        return user_level.value >= scope.value
