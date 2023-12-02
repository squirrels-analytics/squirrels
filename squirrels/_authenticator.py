from typing import Optional
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
import secrets

from . import _utils as u, _constants as c
from .user_base import UserBase, WrongPassword
from ._environcfg import EnvironConfigIO


class Authenticator:
    
    @classmethod
    def get_auth_helper(cls):
        auth_module_path = u.join_paths(c.PYCONFIG_FOLDER, c.AUTH_FILE)
        try:
            return u.import_file_as_module(auth_module_path)
        except FileNotFoundError:
            return None

    def __init__(self, token_expiry_minutes: int, auth_helper = None) -> None:
        self.token_expiry_minutes = token_expiry_minutes
        self.auth_helper = self.get_auth_helper() if auth_helper is None else auth_helper
        self.secret_key = self._get_secret_key()
        self.algorithm = "HS256"

    def _get_secret_key(self):
        secret_key = EnvironConfigIO.obj.get_secret(c.JWT_SECRET_KEY, default_factory=lambda: secrets.token_hex(32))
        return secret_key

    def authenticate_user(self, username: str, password: str) -> Optional[UserBase]:
        if self.auth_helper:
            user_cls = self.auth_helper.User
            real_user = self.auth_helper.get_user_if_valid(username, password)
        else:
            user_cls = UserBase
            real_user = None
        
        if isinstance(real_user, UserBase):
            return real_user
        
        if not isinstance(real_user, WrongPassword):
            fake_users = EnvironConfigIO.obj.get_users()
            if username in fake_users and secrets.compare_digest(fake_users[username][c.USER_PWD_KEY], password):
                is_internal = fake_users[username].get("is_internal", False)
                return user_cls(username, is_internal=is_internal).with_attributes(**fake_users[username])
        
        return None
    
    def create_access_token(self, user: UserBase) -> str:
        expire = datetime.now(timezone.utc) + timedelta(minutes=self.token_expiry_minutes)
        to_encode = {**vars(user), "exp": expire}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt, expire
    
    def get_user_from_token(self, token: Optional[str]) -> Optional[UserBase]:
        if token is not None:
            try:
                payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
                payload.pop("exp")
                if self.auth_helper is not None:
                    return self.auth_helper.User._FromDict(payload)
                else:
                    return UserBase._FromDict(payload)
            except JWTError:
                return None

    def can_user_access_scope(self, user: Optional[UserBase], scope: str) -> bool:
        user_level = 0 if user is None else (1 if not user.is_internal else 2)
        scope_level = 2 if scope == c.PRIVATE_SCOPE else (1 if scope == c.PROTECTED_SCOPE else 0)
        return user_level >= scope_level
