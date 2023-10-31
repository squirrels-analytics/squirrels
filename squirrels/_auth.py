from typing import Optional
from datetime import datetime, timedelta
from typing import Dict, Any
from jose import JWTError, jwt

from squirrels import _utils, _constants as c


class UserBase:
    def __init__(self, username, *, is_internal = False, **kwargs):
        self.username = username
        self.is_internal = is_internal
    
    @classmethod
    def FromDict(cls, user_dict: Dict[str, Any]):
        user = cls()
        for key, val in user_dict.items():
            setattr(user, key, val)
        return user


class UserPwd:
    def __init__(self, user: UserBase, hashed_password: str, **kwargs):
        self.user = user
        self.hashed_password = hashed_password


def get_auth_helper():
    try:
        return _utils.import_file_as_module(c.AUTH_FILE) 
    except FileNotFoundError:
        return None


class Authenticator:
    def __init__(self, token_expiry_minutes: int, auth_helper = None) -> None:
        self.token_expiry_minutes = token_expiry_minutes
        self.auth_helper = get_auth_helper() if auth_helper is None else auth_helper
        self.public_key = '9f6e5dfbffe8559c5551f91473fb0a26c06e23bd589f04a97180f3219103ca63'
        self.algorithm = "HS256"

    def authenticate_user(self, username: str, password: str) -> Optional[UserBase]:
        if self.auth_helper is None:
            raise FileNotFoundError(f"File '{c.AUTH_FILE}' must exist to authenticate user")
        
        return self.auth_helper.get_user_if_valid(username, password)
    
    def create_access_token(self, user: UserBase) -> str:
        expire = datetime.utcnow() + timedelta(minutes=self.token_expiry_minutes)
        to_encode = {**vars(user), "exp": expire}
        encoded_jwt = jwt.encode(to_encode, self.public_key, algorithm=self.algorithm)
        return encoded_jwt, expire
    
    def get_user_from_token(self, token: Optional[str]) -> Optional[UserBase]:
        if token is not None:
            try:
                payload = jwt.decode(token, self.public_key, algorithms=[self.algorithm])
                payload.pop("exp")
                if self.auth_helper is not None:
                    return self.auth_helper.User.FromDict(payload)
            except JWTError:
                return None

    def can_user_access_scope(self, user: Optional[UserBase], scope: str) -> bool:
        user_level = 0 if user is None else (1 if not user.is_internal else 2)
        scope_level = 2 if scope == c.PRIVATE_SCOPE else (1 if scope == c.PROTECTED_SCOPE else 0)
        return user_level >= scope_level
