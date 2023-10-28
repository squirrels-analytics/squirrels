from typing import Optional
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from squirrels import _utils, _constants as c


class UserBase:
    def __init__(self, username, **kwargs):
        self.username = username

class UserPwd:
    def __init__(self, user: UserBase, hashed_password: str, **kwargs):
        self.user = user
        self.hashed_password = hashed_password


class Authenticator:
    def __init__(self, token_expiry_minutes: int, auth_helper = None) -> None:
        self.token_expiry_minutes = token_expiry_minutes
        self.auth_helper = _utils.import_file_as_module(c.AUTH_FILE) if auth_helper is None else auth_helper
        self.public_key = '9f6e5dfbffe8559c5551f91473fb0a26c06e23bd589f04a97180f3219103ca63'
        self.algorithm = "HS256"

    def authenticate_user(self, username: str, password: str) -> Optional[UserBase]:
        user_pwd: UserPwd = self.auth_helper.get_user_and_hashed_pwd(username)
        if not user_pwd:
            return None
        if not self.auth_helper.verify_pwd(password, user_pwd.hashed_password):
            return None
        return user_pwd.user
    
    def create_access_token(self, user: UserBase) -> str:
        expire = datetime.utcnow() + timedelta(minutes=self.token_expiry_minutes)
        to_encode = {**vars(user), "exp": expire}
        encoded_jwt = jwt.encode(to_encode, self.public_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def get_user_from_token(self, token: str) -> UserBase:
        user: Optional[UserBase] = None
        try:
            payload = jwt.decode(token, self.public_key, algorithms=[self.algorithm])
            payload.pop("exp")
            user = self.auth_helper.User(**payload)
        except JWTError:
            pass
        
        return user
