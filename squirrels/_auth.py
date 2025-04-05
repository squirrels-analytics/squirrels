from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import cached_property
from jwt.exceptions import InvalidTokenError
from passlib.context import CryptContext
from pydantic import BaseModel, ConfigDict, ValidationError
from pydantic_core import PydanticUndefined
from sqlalchemy import create_engine, Engine, func, inspect, text, ForeignKey
from sqlalchemy import Column, String, Integer, Float, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Mapped, mapped_column
import jwt, types, typing as _t, uuid

from ._manifest import PermissionScope
from ._py_module import PyModule
from ._exceptions import InvalidInputError, ConfigurationError
from . import _utils as u, _constants as c

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

reserved_fields = ["username", "is_admin"]
disallowed_fields = ["password", "password_hash", "created_at", "token_id", "exp"]

class BaseUser(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    username: str
    is_admin: bool = False
    
    @classmethod
    def dropped_columns(cls):
        return []
    
    def __hash__(self):
        return hash(self.username)

User = _t.TypeVar('User', bound=BaseUser)

class AccessToken(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    token_id: str
    title: str
    username: str
    created_at: datetime
    expires_at: datetime


class UserField(BaseModel):
    name: str
    type: str
    nullable: bool
    enum: list[str] | None
    default: _t.Any | None


class Authenticator(_t.Generic[User]):
    def __init__(self, logger: u.Logger, base_path: str, env_vars: dict[str, str], *, sa_engine: Engine | None = None, cls: type[User] | None = None):
        self.logger = logger
        self.env_vars = env_vars
        self.secret_key = self.env_vars.get(c.SQRL_SECRET_KEY)

        # Create a new declarative base for this instance
        self.Base = declarative_base()
        
        # Define DbBaseUser class for this instance
        class DbBaseUser(self.Base):
            __tablename__ = 'users'
            __table_args__ = {'extend_existing': True}
            username: Mapped[str] = mapped_column(primary_key=True)
            is_admin: Mapped[bool] = mapped_column(nullable=False, default=False)
            password_hash: Mapped[str] = mapped_column(nullable=False)
            created_at: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
        
        # Define DbAccessToken class for this instance
        class DbAccessToken(self.Base):
            __tablename__ = 'access_tokens'
            
            token_id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid.uuid4()))
            title: Mapped[str] = mapped_column(nullable=False)
            username: Mapped[str] = mapped_column(ForeignKey('users.username', ondelete='CASCADE'), nullable=False)
            created_at: Mapped[datetime] = mapped_column(nullable=False)
            expires_at: Mapped[datetime] = mapped_column(nullable=False)

            def __repr__(self):
                return f"<AccessToken(token_id='{self.token_id}', username='{self.username}')>"
        
        self.DbBaseUser = DbBaseUser
        self.DbAccessToken = DbAccessToken
        
        self.User = self._get_user_model(base_path) if cls is None else cls
        self.DbUser: type[DbBaseUser] = self._initialize_db_user_model(self.User)
        
        if sa_engine is None:
            sqlite_relative_path = env_vars.get(c.SQRL_AUTH_DB_FILE_PATH, f"{c.TARGET_FOLDER}/{c.DB_FILE}")
            sqlite_path = u.Path(base_path, sqlite_relative_path)
            sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            self.engine = create_engine(f"sqlite:///{str(sqlite_path)}")
        else:
            self.engine = sa_engine
        
        # Configure SQLite pragmas
        with self.engine.connect() as conn:
            conn.execute(text("PRAGMA journal_mode = WAL"))
            conn.execute(text("PRAGMA synchronous = NORMAL"))
            conn.commit()
        
        self.Base.metadata.create_all(self.engine)

        self.Session = sessionmaker(bind=self.engine)

        self._initialize_db(self.User, self.DbUser, self.engine, self.Session)
    
    def _get_user_model(self, base_path: str) -> type[BaseUser]:
        user_module_path = u.Path(base_path, c.PYCONFIGS_FOLDER, c.USER_FILE)
        user_module = PyModule(user_module_path)
        User = user_module.get_func_or_class("User", default_attr=BaseUser)
        if not issubclass(User, BaseUser):
            raise ConfigurationError(f"User class in '{c.USER_FILE}' must inherit from BaseUser")
        return User

    def _initialize_db_user_model(self, *args) -> type:
        """Get the user model with any custom attributes defined in user.py"""
        attrs = {}

        # Iterate over all fields in the User model
        for field_name, field in self.User.model_fields.items():
            if field_name in reserved_fields:
                continue
            if field_name in disallowed_fields:
                raise ConfigurationError(f"Field name '{field_name}' is disallowed in the User model and cannot be used")
            
            field_type = field.annotation
            if _t.get_origin(field_type) in (_t.Union, types.UnionType):
                field_type = _t.get_args(field_type)[0]
                nullable = True
            else:
                nullable = False
    
            if _t.get_origin(field_type) == _t.Literal:
                field_type = str

            # Map Python types and default values to SQLAlchemy columns
            default_value = field.default
            if default_value is PydanticUndefined:
                raise ConfigurationError(f"No default value found for field '{field_name}' in User model")
            elif not nullable and default_value is None:
                raise ConfigurationError(f"Default value for non-nullable field '{field_name}' was set as None in User model")
            elif default_value is not None and type(default_value) is not field_type:
                raise ConfigurationError(f"Default value for field '{field_name}' does not match field type in User model")
            
            if field_type == str:
                col_type = String
            elif field_type == int:
                col_type = Integer
            elif field_type == float:
                col_type = Float
            elif field_type == bool:
                col_type = Boolean
            elif isinstance(field_type, type) and issubclass(field_type, Enum):
                col_type = String
                default_value = default_value.value
            else:
                continue

            attrs[field_name] = Column(col_type, nullable=nullable, default=default_value) # type: ignore

        # Create the sqlalchemy model class
        DbUser = type('DbUser', (self.DbBaseUser,), attrs)
        return DbUser

    def _initialize_db(self, *args): # TODO: Use logger instead of print
        session = self.Session()
        try:
            # Get existing columns in the database
            inspector = inspect(self.engine)
            existing_columns = {col['name'] for col in inspector.get_columns('users')}

            # Get all columns defined in the model
            dropped_columns = set(self.User.dropped_columns())
            model_columns = set(self.DbUser.__table__.columns.keys()) - dropped_columns

            # Find columns that are in the model but not in the database
            new_columns = model_columns - existing_columns
            if new_columns:
                add_columns_msg = f"Adding columns to database: {new_columns}"
                print("NOTE:", add_columns_msg)
                self.logger.info(add_columns_msg)
                
                for col_name in new_columns:
                    col = self.DbUser.__table__.columns[col_name]
                    column_type = col.type.compile(self.engine.dialect)
                    nullable = "NULL" if col.nullable else "NOT NULL"
                    if col.default is not None:
                        default_val = f"'{col.default.arg}'" if isinstance(col.default.arg, str) else col.default.arg
                        default = f"DEFAULT {default_val}"
                    else:
                        default = ""
                    
                    alter_stmt = f"ALTER TABLE users ADD COLUMN {col_name} {column_type} {nullable} {default}"
                    session.execute(text(alter_stmt))
                
                session.commit()
            
            # Determine columns to drop
            columns_to_drop = dropped_columns.intersection(existing_columns)
            if columns_to_drop:
                drop_columns_msg = f"Dropping columns from database: {columns_to_drop}"
                print("NOTE:", drop_columns_msg)
                self.logger.info(drop_columns_msg)
                
                for col_name in columns_to_drop:
                    session.execute(text(f"ALTER TABLE users DROP COLUMN {col_name}"))
                
                session.commit()

            # Find columns that are in the database but not in the model
            extra_db_columns = existing_columns - columns_to_drop - model_columns
            if extra_db_columns:
                self.logger.warning(f"The following database columns are not in the User model: {extra_db_columns}\n"
                    "If you want to drop these columns, please use the `dropped_columns` class method of the User model.")

            # Get admin password from environment variable if exists
            admin_password = self.env_vars.get(c.SQRL_SECRET_ADMIN_PASSWORD)
            
            # If admin password variable exists, find username "admin". If it does not exist, add it
            if admin_password is not None:
                password_hash = pwd_context.hash(admin_password)
                admin_user = session.get(self.DbUser, c.ADMIN_USERNAME)
                if admin_user is None:
                    admin_user = self.DbUser(username=c.ADMIN_USERNAME, password_hash=password_hash, is_admin=True)
                    session.add(admin_user)
                else:
                    admin_user.password_hash = password_hash
            
            session.commit()

        finally:
            session.close()

    @cached_property
    def user_fields(self) -> list[UserField]:
        """
        Get the fields of the User model as a list of dictionaries
        
        Each dictionary contains the following keys:
        - name: The name of the field
        - type: The type of the field
        - nullable: Whether the field is nullable
        - enum: The possible values of the field (or None if not applicable)
        - default: The default value of the field (or None if field is required)
        """
        schema = self.User.model_json_schema()

        fields = []
        
        properties: dict[str, dict[str, _t.Any]] = schema.get("properties", {})
        for field_name, field_schema in properties.items():
            if choices := field_schema.get("anyOf"):
                field_type = choices[0]["type"]
                nullable = (choices[1]["type"] == "null")
            else:
                field_type = field_schema["type"]
                nullable = False
            
            field_data = UserField(name=field_name, type=field_type, nullable=nullable, enum=field_schema.get("enum"), default=field_schema.get("default"))
            fields.append(field_data)

        return fields
    
    def add_user(self, username: str, user_fields: dict, *, update_user: bool = False) -> None:
        session = self.Session()

        # Validate the user data
        try:
            user_data = self.User(**user_fields, username=username).model_dump(mode='json')
        except ValidationError as e:
            raise InvalidInputError(102, f"Invalid user field '{e.errors()[0]['loc'][0]}': {e.errors()[0]['msg']}")

        # Add a new user
        try:
            # Check if the user already exists
            existing_user = session.get(self.DbUser, username)
            if existing_user is not None:
                if not update_user:
                    raise InvalidInputError(101, f"User '{username}' already exists")
                
                if username == c.ADMIN_USERNAME:
                    raise InvalidInputError(24, "Changing the admin user is not permitted")
                new_user = self.DbUser(password_hash=existing_user.password_hash, **user_data)
                session.delete(existing_user)
            else:
                if update_user:
                    raise InvalidInputError(41, f"No user found for username: {username}")
                
                password = user_fields.get('password')
                if password is None:
                    raise InvalidInputError(100, f"Missing required field 'password' when adding a new user")
                password_hash = pwd_context.hash(password)
                new_user = self.DbUser(password_hash=password_hash, **user_data)
            
            # Add the user to the session
            session.add(new_user)
            
            # Commit the transaction
            session.commit()

        finally:
            session.close()

    def get_user(self, username: str, password: str) -> User:
        session = self.Session()
        try:
            # Query for user by username
            db_user = session.get(self.DbUser, username)
            
            if db_user and pwd_context.verify(password, db_user.password_hash):
                user = self.User.model_validate(db_user)
                return user # type: ignore
            else:
                raise InvalidInputError(0, f"Username or password not found")

        finally:
            session.close()
    
    def change_password(self, username: str, old_password: str, new_password: str) -> None:
        session = self.Session()
        try:
            db_user = session.get(self.DbUser, username)
            if db_user is None:
                raise InvalidInputError(2, f"User not found")
            
            if pwd_context.verify(old_password, db_user.password_hash):
                db_user.password_hash = pwd_context.hash(new_password)
                session.commit()
            else:
                raise InvalidInputError(3, f"Incorrect password")
        finally:
            session.close()

    def delete_user(self, username: str) -> None:
        if username == c.ADMIN_USERNAME:
            raise InvalidInputError(23, "Cannot delete the admin user")
        
        session = self.Session()
        try:
            db_user = session.get(self.DbUser, username)
            if db_user is None:
                raise InvalidInputError(41, f"No user found for username: {username}")
            session.delete(db_user)
            session.commit()
        finally:
            session.close()

    def get_all_users(self) -> list[User]:
        session = self.Session()
        try:
            db_users = session.query(self.DbUser).all()
            return [self.User.model_validate(user) for user in db_users] # type: ignore
        finally:
            session.close()
    
    def create_access_token(self, user: User, expiry_minutes: int | None, *, title: str | None = None) -> tuple[str, datetime]:
        created_at = datetime.now(timezone.utc)
        expire_at = created_at + timedelta(minutes=expiry_minutes) if expiry_minutes is not None else datetime.max
        token_id = None
        if title is not None:
            session = self.Session()
            try:
                access_token = self.DbAccessToken(title=title, username=user.username, created_at=created_at, expires_at=expire_at)
                session.add(access_token)
                session.commit()
                token_id = access_token.token_id
            finally:
                session.close()
        
        if self.secret_key is None:
            raise ConfigurationError(f"Environment variable '{c.SQRL_SECRET_KEY}' is required to create an access token")
        to_encode = {"username": user.username, "token_id": token_id, "exp": expire_at}
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm="HS256")
        return encoded_jwt, expire_at
    
    def get_user_from_token(self, token: str | None) -> User | None:
        if token is None or token == "":
            return None
        
        if self.secret_key is None:
            raise ConfigurationError(f"Environment variable '{c.SQRL_SECRET_KEY}' is required to get user from an access token")

        try:
            payload: dict = jwt.decode(token, self.secret_key, algorithms=["HS256"])
        except InvalidTokenError:
            raise InvalidInputError(1, "Invalid authorization token")
        
        session = self.Session()
        try:
            if payload.get("token_id") is not None:
                access_token = session.query(self.DbAccessToken).filter(
                    self.DbAccessToken.username == payload["username"],
                    self.DbAccessToken.token_id == payload["token_id"],
                    self.DbAccessToken.expires_at >= func.now()
                ).first()
                if access_token is None:
                    raise InvalidInputError(1, "Invalid authorization token")
            
            db_user = session.get(self.DbUser, payload["username"])
            if db_user is None:
                raise InvalidInputError(1, "Invalid authorization token")
        finally:
            session.close()
        
        user = self.User.model_validate(db_user)
        return user # type: ignore
    
    def get_all_tokens(self, username: str) -> list[AccessToken]:
        session = self.Session()
        try:
            tokens = session.query(self.DbAccessToken).filter(
                self.DbAccessToken.username == username,
                self.DbAccessToken.expires_at >= func.now()
            ).all()
            
            return [AccessToken.model_validate(token) for token in tokens]
        finally:
            session.close()
    
    def revoke_token(self, username: str, token_id: str) -> None:
        session = self.Session()
        try:
            access_token = session.query(self.DbAccessToken).filter(
                self.DbAccessToken.username == username,
                self.DbAccessToken.token_id == token_id
            ).first()
            
            if access_token is None:
                raise InvalidInputError(40, f"No access token found for token_id: {token_id}")
            
            session.delete(access_token)
            session.commit()
        finally:
            session.close()

    def can_user_access_scope(self, user: User | None, scope: PermissionScope) -> bool:
        if user is None:
            user_level = PermissionScope.PUBLIC
        elif user.is_admin:
            user_level = PermissionScope.PRIVATE
        else:
            user_level = PermissionScope.PROTECTED
        
        return user_level.value >= scope.value

    def close(self) -> None:
        self.engine.dispose()
