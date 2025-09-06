from typing import Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import cached_property
from jwt.exceptions import InvalidTokenError
from passlib.context import CryptContext
from pydantic import ValidationError
from pydantic_core import PydanticUndefined
from sqlalchemy import create_engine, Engine, func, inspect, text, ForeignKey
from sqlalchemy import Column, String, Integer, Float, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker, Mapped, mapped_column
import jwt, types, typing as _t, uuid, secrets, json

from ._manifest import PermissionScope
from ._py_module import PyModule
from ._exceptions import InvalidInputError, ConfigurationError
from ._arguments.init_time_args import AuthProviderArgs
from ._schemas.auth_models import (
    BaseUser, ApiKey, UserField, AuthProvider, ProviderConfigs, ClientRegistrationRequest, ClientUpdateRequest,
    ClientDetailsResponse, ClientRegistrationResponse, ClientUpdateResponse, TokenResponse
)
from . import _utils as u, _constants as c

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

reserved_fields = ["username", "is_admin"]
disallowed_fields = ["password", "password_hash", "created_at", "token_id", "exp"]

User = _t.TypeVar('User', bound=BaseUser)

ProviderFunctionType = Callable[[AuthProviderArgs], AuthProvider]


class Authenticator(_t.Generic[User]):
    providers: list[ProviderFunctionType] = []  # static variable to stage providers

    def __init__(
        self, logger: u.Logger, base_path: str, auth_args: AuthProviderArgs, provider_functions: list[ProviderFunctionType], 
        user_cls: type[User], *, sa_engine: Engine | None = None, external_only: bool = False
    ):
        self.logger = logger
        self.env_vars = auth_args.env_vars
        self.secret_key = self.env_vars.get(c.SQRL_SECRET_KEY)
        self.external_only = external_only

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
        
        # Define DbApiKey class for this instance
        class DbApiKey(self.Base):
            __tablename__ = 'api_keys'
            
            id: Mapped[str] = mapped_column(primary_key=True, default=lambda: uuid.uuid4().hex)
            hashed_key: Mapped[str] = mapped_column(unique=True, nullable=False)
            title: Mapped[str] = mapped_column(nullable=False)
            username: Mapped[str] = mapped_column(ForeignKey('users.username', ondelete='CASCADE'), nullable=False)
            created_at: Mapped[datetime] = mapped_column(nullable=False)
            expires_at: Mapped[datetime] = mapped_column(nullable=False)
        
            def __repr__(self):
                return f"<DbApiKey(id='{self.id}', username='{self.username}')>"
        
        # Define DbOAuthClient class for this instance
        class DbOAuthClient(self.Base):
            __tablename__ = 'oauth_clients'
            
            client_id: Mapped[str] = mapped_column(primary_key=True, default=lambda: uuid.uuid4().hex)
            client_secret_hash: Mapped[str] = mapped_column(nullable=False)
            client_name: Mapped[str] = mapped_column(nullable=False)
            redirect_uris: Mapped[str] = mapped_column(nullable=False)  # JSON array of allowed redirect URIs
            scope: Mapped[str] = mapped_column(nullable=False, default='read')
            grant_types: Mapped[str] = mapped_column(nullable=False, default='authorization_code,refresh_token')
            response_types: Mapped[str] = mapped_column(nullable=False, default='code')
            client_type: Mapped[str] = mapped_column(nullable=False, default='confidential')  # 'confidential' or 'public'
            registration_access_token_hash: Mapped[str] = mapped_column(nullable=False)  # Token for client management
            created_at: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
            is_active: Mapped[bool] = mapped_column(nullable=False, default=True)
            
            def __repr__(self):
                return f"<DbOAuthClient(client_id='{self.client_id}', name='{self.client_name}')>"
        
        # Define DbAuthorizationCode class for this instance
        class DbAuthorizationCode(self.Base):
            __tablename__ = 'authorization_codes'
            
            code: Mapped[str] = mapped_column(primary_key=True, default=lambda: uuid.uuid4().hex)
            client_id: Mapped[str] = mapped_column(ForeignKey('oauth_clients.client_id', ondelete='CASCADE'), nullable=False)
            username: Mapped[str] = mapped_column(ForeignKey('users.username', ondelete='CASCADE'), nullable=False)
            redirect_uri: Mapped[str] = mapped_column(nullable=False)
            scope: Mapped[str] = mapped_column(nullable=True)
            code_challenge: Mapped[str] = mapped_column(nullable=False)  # PKCE always required
            code_challenge_method: Mapped[str] = mapped_column(nullable=False)  # only S256 is supported
            created_at: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
            expires_at: Mapped[datetime] = mapped_column(nullable=False)  # 10 minutes from creation
            used: Mapped[bool] = mapped_column(nullable=False, default=False)
            
            def __repr__(self):
                return f"<DbAuthorizationCode(code='{self.code[:8]}...', client_id='{self.client_id}')>"
        
        # Define DbOAuthToken class for this instance  
        class DbOAuthToken(self.Base):
            __tablename__ = 'oauth_tokens'
            
            token_id: Mapped[str] = mapped_column(primary_key=True, default=lambda: uuid.uuid4().hex)
            access_token_hash: Mapped[str] = mapped_column(unique=True, nullable=False)
            refresh_token_hash: Mapped[str] = mapped_column(unique=True, nullable=True)  # NULL for client_credentials grants
            client_id: Mapped[str] = mapped_column(ForeignKey('oauth_clients.client_id', ondelete='CASCADE'), nullable=False)
            username: Mapped[str] = mapped_column(ForeignKey('users.username', ondelete='CASCADE'), nullable=False)
            scope: Mapped[str] = mapped_column(nullable=True)
            token_type: Mapped[str] = mapped_column(nullable=False, default='Bearer')
            created_at: Mapped[datetime] = mapped_column(nullable=False, server_default=func.now())
            access_token_expires_at: Mapped[datetime] = mapped_column(nullable=False)  # Uses SQRL_AUTH_TOKEN_EXPIRE_MINUTES
            refresh_token_expires_at: Mapped[datetime] = mapped_column(nullable=True)  # 30 days from creation, NULL for client_credentials
            is_revoked: Mapped[bool] = mapped_column(nullable=False, default=False)
            
            def __repr__(self):
                return f"<DbOAuthToken(token_id='{self.token_id}', client_id='{self.client_id}', username='{self.username}')>"
        
        self.DbBaseUser = DbBaseUser
        self.DbApiKey = DbApiKey
        self.DbOAuthClient = DbOAuthClient
        self.DbAuthorizationCode = DbAuthorizationCode
        self.DbOAuthToken = DbOAuthToken
        
        self.User = user_cls
        self.DbUser: type[DbBaseUser] = self._initialize_db_user_model(self.User)

        self.auth_providers = [provider_function(auth_args) for provider_function in provider_functions]
        
        if sa_engine is None:
            sqlite_relative_path = self.env_vars.get(c.SQRL_AUTH_DB_FILE_PATH, f"{c.TARGET_FOLDER}/{c.DB_FILE}")
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
                print("NOTE -", add_columns_msg)
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
                print("NOTE -", drop_columns_msg)
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
            raise InvalidInputError(400, "Invalid user data", f"Invalid user field '{e.errors()[0]['loc'][0]}': {e.errors()[0]['msg']}")

        # Add a new user
        try:
            # Check if the user already exists
            existing_user = session.get(self.DbUser, username)
            if existing_user is not None:
                if not update_user:
                    raise InvalidInputError(400, "Username already exists", f"User '{username}' already exists")
                
                if username == c.ADMIN_USERNAME and user_data.get("is_admin") is False:
                    raise InvalidInputError(403, "Non-admin 'admin' user not permitted", "Setting the admin user to non-admin is not permitted")
                
                new_user = self.DbUser(password_hash=existing_user.password_hash, **user_data)
                session.delete(existing_user)
            else:
                if update_user:
                    raise InvalidInputError(404, "No user found for username", f"No user found for username: {username}")
                
                password = user_fields.get('password')
                if password is None:
                    raise InvalidInputError(400, "Missing required field 'password'", f"Missing required field 'password' when adding a new user")
                password_hash = pwd_context.hash(password)
                new_user = self.DbUser(password_hash=password_hash, **user_data)
            
            # Add the user to the session
            session.add(new_user)
            
            # Commit the transaction
            session.commit()

        finally:
            session.close()
    
    def create_or_get_user_from_provider(self, provider_name: str, user_info: dict) -> User:
        provider = next((p for p in self.auth_providers if p.name == provider_name), None)
        if provider is None:
            raise InvalidInputError(404, "Provider not found", f"Provider '{provider_name}' not found")
        
        user = provider.provider_configs.get_user(user_info)
        session = self.Session()
        try:
            db_user = session.get(self.DbUser, user.username)
            if db_user is None:
                # Create new user
                user_data = user.model_dump()
                password_hash = ""  # No hash makes it impossible to login with username and password
                db_user = self.DbUser(password_hash=password_hash, **user_data)
                session.add(db_user)
                session.commit()
        
            return self.User.model_validate(db_user)
        
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
                raise InvalidInputError(401, "Incorrect username or password", f"Incorrect username or password")

        finally:
            session.close()
    
    def change_password(self, username: str, old_password: str, new_password: str) -> None:
        session = self.Session()
        try:
            db_user = session.get(self.DbUser, username)
            if db_user is None:
                raise InvalidInputError(401, "User not found", f"Username '{username}' not found for password change")
            
            if db_user.password_hash and pwd_context.verify(old_password, db_user.password_hash):
                db_user.password_hash = pwd_context.hash(new_password)
                session.commit()
            else:
                raise InvalidInputError(401, "Incorrect password", f"Incorrect password")
        finally:
            session.close()

    def delete_user(self, username: str) -> None:
        if username == c.ADMIN_USERNAME:
            raise InvalidInputError(403, "Cannot delete admin user", "Cannot delete the admin user")
        
        session = self.Session()
        try:
            db_user = session.get(self.DbUser, username)
            if db_user is None:
                raise InvalidInputError(404, "No user found for username", f"No user found for username: {username}")
            session.delete(db_user)
            session.commit()
        finally:
            session.close()

    def get_all_users(self) -> list:
        session = self.Session()
        try:
            db_users = session.query(self.DbUser).all()
            return [self.User.model_validate(user) for user in db_users]
        finally:
            session.close()
    
    def create_access_token(self, user: User, expiry_minutes: int | None, *, title: str | None = None) -> tuple[str, datetime]:
        """
        Creates an API key if title is provided. Otherwise, creates a JWT token.
        """
        created_at = datetime.now(timezone.utc)
        expire_at = created_at + timedelta(minutes=expiry_minutes) if expiry_minutes is not None else datetime.max
        
        if self.secret_key is None:
            raise ConfigurationError(f"Environment variable '{c.SQRL_SECRET_KEY}' is required to create an access token")
        
        if title is not None:
            session = self.Session()
            try:
                token_id = "sqrl-" + uuid.uuid4().hex
                hashed_key = u.hash_string(token_id, salt=self.secret_key)
                api_key = self.DbApiKey(hashed_key=hashed_key, title=title, username=user.username, created_at=created_at, expires_at=expire_at)
                session.add(api_key)
                session.commit()
            finally:
                session.close()
        else:
            to_encode = {"username": user.username, "exp": expire_at}
            token_id = jwt.encode(to_encode, self.secret_key, algorithm="HS256")
        
        return token_id, expire_at
    
    def get_user_from_token(self, token: str | None) -> User | None:
        """
        Get a user from an access token (JWT, or API key if token starts with 'sqrl-')
        """
        if not token:
            return None
        
        if self.secret_key is None:
            raise ConfigurationError(f"Environment variable '{c.SQRL_SECRET_KEY}' is required to get user from an access token")

        session = self.Session()
        try:
            if token.startswith("sqrl-"):
                hashed_key = u.hash_string(token, salt=self.secret_key)
                api_key = session.query(self.DbApiKey).filter(
                    self.DbApiKey.hashed_key == hashed_key,
                    self.DbApiKey.expires_at >= func.now()
                ).first()
                if api_key is None:
                    raise InvalidTokenError()
                username = api_key.username
            else:
                payload: dict = jwt.decode(token, self.secret_key, algorithms=["HS256"])
                username = payload["username"]
                
            db_user = session.get(self.DbUser, username)
            if db_user is None:
                raise InvalidTokenError()
        
        except InvalidTokenError:
            raise InvalidInputError(401, "Invalid authorization token", "Invalid authorization token")
        finally:
            session.close()
        
        user = self.User.model_validate(db_user)
        return user # type: ignore
    
    def get_all_api_keys(self, username: str) -> list[ApiKey]:
        """
        Get the ID, title, and expiry date of all API keys for a user. Note that the ID is a hash of the API key, not the API key itself.
        """
        session = self.Session()
        try:
            tokens = session.query(self.DbApiKey).filter(
                self.DbApiKey.username == username,
                self.DbApiKey.expires_at >= func.now()
            ).all()
            
            return [ApiKey.model_validate(token) for token in tokens]
        finally:
            session.close()
    
    def revoke_api_key(self, username: str, api_key_id: str) -> None:
        """
        Revoke an API key
        """
        session = self.Session()
        try:

            api_key = session.query(self.DbApiKey).filter(
                self.DbApiKey.username == username,
                self.DbApiKey.id == api_key_id
            ).first()
            
            if api_key is None:
                raise InvalidInputError(404, "API key not found", f"The API key could not be found: {api_key_id}")
            
            session.delete(api_key)
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

    # OAuth Client Management Methods

    def generate_secret_and_hash(self) -> tuple[str, str]:
        """Generate a secure access token and its hash"""
        secret = secrets.token_urlsafe(64)
        secret_hash = pwd_context.hash(secret)
        return secret, secret_hash
    
    def _validate_client_registration_request(self, request: ClientRegistrationRequest | ClientUpdateRequest) -> dict:
        updates = {}
        if request.client_name:
            updates['client_name'] = request.client_name

        # Validate redirect_uris if being updated
        if request.redirect_uris:
            for uri in request.redirect_uris:
                if not self._validate_redirect_uri_format(uri):
                    raise InvalidInputError(400, "invalid_redirect_uri", f"Invalid redirect URI format: {uri}")
            updates['redirect_uris'] = json.dumps(request.redirect_uris)
        
        # Validate grant_types if being updated
        if request.grant_types:
            if not all(grant_type in c.SUPPORTED_GRANT_TYPES for grant_type in request.grant_types):
                raise InvalidInputError(400, "invalid_grant_types", f"Invalid grant types. Supported grant types are: {c.SUPPORTED_GRANT_TYPES}")
            updates['grant_types'] = ','.join(request.grant_types)
        
        # Validate response_types if being updated
        if request.response_types:
            if not all(response_type in c.SUPPORTED_RESPONSE_TYPES for response_type in request.response_types):
                raise InvalidInputError(400, "invalid_response_types", f"Invalid response types. Supported response types are: {c.SUPPORTED_RESPONSE_TYPES}")
            updates['response_types'] = ','.join(request.response_types)
        
        # Validate scope if being updated
        if request.scope:
            scopes = request.scope.split()
            if not all(scope in c.SUPPORTED_SCOPES for scope in scopes):
                raise InvalidInputError(400, "invalid_scope", f"Invalid scope. Supported scopes are: {c.SUPPORTED_SCOPES}")
            updates['scope'] = ','.join(scopes)
        
        return updates
    
    def register_oauth_client(
        self, request: ClientRegistrationRequest, client_management_path_format: str
    ) -> ClientRegistrationResponse:
        """Register a new OAuth client and return client_id, client_secret, and registration_access_token"""
        grant_types = request.grant_types
        if grant_types is None:
            grant_types = ['authorization_code', 'refresh_token']
        
        # Validate request
        self._validate_client_registration_request(request)
        
        # Generate secure client credentials and registration access token
        client_id = secrets.token_urlsafe(16)
        client_secret, client_secret_hash = self.generate_secret_and_hash()

        registration_access_token, registration_access_token_hash = self.generate_secret_and_hash()
        registration_client_uri = client_management_path_format.format(client_id=client_id)
        
        session = self.Session()
        try:
            oauth_client = self.DbOAuthClient(
                client_id=client_id,
                client_secret_hash=client_secret_hash,
                client_name=request.client_name,
                redirect_uris=json.dumps(request.redirect_uris),
                scope=request.scope,
                grant_types=','.join(grant_types),
                registration_access_token_hash=registration_access_token_hash
            )
            session.add(oauth_client)
            session.commit()
            
            return ClientRegistrationResponse(
                client_id=client_id,
                client_secret=client_secret,
                client_name=request.client_name,
                redirect_uris=request.redirect_uris,
                scope=request.scope,
                grant_types=grant_types,
                response_types=request.response_types,
                created_at=datetime.now(timezone.utc),
                is_active=True,
                registration_client_uri=registration_client_uri,
                registration_access_token=registration_access_token,
            )
            
        finally:
            session.close()
    
    def get_oauth_client_details(self, client_id: str) -> ClientDetailsResponse:
        """Get OAuth client details with parsed JSON fields"""
        session = self.Session()
        try:
            client = session.get(self.DbOAuthClient, client_id)
            if client is None:
                raise InvalidInputError(404, "invalid_client_id", "Client not found")
            if not client.is_active:
                raise InvalidInputError(404, "invalid_client_id", "Client is no longer active")
            
            return ClientDetailsResponse(
                client_id=client.client_id,
                client_name=client.client_name,
                redirect_uris=json.loads(client.redirect_uris),
                scope=client.scope,
                grant_types=client.grant_types.split(','),
                response_types=client.response_types.split(','),
                created_at=client.created_at,
                is_active=client.is_active
            )
        finally:
            session.close()

    def validate_client_credentials(self, client_id: str, client_secret: str) -> bool:
        """Validate OAuth client credentials"""
        session = self.Session()
        try:
            client = session.get(self.DbOAuthClient, client_id)
            if client is None or not client.is_active:
                return False
            return pwd_context.verify(client_secret, client.client_secret_hash)
        finally:
            session.close()
    
    def validate_redirect_uri(self, client_id: str, redirect_uri: str) -> bool:
        """Validate that redirect_uri is registered for the client"""
        session = self.Session()
        try:
            client = session.get(self.DbOAuthClient, client_id)
            if client is None or not client.is_active:
                return False
            
            registered_uris = json.loads(client.redirect_uris)
            return redirect_uri in registered_uris
        finally:
            session.close()
    
    def validate_registration_access_token(self, client_id: str, registration_access_token: str) -> bool:
        """Validate registration access token for client management operations"""
        session = self.Session()
        try:
            client = session.get(self.DbOAuthClient, client_id)
            if client is None:
                return False
            
            return pwd_context.verify(registration_access_token, client.registration_access_token_hash)
        finally:
            session.close()
    
    def _validate_redirect_uri_format(self, uri: str) -> bool:
        """Validate redirect URI format for security"""
        # Basic validation - must be https (except localhost) and not contain fragments
        if '#' in uri:
            return False
        
        if uri.startswith('http://'):
            # Only allow http for localhost/development
            if not ('localhost' in uri or '127.0.0.1' in uri):
                return False
        elif not uri.startswith('https://'):
            # Custom schemes allowed for mobile apps
            if '://' not in uri:
                return False
        
        return True
    
    def update_oauth_client_with_token_rotation(self, client_id: str, request: ClientUpdateRequest) -> ClientUpdateResponse:
        """Update OAuth client metadata and rotate registration access token"""
            
        # Build update dictionary, excluding None values
        updates = self._validate_client_registration_request(request)

        session = self.Session()
        try:
            client = session.get(self.DbOAuthClient, client_id)
            if client is None:
                raise InvalidInputError(404, "invalid_client_id", f"OAuth client not found: {client_id}")
            if not client.is_active and not request.is_active:
                raise InvalidInputError(400, "invalid_request", "Cannot update a deactivated client")
            
            if request.is_active is not None:
                client.is_active = request.is_active
            
            # Generate new registration access token
            new_registration_access_token, new_registration_access_token_hash = self.generate_secret_and_hash()
            
            # Update client fields
            for key, value in updates.items():
                if hasattr(client, key):
                    setattr(client, key, value)
            
            # Update registration access token
            client.registration_access_token_hash = new_registration_access_token_hash
            
            session.commit()

            return ClientUpdateResponse(
                client_id=client.client_id,
                client_name=client.client_name,
                redirect_uris=json.loads(client.redirect_uris),
                scope=client.scope,
                grant_types=client.grant_types.split(','),
                response_types=client.response_types.split(','),
                created_at=client.created_at,
                is_active=client.is_active,
                registration_access_token=new_registration_access_token,
            )
            
        finally:
            session.close()
    
    def revoke_oauth_client(self, client_id: str) -> None:
        """Revoke (deactivate) an OAuth client"""
        session = self.Session()
        try:
            client = session.get(self.DbOAuthClient, client_id)
            if client is None:
                raise InvalidInputError(404, "client_not_found", f"OAuth client not found: {client_id}")
            
            client.is_active = False
            session.commit()
        finally:
            session.close()

    # Authorization Code Flow Methods

    def create_authorization_code(
        self, client_id: str, username: str, redirect_uri: str, scope: str, 
        code_challenge: str, code_challenge_method: str = "S256"
    ) -> str:
        """Create and store an authorization code for the authorization code flow"""

        # Validate client_id
        client_details = self.get_oauth_client_details(client_id)
        if client_details is None or not client_details.is_active:
            raise InvalidInputError(400, "invalid_client", "Invalid or inactive client")
        
        # Validate redirect_uri
        if not self.validate_redirect_uri(client_id, redirect_uri):
            raise InvalidInputError(400, "invalid_redirect_uri", "Invalid redirect_uri for this client")
        
        # Validate PKCE parameters
        if not code_challenge:
            raise InvalidInputError(400, "invalid_request", "code_challenge is required")
        
        if code_challenge_method not in ["S256"]:
            raise InvalidInputError(400, "invalid_request", "code_challenge_method must be 'S256'")
        
        # Generate authorization code
        code = secrets.token_urlsafe(48)
        
        # Set expiration (10 minutes from now)
        created_at = datetime.now(timezone.utc)
        expires_at = created_at + timedelta(minutes=10)
        
        session = self.Session()
        try:
            auth_code = self.DbAuthorizationCode(
                code=code,
                client_id=client_id,
                username=username,
                redirect_uri=redirect_uri,
                scope=scope,
                code_challenge=code_challenge,
                code_challenge_method=code_challenge_method,
                created_at=created_at,
                expires_at=expires_at,
                used=False
            )
            session.add(auth_code)
            session.commit()
            
            return code
            
        finally:
            session.close()
    
    def exchange_authorization_code(
        self, code: str, client_id: str, redirect_uri: str, code_verifier: str, access_token_expiry_minutes: int
    ) -> TokenResponse:
        """
        Exchange authorization code for access and refresh tokens
        Returns (access_token, refresh_token)
        """
        session = self.Session()
        try:
            # Get and validate authorization code
            auth_code = session.query(self.DbAuthorizationCode).filter(
                self.DbAuthorizationCode.code == code,
                self.DbAuthorizationCode.client_id == client_id,
                self.DbAuthorizationCode.expires_at >= func.now(),
                self.DbAuthorizationCode.used == False
            ).first()
            
            if auth_code is None:
                raise InvalidInputError(400, "invalid_grant", "Invalid authorization code")
            
            # Validate redirect URI
            if auth_code.redirect_uri != redirect_uri:
                raise InvalidInputError(400, "invalid_grant", "Redirect URI mismatch")
            
            if not u.validate_pkce_challenge(code_verifier, auth_code.code_challenge):
                raise InvalidInputError(400, "invalid_grant", "Invalid code_verifier")
            
            # Get user
            user = session.get(self.DbUser, auth_code.username)
            if user is None:
                raise InvalidInputError(400, "invalid_grant", "User not found")
            
            # Mark authorization code as used
            auth_code.used = True
            
            # Generate tokens
            user_obj = self.User.model_validate(user)
            access_token, token_expires_at = self.create_access_token(user_obj, expiry_minutes=access_token_expiry_minutes)
            access_token_hash = pwd_context.hash(access_token)
            
            # Generate refresh token
            refresh_token, refresh_token_hash = self.generate_secret_and_hash()
            refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=30)
            
            oauth_token = self.DbOAuthToken(
                access_token_hash=access_token_hash,
                refresh_token_hash=refresh_token_hash,
                client_id=client_id,
                username=auth_code.username,
                scope=auth_code.scope,
                access_token_expires_at=token_expires_at,
                refresh_token_expires_at=refresh_expires_at
            )
            session.add(oauth_token)
            
            session.commit()

            return TokenResponse(
                access_token=access_token,
                expires_in=access_token_expiry_minutes*60,
                refresh_token=refresh_token
            )
            
        finally:
            session.close()
        
    def refresh_oauth_access_token(self, refresh_token: str, client_id: str, access_token_expiry_minutes: int) -> TokenResponse:
        """
        Refresh OAuth access token using refresh token
        Returns (access_token, new_refresh_token)
        """
        session = self.Session()
        try:
            # Validate client
            client = session.get(self.DbOAuthClient, client_id)
            if client is None or not client.is_active:
                raise InvalidInputError(400, "invalid_client", "Invalid or inactive client")
            
            # Find active refresh token for this client
            oauth_token = session.query(self.DbOAuthToken).filter(
                self.DbOAuthToken.client_id == client_id,
                self.DbOAuthToken.refresh_token_expires_at >= func.now(),
                self.DbOAuthToken.is_revoked == False
            ).first()
            
            # Find the token that matches our refresh token
            if oauth_token is None or not pwd_context.verify(refresh_token, oauth_token.refresh_token_hash):
                raise InvalidInputError(400, "invalid_grant", "Invalid or expired refresh token")
            
            # Get user
            user = session.get(self.DbUser, oauth_token.username)
            if user is None:
                raise InvalidInputError(400, "invalid_client", "User not found")
            
            # Check secret key is available
            if self.secret_key is None:
                raise ConfigurationError(f"Environment variable '{c.SQRL_SECRET_KEY}' is required for OAuth token operations")
            
            # Generate new tokens
            user_obj = self.User.model_validate(user)
            access_token, token_expires_at = self.create_access_token(user_obj, expiry_minutes=access_token_expiry_minutes)
            access_token_hash = pwd_context.hash(access_token)
            
            # Generate new refresh token
            new_refresh_token, new_refresh_token_hash = self.generate_secret_and_hash()
            refresh_expires_at = datetime.now(timezone.utc) + timedelta(days=30)
            
            # Revoke old token
            oauth_token.is_revoked = True
            
            # Create new token entry
            new_oauth_token = self.DbOAuthToken(
                access_token_hash=access_token_hash,
                refresh_token_hash=new_refresh_token_hash,
                client_id=client_id,
                username=oauth_token.username,
                scope=oauth_token.scope,
                access_token_expires_at=token_expires_at,
                refresh_token_expires_at=refresh_expires_at
            )
            session.add(new_oauth_token)
            
            session.commit()
            
            return TokenResponse(
                access_token=access_token,
                expires_in=access_token_expiry_minutes*60,
                refresh_token=new_refresh_token
            )
            
        finally:
            session.close()
    
    def revoke_oauth_token(self, client_id: str, token: str, token_type_hint: str | None = None) -> None:
        """
        Revoke an OAuth refresh token
        token_type_hint is optional or must be 'refresh_token'. Revoking access token is not supported yet.
        """
        if token_type_hint and token_type_hint != 'refresh_token':
            raise InvalidInputError(400, "invalid_request", "Only refresh tokens can be revoked")
        
        session = self.Session()
        try:
            # Validate client
            client = session.get(self.DbOAuthClient, client_id)
            if client is None or not client.is_active:
                raise InvalidInputError(400, "invalid_client", "Invalid or inactive client")
            
            # Get all potentially matching tokens
            oauth_tokens = session.query(self.DbOAuthToken).filter(
                self.DbOAuthToken.client_id == client_id,
                self.DbOAuthToken.refresh_token_expires_at >= func.now(),
                self.DbOAuthToken.is_revoked == False
            ).all()
            
            # Find the token that matches
            oauth_token = None
            for token_obj in oauth_tokens:
                if pwd_context.verify(token, token_obj.refresh_token_hash):
                    oauth_token = token_obj
                    break
            
            # Revoke token if found (per OAuth spec, always return success)
            if oauth_token:
                oauth_token.is_revoked = True
                session.commit()
                
        finally:
            session.close()
    
    def close(self) -> None:
        self.engine.dispose()


def provider(name: str, label: str, icon: str):
    """
    Decorator to register an authentication provider

    Arguments:
        name: The name of the provider (must be unique, e.g. 'google')
        label: The label of the provider (e.g. 'Google')
        icon: The URL of the icon of the provider (e.g. 'https://www.google.com/favicon.ico')
    """
    def decorator(func: Callable[[AuthProviderArgs], ProviderConfigs]):
        def wrapper(sqrl: AuthProviderArgs):
            provider_configs = func(sqrl)
            return AuthProvider(name=name, label=label, icon=icon, provider_configs=provider_configs)
        Authenticator.providers.append(wrapper)
        return wrapper
    return decorator
