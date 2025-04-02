class InvalidInputError(Exception):
    """
    Use this exception when the error is due to providing invalid inputs to the REST API

    Specific error code ranges are reserved for specific categories of errors.
    0-19: 401 unauthorized errors
    20-39: 403 forbidden errors
    40-59: 404 not found errors
    60-69: 409 conflict errors
    70-99: Reserved for future use
    100-199: 400 bad request errors related to authentication
    200-299: 400 bad request errors related to data analytics

    Error code definitions:
    0 - Incorrect username or password
    1 - Invalid authorization token
    2 - Username not found for password change
    3 - Incorrect password for password change
    20 - Authorized user is forbidden to add or update users
    21 - Authorized user is forbidden to delete users
    22 - Cannot delete your own user
    23 - Cannot delete the admin user
    24 - Cannot change the admin user
    25 - User does not have permission to access the dataset / dashboard
    26 - User does not have permission to build the virtual data environment
    27 - User does not have permission to query data models
    40 - No token found for token_id
    41 - No user found for username
    60 - An existing build process is already running and a concurrent build is not allowed
    61 - Model depends on static data models that cannot be found
    100 - Missing required field 'username' or 'password' when adding a new user
    101 - Username already exists when adding a new user
    102 - Invalid user data when adding a new user
    200 - Invalid value for dataset parameter
    201 - Invalid query parameter provided
    202 - Could not determine parent parameter for parameter refresh
    203 - SQL query must be provided
    204 - Failed to run provided SQL query
    """
    def __init__(self, error_code: int, message: str, *args) -> None:
        self.error_code = error_code
        super().__init__(message, *args)


class ConfigurationError(Exception):
    """
    Use this exception when the server error is due to errors in the squirrels project instead of the squirrels framework/library
    """
    pass


class FileExecutionError(Exception):
    def __init__(self, message: str, error: Exception, *args) -> None:
        t = "  "
        new_message = f"\n" + message + f"\n{t}Produced error message:\n{t}{t}{error} (see above for more details on handled exception)"
        super().__init__(new_message, *args)
        self.error = error
