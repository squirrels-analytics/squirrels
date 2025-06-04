class InvalidInputError(Exception):
    """
    Use this exception when the error is due to providing invalid inputs to the REST API

    Attributes:
        status_code: The HTTP status code to return
        error: A short error message that should never change in the future
        error_description: A detailed error message (that is allowed to change in the future)
    """
    def __init__(self, status_code: int, error: str, error_description: str, *args) -> None:
        self.status_code = status_code
        self.error = error
        self.error_description = error_description
        super().__init__(error_description, *args)


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
