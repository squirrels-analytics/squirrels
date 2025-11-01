"""
Request context management using ContextVars for request-scoped data.
Provides thread-safe and async-safe access to request IDs throughout the request lifecycle.
"""
from contextvars import ContextVar
import uuid
import base64

# ContextVar for storing the current request ID
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)


def get_request_id() -> str | None:
    """
    Get the current request ID from the context.
    
    Returns:
        The request ID string if available, None otherwise (e.g., in background tasks).
    """
    return _request_id.get()


def set_request_id() -> str:
    """
    Set a new request ID in the context.
    Uses base64 URL-safe encoding of UUID bytes to create a shorter ID (22 chars vs 36).
    
    Returns:
        The request ID that was set.
    """
    request_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')
    _request_id.set(request_id)
    return request_id
