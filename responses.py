from __future__ import annotations
from typing import Any, Optional, Dict
from pydantic import BaseModel


class ErrorInfo(BaseModel):
    type: str
    details: Optional[Any] = None


class Envelope(BaseModel):
    success: bool
    code: int
    message: str
    data: Optional[Any] = None
    error: Optional[ErrorInfo] = None
    request_id: str


def ok(request_id: str, data: Any = None, message: str = "OK", code: int = 200) -> Envelope:
    return Envelope(success=True, code=code, message=message, data=data, error=None, request_id=request_id)


def fail(
    request_id: str,
    code: int,
    message: str,
    error_type: str,
    details: Any = None,
) -> Envelope:
    return Envelope(
        success=False,
        code=code,
        message=message,
        data=None,
        error=ErrorInfo(type=error_type, details=details),
        request_id=request_id,
    )
