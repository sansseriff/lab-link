from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


Severity = Literal["info", "warning", "error"]
DisplayHint = Literal["toast", "banner", "inline"]


@dataclass(slots=True)
class CommandError(Exception):
    code: str
    message: str
    detail: str | None = None
    severity: Severity = "error"
    display: DisplayHint = "toast"
    path: str | None = None
    recoverable: bool = True

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_message(
        self,
        *,
        command: str,
        request_id: str | None,
        version: int,
        origin_client_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "command_error",
            "command": command,
            "requestId": request_id,
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "display": self.display,
            "recoverable": self.recoverable,
            "version": version,
        }
        if self.detail is not None:
            payload["detail"] = self.detail
        if self.path is not None:
            payload["path"] = self.path
        if origin_client_id is not None:
            payload["originClientId"] = origin_client_id
        return payload
