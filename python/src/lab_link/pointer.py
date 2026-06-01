from __future__ import annotations


def escape_pointer_part(part: object) -> str:
    return str(part).replace("~", "~0").replace("/", "~1")


def ptr(*parts: object) -> str:
    if not parts:
        return ""
    return "/" + "/".join(escape_pointer_part(part) for part in parts)
