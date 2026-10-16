"""Results returned by ZeroETL operations."""

import base64
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import math
from typing import Optional


def _json_safe(value):
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, float) and not math.isfinite(value):
        return str(value).replace("nan", "NaN").replace("inf", "Infinity")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported result value: {type(value).__name__}")


@dataclass
class OperationResult:
    operation: str
    table_name: str
    status: str = "success"
    counts: Optional[dict] = None
    rows: Optional[list] = None
    message: Optional[str] = None

    def to_dict(self) -> dict:
        return _json_safe(vars(self))
