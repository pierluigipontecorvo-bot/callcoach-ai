"""
Miscellaneous utility functions.
"""

from datetime import datetime, timezone
from typing import Optional


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def parse_iso_datetime(value: str) -> Optional[datetime]:
    """
    Parse an ISO-8601 datetime string to a timezone-aware datetime.
    Returns None if parsing fails.
    """
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    return None
