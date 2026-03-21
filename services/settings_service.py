"""Read/write from settings table with in-memory cache."""
import logging

logger = logging.getLogger(__name__)

_cache: dict = {}


async def get_setting(key: str, default: str = "") -> str:
    if key in _cache:
        return _cache[key]
    from database import AsyncSessionLocal
    from models import Setting
    from sqlalchemy import select

    async with AsyncSessionLocal() as sess:
        row = await sess.scalar(select(Setting).where(Setting.key == key))
        value = row.value if row else default
        _cache[key] = value
        return value


async def set_setting(key: str, value: str):
    _cache[key] = value
    from database import AsyncSessionLocal
    from models import Setting
    from sqlalchemy import select

    async with AsyncSessionLocal() as sess:
        async with sess.begin():
            row = await sess.scalar(select(Setting).where(Setting.key == key))
            if row:
                row.value = value
            else:
                sess.add(Setting(key=key, value=value))


def invalidate_cache():
    _cache.clear()
