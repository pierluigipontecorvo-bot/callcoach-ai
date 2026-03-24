from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from config import settings

# Convert postgresql:// to postgresql+asyncpg:// for async driver
_db_url = settings.database_url
if _db_url.startswith("postgresql://"):
    _db_url = _db_url.replace("postgresql://", "postgresql+asyncpg://", 1)
elif _db_url.startswith("postgres://"):
    _db_url = _db_url.replace("postgres://", "postgresql+asyncpg://", 1)

engine = create_async_engine(
    _db_url,
    echo=False,
    pool_pre_ping=False,   # disabilitato: causa problemi con PgBouncer/Supavisor
    pool_size=3,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=300,      # ricicla connessioni ogni 5 min (evita stale connections)
    connect_args={
        "command_timeout": 15,           # ogni query: max 15 secondi
        "statement_cache_size": 0,       # disabilita prepared statements per PgBouncer
    },
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    """FastAPI dependency that yields an async DB session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
