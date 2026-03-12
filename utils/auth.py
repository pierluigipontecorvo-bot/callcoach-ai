"""
JWT-based admin authentication.

Flow:
  POST /admin/login  {password: "..."}  ->  {access_token: "...", token_type: "bearer"}
  All /admin/* endpoints require:  Authorization: Bearer <token>
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from config import settings

_ALGORITHM = "HS256"
_TOKEN_EXPIRE_HOURS = 24

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
_bearer_scheme = HTTPBearer(auto_error=False)


# ── Token creation ─────────────────────────────────────────────────────────────

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(tz=timezone.utc) + (
        expires_delta or timedelta(hours=_TOKEN_EXPIRE_HOURS)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.secret_key, algorithm=_ALGORITHM)


# ── Password check ────────────────────────────────────────────────────────────

def verify_admin_password(plain: str) -> bool:
    return plain == settings.admin_password


# ── Dependency: require valid JWT ─────────────────────────────────────────────

async def require_admin(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> dict:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if credentials is None:
        raise credentials_exception
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.secret_key,
            algorithms=[_ALGORITHM],
        )
        role: str = payload.get("role", "")
        if role != "admin":
            raise credentials_exception
        return payload
    except JWTError:
        raise credentials_exception
