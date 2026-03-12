"""
Admin API router.

Endpoints:
  POST /admin/login                  — get JWT token
  GET  /admin/campaigns              — list campaigns
  POST /admin/campaigns              — create campaign
  GET  /admin/campaigns/{code}       — get campaign by code
  PUT  /admin/campaigns/{code}       — update campaign
  GET  /admin/analyses               — list analyses (with filters)
  GET  /admin/analyses/{id}          — get analysis detail
  GET  /admin/health                 — health + DB check
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import Analysis, Campaign
from schemas import (
    AnalysisDetailOut,
    AnalysisOut,
    CampaignCreate,
    CampaignOut,
    CampaignUpdate,
    LoginRequest,
    TokenResponse,
)
from utils.auth import create_access_token, require_admin, verify_admin_password

router = APIRouter(tags=["admin"])
logger = logging.getLogger(__name__)


# ── Auth ──────────────────────────────────────────────────────────────────────

@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest):
    if not verify_admin_password(body.password):
        raise HTTPException(status_code=401, detail="Invalid password")
    token = create_access_token({"role": "admin", "sub": "admin"})
    return TokenResponse(access_token=token)


# ── Campaigns ──────────────────────────────────────────────────────────────────

@router.get("/campaigns", response_model=list[CampaignOut])
async def list_campaigns(
    active_only: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    stmt = select(Campaign).order_by(Campaign.code)
    if active_only:
        stmt = stmt.where(Campaign.active.is_(True))
    result = await db.execute(stmt)
    return result.scalars().all()


@router.post("/campaigns", response_model=CampaignOut, status_code=201)
async def create_campaign(
    body: CampaignCreate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    existing = await db.execute(
        select(Campaign).where(Campaign.code == body.code)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Campaign {body.code!r} already exists")

    campaign = Campaign(**body.model_dump())
    db.add(campaign)
    await db.commit()
    await db.refresh(campaign)
    return campaign


@router.get("/campaigns/{code}", response_model=CampaignOut)
async def get_campaign(
    code: str,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Campaign).where(Campaign.code == code))
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign


@router.put("/campaigns/{code}", response_model=CampaignOut)
async def update_campaign(
    code: str,
    body: CampaignUpdate,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Campaign).where(Campaign.code == code))
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(campaign, field, value)

    await db.commit()
    await db.refresh(campaign)
    return campaign


# ── Analyses ──────────────────────────────────────────────────────────────────

@router.get("/analyses", response_model=list[AnalysisOut])
async def list_analyses(
    campaign_code: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    stmt = (
        select(Analysis)
        .order_by(desc(Analysis.created_at))
        .limit(limit)
        .offset(offset)
    )
    if campaign_code:
        stmt = stmt.where(Analysis.campaign_code == campaign_code)
    if status:
        stmt = stmt.where(Analysis.processing_status == status)

    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/analyses/{analysis_id}", response_model=AnalysisDetailOut)
async def get_analysis(
    analysis_id: int,
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    result = await db.execute(select(Analysis).where(Analysis.id == analysis_id))
    analysis = result.scalar_one_or_none()
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    return analysis


# ── Health ────────────────────────────────────────────────────────────────────

@router.get("/health")
async def admin_health(
    db: AsyncSession = Depends(get_db),
    _=Depends(require_admin),
):
    from sqlalchemy import text

    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        logger.error("DB health check failed: %s", exc)
        db_ok = False

    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "ok" if db_ok else "error",
    }
