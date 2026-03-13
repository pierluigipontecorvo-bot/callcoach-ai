"""
Browser-facing admin UI router.

Serves Jinja2 HTML pages for campaign and analysis management.
Auth: JWT stored in an HttpOnly cookie (callcoach_token).

Routes:
  GET  /admin/ui/login                      — login form
  POST /admin/ui/login                      — submit credentials
  GET  /admin/ui/logout                     — clear cookie, redirect
  GET  /admin/ui/campaigns                  — list all configurations
  GET  /admin/ui/campaigns/new              — new campaign form
  POST /admin/ui/campaigns/new              — create campaign
  GET  /admin/ui/campaigns/{id}/edit        — edit form
  POST /admin/ui/campaigns/{id}/edit        — update campaign
  POST /admin/ui/campaigns/{id}/delete      — delete campaign
  GET  /admin/ui/analyses                   — list analyses (last 100)
  GET  /admin/ui/analyses/{id}              — analysis detail
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from jose import JWTError, jwt
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
from models import Analysis, Campaign
from utils.auth import create_access_token, verify_admin_password

router = APIRouter(prefix="/admin/ui", tags=["admin-ui"])
templates = Jinja2Templates(directory="templates")
logger = logging.getLogger(__name__)

_COOKIE_NAME = "callcoach_token"
_ALGORITHM = "HS256"


# ── Auth helper ───────────────────────────────────────────────────────────────

def _is_admin(request: Request) -> bool:
    """Return True if the request carries a valid admin JWT cookie."""
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        return False
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[_ALGORITHM])
        return payload.get("role") == "admin"
    except (JWTError, Exception):
        return False


def _login_redirect() -> RedirectResponse:
    return RedirectResponse(url="/admin/ui/login", status_code=303)


# ── Login / Logout ────────────────────────────────────────────────────────────

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    # Already authenticated → go straight to campaigns
    if _is_admin(request):
        return RedirectResponse(url="/admin/ui/campaigns", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": ""})


@router.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    if not verify_admin_password(password):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Password errata. Riprova."},
            status_code=401,
        )
    token = create_access_token({"role": "admin", "sub": "admin"})
    response = RedirectResponse(url="/admin/ui/campaigns", status_code=303)
    response.set_cookie(
        _COOKIE_NAME,
        token,
        httponly=True,
        max_age=86_400,   # 24 hours
        samesite="lax",
    )
    return response


@router.get("/logout")
async def logout():
    response = RedirectResponse(url="/admin/ui/login", status_code=303)
    response.delete_cookie(_COOKIE_NAME)
    return response


# ── Campaign list ─────────────────────────────────────────────────────────────

@router.get("/campaigns", response_class=HTMLResponse)
async def campaigns_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Campaign).order_by(Campaign.code))
    campaigns = result.scalars().all()

    # Flash message from URL query param (set after create/update/delete)
    flash_ok = request.query_params.get("ok", "")
    flash_err = request.query_params.get("err", "")

    return templates.TemplateResponse(
        "campaigns_list.html",
        {
            "request": request,
            "campaigns": campaigns,
            "active_page": "campaigns",
            "flash_ok": flash_ok,
            "flash_err": flash_err,
        },
    )


# ── New campaign ──────────────────────────────────────────────────────────────

@router.get("/campaigns/new", response_class=HTMLResponse)
async def campaign_new_form(request: Request):
    if not _is_admin(request):
        return _login_redirect()
    return templates.TemplateResponse(
        "campaigns_form.html",
        {
            "request": request,
            "campaign": None,
            "active_page": "campaigns",
            "flash_err": None,
            "form_data": None,
        },
    )


@router.post("/campaigns/new")
async def campaign_new_submit(
    request: Request,
    db: AsyncSession = Depends(get_db),
    code: str = Form(...),
    nome: str = Form(""),
    script: str = Form(""),
    qualification_params: str = Form(""),
    client_info: str = Form(""),
    email_recipients_raw: str = Form(""),
    notes: str = Form(""),
    active: str = Form("off"),
):
    if not _is_admin(request):
        return _login_redirect()

    code = code.strip().upper()

    # Validate
    if not code:
        return templates.TemplateResponse(
            "campaigns_form.html",
            {
                "request": request,
                "campaign": None,
                "active_page": "campaigns",
                "flash_err": "Il codice è obbligatorio.",
                "form_data": _form_snapshot(locals()),
            },
            status_code=422,
        )

    # Check duplicate
    existing = await db.execute(select(Campaign).where(Campaign.code == code))
    if existing.scalar_one_or_none():
        return templates.TemplateResponse(
            "campaigns_form.html",
            {
                "request": request,
                "campaign": None,
                "active_page": "campaigns",
                "flash_err": f"Esiste già una configurazione con codice «{code}».",
                "form_data": _form_snapshot(locals()),
            },
            status_code=422,
        )

    recipients = _parse_recipients(email_recipients_raw)
    campaign = Campaign(
        code=code,
        type=code.split("-")[0],          # derive type from first segment
        nome=nome.strip() or None,
        script=script.strip() or None,
        qualification_params=qualification_params.strip() or None,
        client_info=client_info.strip() or None,
        email_recipients=recipients or None,
        notes=notes.strip() or None,
        active=(active == "on"),
    )
    db.add(campaign)
    await db.commit()

    from urllib.parse import quote
    msg = quote(f"Configurazione «{code}» creata con successo.")
    return RedirectResponse(url=f"/admin/ui/campaigns?ok={msg}", status_code=303)


# ── Edit campaign ─────────────────────────────────────────────────────────────

@router.get("/campaigns/{campaign_id}/edit", response_class=HTMLResponse)
async def campaign_edit_form(
    campaign_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    if not campaign:
        return RedirectResponse(url="/admin/ui/campaigns", status_code=303)

    return templates.TemplateResponse(
        "campaigns_form.html",
        {
            "request": request,
            "campaign": campaign,
            "active_page": "campaigns",
            "flash_err": None,
            "form_data": None,
        },
    )


@router.post("/campaigns/{campaign_id}/edit")
async def campaign_edit_submit(
    campaign_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    nome: str = Form(""),
    script: str = Form(""),
    qualification_params: str = Form(""),
    client_info: str = Form(""),
    email_recipients_raw: str = Form(""),
    notes: str = Form(""),
    active: str = Form("off"),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    if not campaign:
        return RedirectResponse(url="/admin/ui/campaigns", status_code=303)

    campaign.nome = nome.strip() or None
    campaign.script = script.strip() or None
    campaign.qualification_params = qualification_params.strip() or None
    campaign.client_info = client_info.strip() or None
    campaign.email_recipients = _parse_recipients(email_recipients_raw) or None
    campaign.notes = notes.strip() or None
    campaign.active = (active == "on")

    await db.commit()

    from urllib.parse import quote
    msg = quote(f"Configurazione «{campaign.code}» aggiornata.")
    return RedirectResponse(url=f"/admin/ui/campaigns?ok={msg}", status_code=303)


# ── Delete campaign ───────────────────────────────────────────────────────────

@router.post("/campaigns/{campaign_id}/delete")
async def campaign_delete(
    campaign_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Campaign).where(Campaign.id == campaign_id))
    campaign = result.scalar_one_or_none()
    if not campaign:
        return RedirectResponse(url="/admin/ui/campaigns", status_code=303)

    code = campaign.code
    await db.delete(campaign)
    await db.commit()

    from urllib.parse import quote
    msg = quote(f"Configurazione «{code}» eliminata.")
    return RedirectResponse(url=f"/admin/ui/campaigns?ok={msg}", status_code=303)


# ── Analyses list ─────────────────────────────────────────────────────────────

@router.get("/analyses", response_class=HTMLResponse)
async def analyses_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(
        select(Analysis).order_by(desc(Analysis.created_at)).limit(100)
    )
    analyses = result.scalars().all()

    return templates.TemplateResponse(
        "analyses_list.html",
        {
            "request": request,
            "analyses": analyses,
            "active_page": "analyses",
        },
    )


# ── Analysis detail ───────────────────────────────────────────────────────────

@router.get("/analyses/{analysis_id}", response_class=HTMLResponse)
async def analysis_detail(
    analysis_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Analysis).where(Analysis.id == analysis_id))
    analysis = result.scalar_one_or_none()
    if not analysis:
        return RedirectResponse(url="/admin/ui/analyses", status_code=303)

    return templates.TemplateResponse(
        "analysis_detail.html",
        {
            "request": request,
            "analysis": analysis,
            "active_page": "analyses",
        },
    )


# ── Private helpers ───────────────────────────────────────────────────────────

def _parse_recipients(raw: str) -> list[str]:
    """Split a textarea (one email per line) into a list of non-empty strings."""
    return [e.strip() for e in raw.splitlines() if e.strip()]


def _form_snapshot(local_vars: dict) -> dict:
    """Collect form field values for re-populating the form on error."""
    keys = ("code", "nome", "script", "qualification_params",
            "client_info", "email_recipients_raw", "notes", "active")
    return {k: local_vars.get(k, "") for k in keys}
