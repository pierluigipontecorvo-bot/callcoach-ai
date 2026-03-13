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
  GET  /admin/ui/global                     — edit global documents (_GLOBAL_)
  POST /admin/ui/upload-extract             — extract text from uploaded file
  GET  /admin/ui/analyses                   — list analyses (last 100)
  GET  /admin/ui/analyses/{id}              — analysis detail
"""

import asyncio
import io
import logging
import re
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from jose import JWTError, jwt
from sqlalchemy import delete as sa_delete, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database import get_db
from models import Analysis, Campaign
from services.campaign_db import GLOBAL_CODE
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
            "flash_ok": request.query_params.get("ok", ""),
            "flash_err": request.query_params.get("err", ""),
        },
    )


# ── Clear all analyses ────────────────────────────────────────────────────────

@router.post("/analyses/clear")
async def clear_analyses(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    count_result = await db.execute(select(func.count()).select_from(Analysis))
    count = count_result.scalar_one_or_none() or 0
    await db.execute(sa_delete(Analysis))
    await db.commit()

    from urllib.parse import quote
    msg = quote(f"{count} analisi eliminate.")
    return RedirectResponse(url=f"/admin/ui/analyses?ok={msg}", status_code=303)


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


# ── Analysis print / PDF ──────────────────────────────────────────────────────

@router.get("/analyses/{analysis_id}/print", response_class=HTMLResponse)
async def analysis_print(
    analysis_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """
    Serve the analysis report as a standalone HTML page suitable for
    browser print-to-PDF.  Transcript and raw JSON are excluded.
    """
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Analysis).where(Analysis.id == analysis_id))
    analysis = result.scalar_one_or_none()
    if not analysis or not analysis.report_html:
        return RedirectResponse(url=f"/admin/ui/analyses/{analysis_id}", status_code=303)

    # Inject print controls and print CSS into the report HTML
    report_id_label = f"Report #{analysis_id}"
    if analysis.campaign_code:
        report_id_label += f" &mdash; {analysis.campaign_code}"

    print_bar = f"""
<style>
  @media print {{
    .ec-print-bar {{ display: none !important; }}
    body {{ background: #fff !important; }}
  }}
</style>
<div class="ec-print-bar" style="
  position:sticky; top:0; z-index:100;
  background:#001126; color:#fff;
  padding:8px 16px;
  display:flex; align-items:center; justify-content:space-between;
  font-family:Arial,sans-serif; font-size:12px;
">
  <span style="color:rgba(255,255,255,.6)">{report_id_label}</span>
  <div style="display:flex;gap:8px">
    <button onclick="window.print()" style="
      background:#fff; color:#001126; border:none;
      padding:5px 14px; border-radius:4px; cursor:pointer;
      font-size:12px; font-weight:700; letter-spacing:.3px;
    ">Stampa / Salva PDF</button>
    <button onclick="window.close()" style="
      background:rgba(255,255,255,.15); color:#fff; border:none;
      padding:5px 12px; border-radius:4px; cursor:pointer; font-size:12px;
    ">Chiudi</button>
  </div>
</div>
"""

    html = analysis.report_html
    if "<body>" in html:
        html = html.replace("<body>", f"<body>\n{print_bar}", 1)
    else:
        html = print_bar + html

    return HTMLResponse(content=html)


# ── Appointments list ─────────────────────────────────────────────────────────

@router.get("/appointments", response_class=HTMLResponse)
async def appointments_list(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    from datetime import datetime, timedelta, timezone

    from services.acuity import find_operator_email, format_operator_display, list_appointments
    from services.campaign_parser import parse_campaign_code
    from utils.helpers import parse_iso_datetime

    now = datetime.now(timezone.utc)
    min_date = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    max_date = (now + timedelta(days=30)).strftime("%Y-%m-%d")

    # Fetch from both accounts in parallel
    acuity_results = await asyncio.gather(
        list_appointments(1, min_date=min_date, max_date=max_date),
        list_appointments(2, min_date=min_date, max_date=max_date),
        return_exceptions=True,
    )
    appts_1 = acuity_results[0] if isinstance(acuity_results[0], list) else []
    appts_2 = acuity_results[1] if isinstance(acuity_results[1], list) else []

    for a in appts_1:
        a["_account"] = 1
    for a in appts_2:
        a["_account"] = 2

    all_appts = sorted(appts_1 + appts_2, key=lambda a: a.get("datetime", ""), reverse=True)

    # Load all active campaigns once for in-memory prefix matching
    camp_result = await db.execute(
        select(Campaign)
        .where(Campaign.active.is_(True))
        .where(Campaign.code != GLOBAL_CODE)
    )
    all_campaigns: dict[str, Campaign] = {c.code: c for c in camp_result.scalars().all()}

    # Load existing analyses for these appointments (one query)
    appt_ids = [str(a["id"]) for a in all_appts]
    if appt_ids:
        ana_result = await db.execute(
            select(Analysis.appointment_id, Analysis.processing_status, Analysis.id,
                   Analysis.progress, Analysis.step_message)
            .where(Analysis.appointment_id.in_(appt_ids))
        )
        analyses_map: dict[str, dict] = {
            row.appointment_id: {
                "status": row.processing_status,
                "id": row.id,
                "progress": row.progress or 0,
                "step_message": row.step_message or "",
            }
            for row in ana_result.all()
        }
    else:
        analyses_map = {}

    enriched = []
    for a in all_appts:
        appt_id = str(a["id"])
        parsed = parse_campaign_code(a.get("type", ""))
        campaign_code = parsed.get("raw") if parsed.get("valid") else None

        # In-memory longest-prefix match
        campaign_cfg = _match_campaign_prefix(campaign_code, all_campaigns) if campaign_code else None

        # Operator
        op_email = find_operator_email(a)
        op_display = format_operator_display(op_email) if op_email else "—"

        # Ragione sociale — look in form fields first
        ragione = _extract_ragione_sociale(a)

        # Labels
        labels = [lbl.get("name", "") for lbl in (a.get("labels") or [])]

        # Datetime
        dt_raw = a.get("datetime", "")
        try:
            dt_obj = parse_iso_datetime(dt_raw)
            is_past = dt_obj < now if dt_obj else False
            dt_display = dt_obj.strftime("%d/%m/%Y %H:%M") if dt_obj else dt_raw[:16].replace("T", " ")
        except Exception:
            is_past = False
            dt_display = dt_raw[:16].replace("T", " ")

        enriched.append({
            "id": appt_id,
            "account": a["_account"],
            "dt_display": dt_display,
            "campaign_code": campaign_code or a.get("type", "—"),
            "campaign_cfg": campaign_cfg,
            "ragione": ragione,
            "op_display": op_display,
            "labels": labels,
            "is_past": is_past,
            "analysis": analyses_map.get(appt_id),
        })

    return templates.TemplateResponse(
        "appointments_list.html",
        {
            "request": request,
            "appointments": enriched,
            "active_page": "appointments",
            "flash_ok": request.query_params.get("ok", ""),
            "flash_err": request.query_params.get("err", ""),
        },
    )


@router.post("/appointments/{account_id}/{appointment_id}/analyze")
async def trigger_appointment_analysis(
    account_id: int,
    appointment_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    if not _is_admin(request):
        return _login_redirect()

    from services.acuity import get_appointment
    from routers.webhook import run_analysis_pipeline
    from urllib.parse import quote

    full_appointment = await get_appointment(appointment_id, account_id)
    if not full_appointment:
        msg = quote(f"Impossibile recuperare l'appuntamento {appointment_id} da Acuity.")
        return RedirectResponse(url=f"/admin/ui/appointments?err={msg}", status_code=303)

    background_tasks.add_task(
        run_analysis_pipeline,
        appointment_data=full_appointment,
        acuity_account=account_id,
    )

    msg = quote(f"Analisi avviata per appuntamento {appointment_id}. Apparirà nella lista analisi al termine (qualche minuto).")
    return RedirectResponse(url=f"/admin/ui/appointments?ok={msg}", status_code=303)


# ── Global documents (/admin/ui/global) ──────────────────────────────────────

@router.get("/global", response_class=HTMLResponse)
async def global_docs_form(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Campaign).where(Campaign.code == GLOBAL_CODE))
    campaign = result.scalar_one_or_none()

    flash_ok  = request.query_params.get("ok", "")
    flash_err = request.query_params.get("err", "")

    return templates.TemplateResponse(
        "campaigns_form.html",
        {
            "request": request,
            "campaign": campaign,
            "active_page": "global",
            "is_global": True,
            "flash_ok": flash_ok,
            "flash_err": flash_err,
            "form_data": None,
        },
    )


@router.post("/global")
async def global_docs_submit(
    request: Request,
    db: AsyncSession = Depends(get_db),
    script: str = Form(""),
    qualification_params: str = Form(""),
    client_info: str = Form(""),
    notes: str = Form(""),
    active: str = Form("off"),
):
    if not _is_admin(request):
        return _login_redirect()

    result = await db.execute(select(Campaign).where(Campaign.code == GLOBAL_CODE))
    campaign = result.scalar_one_or_none()

    if not campaign:
        # First save — create the row
        campaign = Campaign(
            code=GLOBAL_CODE,
            type="GLOBAL",
            nome="Documenti Globali — tutte le campagne",
            active=(active == "on"),
        )
        db.add(campaign)

    campaign.script = script.strip() or None
    campaign.qualification_params = qualification_params.strip() or None
    campaign.client_info = client_info.strip() or None
    campaign.notes = notes.strip() or None
    campaign.active = (active == "on")

    await db.commit()

    from urllib.parse import quote
    msg = quote("Documenti globali salvati.")
    return RedirectResponse(url=f"/admin/ui/global?ok={msg}", status_code=303)


# ── File text extraction (/admin/ui/upload-extract) ───────────────────────────

@router.post("/upload-extract")
async def upload_extract(
    request: Request,
    file: UploadFile = File(...),
):
    """
    Receive an uploaded file and return its text content as JSON.
    Supported: PDF, DOCX, TXT.  Images and Google Docs not supported.
    Auth checked via cookie (same as other UI routes).
    """
    if not _is_admin(request):
        return JSONResponse({"error": "Non autenticato"}, status_code=401)

    filename = file.filename or ""
    data = await file.read()
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""

    try:
        if ext == "pdf":
            text = _extract_pdf(data)
        elif ext in ("docx", "doc"):
            text = _extract_docx(data)
        elif ext == "txt":
            text = data.decode("utf-8", errors="ignore")
        else:
            return JSONResponse(
                {"error": f"Formato «{ext}» non supportato. Usa PDF, DOCX o TXT."},
                status_code=400,
            )
    except Exception as exc:
        logger.error("File extraction error (%s): %s", filename, exc)
        return JSONResponse({"error": f"Errore durante l'estrazione: {exc}"}, status_code=500)

    return JSONResponse({"text": text, "filename": filename, "chars": len(text)})


def _extract_pdf(data: bytes) -> str:
    """Extract text from a PDF file."""
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    parts = []
    for page in reader.pages:
        t = page.extract_text()
        if t:
            parts.append(t)
    return "\n\n".join(parts)


def _extract_docx(data: bytes) -> str:
    """Extract text from a DOCX file."""
    from docx import Document
    doc = Document(io.BytesIO(data))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


# ── Private helpers ───────────────────────────────────────────────────────────

def _match_campaign_prefix(campaign_code: str, all_campaigns: dict) -> Optional[Campaign]:
    """In-memory longest-prefix match (mirrors campaign_db.get_campaign_by_code)."""
    if not campaign_code:
        return None
    tokens = campaign_code.split("-")
    for i in range(len(tokens), 0, -1):
        candidate = "-".join(tokens[:i])
        if candidate in all_campaigns:
            return all_campaigns[candidate]
    return None


def _extract_ragione_sociale(appt: dict) -> str:
    """
    Try to find the company/client name from Acuity form fields,
    falling back to firstName + lastName.
    """
    KEYWORDS = ("ragione", "azienda", "cliente", "societa", "company")
    for form in (appt.get("forms") or []):
        for val in (form.get("values") or []):
            name_lower = re.sub(r"[àèéìòù]", "a", (val.get("name") or "").lower())
            if any(kw in name_lower for kw in KEYWORDS):
                v = (val.get("value") or "").strip()
                if v:
                    return v
    parts = [appt.get("firstName", ""), appt.get("lastName", "")]
    return " ".join(p for p in parts if p).strip() or "—"


def _parse_recipients(raw: str) -> list[str]:
    """Split a textarea (one email per line) into a list of non-empty strings."""
    return [e.strip() for e in raw.splitlines() if e.strip()]


def _form_snapshot(local_vars: dict) -> dict:
    """Collect form field values for re-populating the form on error."""
    keys = ("code", "nome", "script", "qualification_params",
            "client_info", "email_recipients_raw", "notes", "active")
    return {k: local_vars.get(k, "") for k in keys}
