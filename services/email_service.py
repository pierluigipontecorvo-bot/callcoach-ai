"""
Email service — Resend API (HTTPS) with Aruba SMTP fallback.

Railway blocks outbound SMTP (ports 465/587 to Aruba are rejected).
Resend works via HTTPS on port 443, which Railway allows.
Set RESEND_API_KEY env var to enable Resend; otherwise falls back to SMTP.
"""

import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib
import httpx

from config import settings

logger = logging.getLogger(__name__)

# Rating 1-3 → emoji for email subject
_RATING_EMOJI = {1: "❌", 2: "⚠️", 3: "✅"}
# Backward compat with old string-based levels
_LEVEL_EMOJI = {
    "inaccurata": "❌", "da_migliorare": "⚠️", "buona": "✅",
    "eccellente": "⭐", "corretta": "✅", "insufficiente": "❌",
}

_FROM_ADDRESS = "CallCoach AI <callcoach@effoncall.com>"


async def _send_via_resend(
    recipients: list[str],
    html_content: str,
    subject: str,
) -> None:
    """Send via Resend REST API (HTTPS — no SMTP port required)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {settings.resend_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "from": _FROM_ADDRESS,
                "to": recipients,
                "subject": subject,
                "html": html_content,
            },
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Resend API error {resp.status_code}: {resp.text[:300]}"
            )
    logger.info("Email sent via Resend to %s", recipients)


async def _send_via_smtp(
    recipients: list[str],
    html_content: str,
    subject: str,
) -> None:
    """Send via Aruba SMTP (fallback — may be blocked from Railway)."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = _FROM_ADDRESS
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    use_tls   = settings.smtp_port == 465
    start_tls = settings.smtp_port == 587

    await aiosmtplib.send(
        msg,
        hostname=settings.smtp_host,
        port=settings.smtp_port,
        username=settings.smtp_user,
        password=settings.smtp_password,
        use_tls=use_tls,
        start_tls=start_tls,
    )
    logger.info("Email sent via SMTP to %s", recipients)


async def send_analysis_report(
    recipients: list[str],
    html_content: str,
    operator_name: str,
    qualification_level: str,
    appointment_datetime: str,
) -> None:
    """
    Send the HTML analysis report.
    Uses Resend API if RESEND_API_KEY is configured, otherwise SMTP.
    Raises on send failure so the caller can record the error.
    """
    emoji = _LEVEL_EMOJI.get(qualification_level, "📊")
    date_part = appointment_datetime[:10] if appointment_datetime else "N/A"
    subject = f"{emoji} CallCoach AI — Report {operator_name} — {date_part}"

    logger.info("Sending email to %s — subject: %s", recipients, subject)

    if settings.resend_api_key:
        await _send_via_resend(recipients, html_content, subject)
    else:
        await _send_via_smtp(recipients, html_content, subject)


# ── HTML helpers ───────────────────────────────────────────────────────────────

def _rating_badge(rating) -> str:
    """Render a coloured ●●○ badge for a 1-5 rating (or — for null)."""
    if rating is None:
        return '<span style="color:#bdc3c7;font-size:13px">— non applicabile</span>'
    colors = {1: "#c0392b", 2: "#e67e22", 3: "#d4ac0d", 4: "#27ae60", 5: "#1a5276"}
    labels = {1: "Insufficiente", 2: "Da migliorare", 3: "Sufficiente", 4: "Buona", 5: "Eccellente"}
    color  = colors.get(rating, "#708090")
    label  = labels.get(rating, str(rating))
    filled = "●" * rating + "○" * (5 - rating)
    return (
        f'<span style="color:{color};font-size:16px;letter-spacing:3px">{filled}</span>'
        f'&nbsp;<span style="color:{color};font-weight:700;font-size:12px;letter-spacing:.3px">'
        f'{rating}/5 — {label}</span>'
    )


_FASE_LABELS = {
    "apertura":                  "Apertura",
    "superamento_gatekeeper":    "Superamento Gatekeeper",
    "introduzione_decision_maker": "Introduzione al DM",
    "trasmissione_valore":       "Trasmissione del Valore",
    "superamento_obiezioni":     "Superamento Obiezioni",
    "negoziazione":              "Negoziazione",
    "chiusura":                  "Chiusura",
}


# ── HTML report generator ──────────────────────────────────────────────────────

def generate_html_report(
    report: dict,
    appointment_info: dict,
    campaign_info: dict,
    operator_name: str = "",
    client_company: str = "",
    model_name: str = "claude-haiku-4-5-20251001",
) -> str:
    """Render the analysis report as a self-contained HTML email."""

    # ── Extract key fields ────────────────────────────────────────────────────
    qual        = report.get("qualificazione", {})
    analisi     = report.get("analisi_telefonata", {})
    fasi        = analisi.get("fasi", {})
    qual_rating      = qual.get("rating", 3)
    fuori_parametro  = qual.get("fuori_parametro", False)

    rating_colors = {1: "#c0392b", 2: "#e67e22", 3: "#d4ac0d", 4: "#27ae60", 5: "#1a5276"}
    accent_color  = rating_colors.get(qual_rating, "#2980b9")

    operator_name  = operator_name or "N/A"
    raw_code       = campaign_info.get("raw", "N/A")
    campaign_client = campaign_info.get("cliente", "N/A")
    # Ragione sociale: sempre da Acuity, mai dall'AI
    ragione_sociale = client_company or appointment_info.get("firstName", "") or "N/A"

    # Appointment date/time: prefer values extracted by Claude from transcript
    data_appt = report.get("data_appuntamento") or ""
    ora_appt  = report.get("ora_appuntamento") or ""
    if data_appt and ora_appt:
        appt_display = f"{data_appt} ore {ora_appt}"
    elif data_appt:
        appt_display = data_appt
    else:
        appt_display = appointment_info.get("datetime", "N/A")

    report_date = datetime.utcnow().strftime("%d/%m/%Y")

    # ── Qualificazione section ────────────────────────────────────────────────
    params_ok_html = "".join(
        f'<li style="margin:3px 0"><span style="color:#1e8449;font-weight:600">✓</span> {p}</li>'
        for p in qual.get("parametri_verificati", [])
    ) or "<li style='color:#999'>—</li>"

    params_ko_html = "".join(
        f'<li style="margin:3px 0"><span style="color:#c0392b;font-weight:600">✗</span> {p}</li>'
        for p in qual.get("parametri_mancanti", [])
    ) or "<li style='color:#999'>—</li>"

    # ── Analisi fasi table ────────────────────────────────────────────────────
    fasi_rows = ""
    for key, label in _FASE_LABELS.items():
        fase  = fasi.get(key, {})
        r     = fase.get("rating")
        spieg = fase.get("spiegazione", "")
        fasi_rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:8px 12px;width:28%;font-weight:600;color:#001126;font-size:12px">{label}</td>
          <td style="padding:8px 12px;width:28%">{_rating_badge(r)}</td>
          <td style="padding:8px 12px;font-size:12px;color:#555;line-height:1.5">{spieg}</td>
        </tr>"""

    # ── Punti di forza ────────────────────────────────────────────────────────
    punti_html = ""
    for i, p in enumerate(report.get("punti_di_forza", []), start=1):
        punti_html += f"""
        <div style="border-left:3px solid #1e8449;padding:10px 14px;margin:8px 0;background:#f7fdf9">
          <div style="font-weight:700;color:#1e8449;font-size:13px;margin-bottom:5px">
            {i}. {p.get('titolo', '')}
          </div>
          <div style="font-size:12px;margin-bottom:4px;color:#333">
            <strong>Hai detto:</strong>
            <em>&ldquo;{p.get('hai_detto', '')}&rdquo;</em>
          </div>
          <div style="font-size:12px;color:#555">
            <strong>Perché efficace:</strong> {p.get('perche_efficace', '')}
          </div>
        </div>"""

    # ── Aree di miglioramento ─────────────────────────────────────────────────
    aree_html = ""
    for i, a in enumerate(report.get("aree_di_miglioramento", []), start=1):
        aree_html += f"""
        <div style="border-left:3px solid #d35400;padding:10px 14px;margin:8px 0;background:#fdf7f2">
          <div style="font-weight:700;color:#d35400;font-size:13px;margin-bottom:5px">
            {i}. {a.get('titolo', '')}
          </div>
          <div style="font-size:12px;margin-bottom:4px;color:#333">
            <strong>Hai detto:</strong>
            <em>&ldquo;{a.get('hai_detto', '')}&rdquo;</em>
          </div>
          <div style="font-size:12px;margin-bottom:4px;color:#333">
            <strong>Avresti potuto dire:</strong>
            <em style="color:#1e6031">&ldquo;{a.get('avresti_potuto_dire', '')}&rdquo;</em>
          </div>
          <div style="font-size:12px;color:#555">
            <strong>Perché:</strong> {a.get('perche', '')}
          </div>
        </div>"""

    # ── Full HTML ─────────────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  body {{ font-family: Arial, Helvetica, sans-serif; max-width: 740px;
         margin: 0 auto; color: #1a1a2e; background: #f4f5f7; padding: 12px; }}
  .card {{ background: #ffffff; border-radius: 6px; border: 1px solid #d3d2d2;
           padding: 16px 20px; margin: 10px 0; }}
  .section-title {{
    font-size: 11px; font-weight: 700; letter-spacing: 1.2px;
    text-transform: uppercase; color: #708090;
    border-bottom: 1px solid #e2e4e9; padding-bottom: 6px; margin: 0 0 12px 0;
  }}
  table {{ border-collapse: collapse; width: 100%; }}
  ul {{ margin: 4px 0; padding-left: 16px; line-height: 1.8; font-size: 13px; }}
  td {{ vertical-align: top; }}
</style>
</head>
<body>

<!-- ─── HEADER ─── -->
<div style="background:#001126;color:#ffffff;padding:18px 24px;border-radius:6px 6px 0 0">
  <div style="font-size:10px;letter-spacing:1.5px;text-transform:uppercase;
              color:rgba(255,255,255,.5);margin-bottom:6px">Effoncall — CallCoach AI</div>
  <div style="font-size:19px;font-weight:700;letter-spacing:-.2px">Report Analisi Chiamata</div>
  <div style="margin-top:6px;font-size:12px;color:rgba(255,255,255,.7)">
    Campagna&nbsp;<strong style="color:#fff">{raw_code}</strong>
    &nbsp;&mdash;&nbsp;{report_date}
  </div>
</div>

<!-- ─── META INFO ─── -->
<div class="card" style="border-radius:0;border-top:none;background:#fafbfc">
  <table>
    <tr>
      <td style="padding:5px 10px;width:20%;color:#708090;font-size:12px;white-space:nowrap">Operatore</td>
      <td style="padding:5px 10px;font-size:13px;font-weight:600;color:#001126">{operator_name}</td>
      <td style="padding:5px 10px;width:20%;color:#708090;font-size:12px;white-space:nowrap">Campagna</td>
      <td style="padding:5px 10px;font-size:13px;font-weight:600;color:#001126">{raw_code}</td>
    </tr>
    <tr>
      <td style="padding:5px 10px;color:#708090;font-size:12px">Ragione sociale</td>
      <td style="padding:5px 10px;font-size:13px;font-weight:600;color:#001126">{ragione_sociale}</td>
      <td style="padding:5px 10px;color:#708090;font-size:12px">Cliente Effoncall</td>
      <td style="padding:5px 10px;font-size:13px;font-weight:600;color:#001126">{campaign_client}</td>
    </tr>
    <tr>
      <td style="padding:5px 10px;color:#708090;font-size:12px">Data appuntamento</td>
      <td colspan="3" style="padding:5px 10px;font-size:13px;font-weight:600;color:#001126">{appt_display}</td>
    </tr>
  </table>
</div>

<!-- ─── QUALIFICAZIONE ─── -->
<div class="card">
  <div class="section-title">Qualificazione</div>
  {'<div style="background:#fdecea;border:2px solid #c0392b;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;font-weight:700;color:#c0392b;letter-spacing:.2px">⛔ APPUNTAMENTO FUORI PARAMETRO — Le soglie minime di qualificazione non sono state raggiunte.</div>' if fuori_parametro else ''}
  <div style="margin:6px 0 10px">{_rating_badge(qual_rating)}</div>
  <p style="font-size:13px;color:#444;margin:8px 0;line-height:1.6">{qual.get('spiegazione', '')}</p>

  <div style="display:flex;gap:20px;margin-top:14px;flex-wrap:wrap">
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.5px;color:#1e8449;
                  text-transform:uppercase;margin-bottom:6px">Parametri verificati</div>
      <ul>{params_ok_html}</ul>
    </div>
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;font-weight:700;letter-spacing:.5px;color:#c0392b;
                  text-transform:uppercase;margin-bottom:6px">Parametri non richiesti</div>
      <ul>{params_ko_html}</ul>
    </div>
  </div>
</div>

<!-- ─── ANALISI TELEFONATA ─── -->
<div class="card">
  <div class="section-title">Analisi Telefonata</div>
  <div style="margin:6px 0 10px">{_rating_badge(analisi.get('rating_totale'))}</div>
  <p style="font-size:13px;color:#444;margin:8px 0;line-height:1.6">{analisi.get('spiegazione_totale', '')}</p>

  <table style="margin-top:12px;font-size:12px">
    <thead>
      <tr style="background:#f4f5f7;border-bottom:2px solid #d3d2d2">
        <th style="padding:7px 12px;text-align:left;color:#708090;font-size:11px;
                   letter-spacing:.5px;text-transform:uppercase">Fase</th>
        <th style="padding:7px 12px;text-align:left;color:#708090;font-size:11px;
                   letter-spacing:.5px;text-transform:uppercase">Valutazione</th>
        <th style="padding:7px 12px;text-align:left;color:#708090;font-size:11px;
                   letter-spacing:.5px;text-transform:uppercase">Note</th>
      </tr>
    </thead>
    <tbody>{fasi_rows}</tbody>
  </table>
</div>

<!-- ─── PUNTI DI FORZA ─── -->
<div class="card">
  <div class="section-title">Punti di Forza</div>
  {punti_html if punti_html else '<p style="color:#999;font-size:13px">Nessun punto rilevato.</p>'}
</div>

<!-- ─── AREE DI MIGLIORAMENTO ─── -->
<div class="card">
  <div class="section-title">Aree di Miglioramento</div>
  {aree_html if aree_html else '<p style="color:#999;font-size:13px">Nessuna area rilevata.</p>'}
</div>

<!-- ─── FRASE MOTIVAZIONALE ─── -->
{f'''<div style="background:#f4f5f7;border:1px solid #d3d2d2;border-radius:6px;
            padding:12px 18px;margin:10px 0;text-align:center;
            font-size:14px;font-style:italic;color:#001126;line-height:1.6">
  {report.get('frase_motivazionale', '')}
</div>''' if report.get('frase_motivazionale') else ''}

<!-- ─── DISCLAIMER ─── -->
<div style="background:#f4f5f7;border:1px solid #d3d2d2;border-radius:0 0 6px 6px;
            padding:10px 18px;font-size:11px;color:#999;line-height:1.6;margin-top:0">
  <strong style="color:#708090">Nota</strong>&nbsp;&mdash;&nbsp;Questo report
  ha il solo scopo formativo. I consigli e i suggerimenti contenuti non devono essere
  interpretati come imposizioni.<br>
  Generato da CallCoach AI &mdash; Effoncall &nbsp;|&nbsp; callcoach@effoncall.com
</div>

</body>
</html>"""
