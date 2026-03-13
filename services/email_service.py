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
    """Render a coloured ●●○ badge for a 1-3 rating (or — for null)."""
    if rating is None:
        return '<span style="color:#bdc3c7;font-size:13px">— non applicabile</span>'
    colors = {1: "#e74c3c", 2: "#e67e22", 3: "#27ae60"}
    labels = {1: "INACCURATA", 2: "DA MIGLIORARE", 3: "BUONA"}
    color  = colors.get(rating, "#95a5a6")
    label  = labels.get(rating, str(rating))
    filled = "●" * rating + "○" * (3 - rating)
    return (
        f'<span style="color:{color};font-size:17px;letter-spacing:3px">{filled}</span>'
        f'&nbsp;<span style="color:{color};font-weight:700;font-size:12px">'
        f'{rating}/3 — {label}</span>'
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
    model_name: str = "claude-haiku-4-5-20251001",
) -> str:
    """Render the analysis report as a self-contained HTML email."""

    # ── Extract key fields ────────────────────────────────────────────────────
    qual        = report.get("qualificazione", {})
    analisi     = report.get("analisi_telefonata", {})
    fasi        = analisi.get("fasi", {})
    qual_rating = qual.get("rating", 2)

    rating_colors = {1: "#e74c3c", 2: "#e67e22", 3: "#27ae60"}
    accent_color  = rating_colors.get(qual_rating, "#2980b9")

    operator_name  = campaign_info.get("agente", "N/A")
    raw_code       = campaign_info.get("raw", "N/A")
    campaign_client = campaign_info.get("cliente", "N/A")
    ragione_sociale = report.get("ragione_sociale", "N/A")

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
        f'<li style="margin:3px 0">✅ {p}</li>'
        for p in qual.get("parametri_verificati", [])
    ) or "<li>—</li>"

    params_ko_html = "".join(
        f'<li style="margin:3px 0">❌ {p}</li>'
        for p in qual.get("parametri_mancanti", [])
    ) or "<li>—</li>"

    # ── Analisi fasi table ────────────────────────────────────────────────────
    fasi_rows = ""
    for key, label in _FASE_LABELS.items():
        fase = fasi.get(key, {})
        r    = fase.get("rating")
        spieg = fase.get("spiegazione", "")
        fasi_rows += f"""
        <tr style="border-bottom:1px solid #f0f0f0">
          <td style="padding:8px 12px;width:35%;font-weight:600;color:#555">{label}</td>
          <td style="padding:8px 12px;width:25%">{_rating_badge(r)}</td>
          <td style="padding:8px 12px;font-size:13px;color:#666">{spieg}</td>
        </tr>"""

    # ── Punti di forza ────────────────────────────────────────────────────────
    punti_html = ""
    for i, p in enumerate(report.get("punti_di_forza", []), start=1):
        punti_html += f"""
        <div style="background:#f0faf4;border-left:4px solid #27ae60;
                    padding:12px 16px;margin:10px 0;border-radius:4px">
          <div style="font-weight:700;color:#27ae60;margin-bottom:6px">
            {i}. {p.get('titolo', '')}
          </div>
          <div style="font-size:13px;margin-bottom:4px">
            <strong>Hai detto:</strong>
            <em style="color:#333">&ldquo;{p.get('hai_detto', '')}&rdquo;</em>
          </div>
          <div style="font-size:13px;color:#555">
            <strong>Perché efficace:</strong> {p.get('perche_efficace', '')}
          </div>
        </div>"""

    # ── Aree di miglioramento ─────────────────────────────────────────────────
    aree_html = ""
    for i, a in enumerate(report.get("aree_di_miglioramento", []), start=1):
        aree_html += f"""
        <div style="background:#fffbf0;border-left:4px solid #e67e22;
                    padding:12px 16px;margin:10px 0;border-radius:4px">
          <div style="font-weight:700;color:#e67e22;margin-bottom:6px">
            {i}. {a.get('titolo', '')}
          </div>
          <div style="font-size:13px;margin-bottom:4px">
            <strong>Hai detto:</strong>
            <em style="color:#c0392b">&ldquo;{a.get('hai_detto', '')}&rdquo;</em>
          </div>
          <div style="font-size:13px;margin-bottom:4px">
            <strong>Avresti potuto dire:</strong>
            <em style="color:#27ae60">&ldquo;{a.get('avresti_potuto_dire', '')}&rdquo;</em>
          </div>
          <div style="font-size:13px;color:#555">
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
  body {{ font-family: Arial, Helvetica, sans-serif; max-width: 720px;
         margin: 0 auto; color: #333; background: #f5f6fa; padding: 12px; }}
  .card {{ background: white; border-radius: 8px; border: 1px solid #e0e0e0;
           padding: 18px 20px; margin: 12px 0; }}
  .card h3 {{ margin-top: 0; font-size: 15px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  ul {{ margin: 6px 0; padding-left: 18px; line-height: 1.7; font-size: 13px; }}
</style>
</head>
<body>

<!-- ─── HEADER ─── -->
<div style="background:#2c3e50;color:white;padding:20px 24px;border-radius:8px 8px 0 0">
  <div style="font-size:11px;opacity:0.7;letter-spacing:1px;text-transform:uppercase;
              margin-bottom:4px">Effoncall — CallCoach AI</div>
  <h2 style="margin:0;font-size:20px">📞 Report Analisi Chiamata</h2>
  <div style="margin-top:8px;font-size:13px;opacity:0.85">Campagna: <strong>{raw_code}</strong></div>
</div>

<!-- ─── META INFO ─── -->
<div class="card" style="border-radius:0;border-top:none;background:#fafafa">
  <table>
    <tr>
      <td style="padding:4px 8px;width:40%;color:#777;font-size:13px">📅 Data report</td>
      <td style="padding:4px 8px;font-size:13px"><strong>{report_date}</strong></td>
      <td style="padding:4px 8px;width:40%;color:#777;font-size:13px">🤖 Autore</td>
      <td style="padding:4px 8px;font-size:13px"><strong>{model_name}</strong></td>
    </tr>
    <tr>
      <td style="padding:4px 8px;color:#777;font-size:13px">👤 Operatore</td>
      <td style="padding:4px 8px;font-size:13px"><strong>{operator_name}</strong></td>
      <td style="padding:4px 8px;color:#777;font-size:13px">🏷️ Campagna</td>
      <td style="padding:4px 8px;font-size:13px"><strong>{raw_code}</strong></td>
    </tr>
    <tr>
      <td style="padding:4px 8px;color:#777;font-size:13px">🏢 Ragione sociale</td>
      <td style="padding:4px 8px;font-size:13px"><strong>{ragione_sociale}</strong></td>
      <td style="padding:4px 8px;color:#777;font-size:13px">🌍 Cliente EC</td>
      <td style="padding:4px 8px;font-size:13px"><strong>{campaign_client}</strong></td>
    </tr>
    <tr>
      <td style="padding:4px 8px;color:#777;font-size:13px">🗓️ Appuntamento</td>
      <td colspan="3" style="padding:4px 8px;font-size:13px"><strong>{appt_display}</strong></td>
    </tr>
  </table>
</div>

<!-- ─── QUALIFICAZIONE ─── -->
<div class="card">
  <h3 style="color:{accent_color}">📋 QUALIFICAZIONE</h3>
  <div style="font-size:22px;margin:8px 0">{_rating_badge(qual_rating)}</div>
  <p style="font-size:14px;color:#555;margin:8px 0">{qual.get('spiegazione', '')}</p>

  <div style="display:flex;gap:20px;margin-top:12px;flex-wrap:wrap">
    <div style="flex:1;min-width:220px">
      <div style="font-weight:700;font-size:13px;color:#27ae60;margin-bottom:4px">
        ✅ Parametri verificati
      </div>
      <ul>{params_ok_html}</ul>
    </div>
    <div style="flex:1;min-width:220px">
      <div style="font-weight:700;font-size:13px;color:#e74c3c;margin-bottom:4px">
        ❌ Parametri non richiesti
      </div>
      <ul>{params_ko_html}</ul>
    </div>
  </div>
</div>

<!-- ─── ANALISI TELEFONATA ─── -->
<div class="card">
  <h3 style="color:#2c3e50">📊 ANALISI TELEFONATA</h3>
  <div style="font-size:22px;margin:8px 0">{_rating_badge(analisi.get('rating_totale'))}</div>
  <p style="font-size:14px;color:#555;margin:8px 0">{analisi.get('spiegazione_totale', '')}</p>

  <table style="margin-top:14px;font-size:13px">
    <thead>
      <tr style="background:#f8f9fa;border-bottom:2px solid #e0e0e0">
        <th style="padding:8px 12px;text-align:left;color:#555">Fase</th>
        <th style="padding:8px 12px;text-align:left;color:#555">Rating</th>
        <th style="padding:8px 12px;text-align:left;color:#555">Note</th>
      </tr>
    </thead>
    <tbody>{fasi_rows}</tbody>
  </table>
</div>

<!-- ─── PUNTI DI FORZA ─── -->
<div class="card">
  <h3 style="color:#27ae60">✅ PUNTI DI FORZA</h3>
  {punti_html}
</div>

<!-- ─── AREE DI MIGLIORAMENTO ─── -->
<div class="card">
  <h3 style="color:#e67e22">⚠️ AREE DI MIGLIORAMENTO</h3>
  {aree_html}
</div>

<!-- ─── FRASE MOTIVAZIONALE ─── -->
<div style="background:#fff8e1;border:1px solid #ffd54f;border-radius:8px;
            padding:14px 18px;margin:12px 0;text-align:center;
            font-size:15px;font-style:italic;color:#5d4037">
  🌟 {report.get('frase_motivazionale', '')}
</div>

<!-- ─── DISCLAIMER ─── -->
<div style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:0 0 8px 8px;
            padding:12px 18px;font-size:11px;color:#888;line-height:1.6">
  <strong>⚠️ DISCLAIMER</strong><br>
  Questo report deve essere considerato un aiuto formativo e ha il solo scopo di aiutare
  gli operatori a migliorare le loro prestazioni individuali, contenendo consigli basati
  su fatti oggettivi. Questi consigli e suggerimenti non devono in alcun modo essere
  interpretati come imposizioni.<br><br>
  Report generato automaticamente da CallCoach AI — Effoncall |
  Non rispondere a questa email | callcoach@effoncall.com
</div>

</body>
</html>"""
