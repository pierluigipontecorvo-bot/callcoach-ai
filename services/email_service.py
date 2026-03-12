"""
Email service — Aruba SMTP (smtps.aruba.it:465, SSL).
"""

import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiosmtplib

from config import settings

logger = logging.getLogger(__name__)

_LEVEL_EMOJI = {
    "eccellente": "⭐",
    "corretta": "✅",
    "da_migliorare": "⚠️",
    "insufficiente": "❌",
}


async def send_analysis_report(
    recipients: list[str],
    html_content: str,
    operator_name: str,
    qualification_level: str,
    appointment_datetime: str,
) -> None:
    """
    Send the HTML analysis report via Aruba SMTP (port 465, SSL).
    Raises on send failure so the caller can record the error.
    """
    emoji = _LEVEL_EMOJI.get(qualification_level, "📊")
    date_part = appointment_datetime[:10] if appointment_datetime else "N/A"
    subject = f"{emoji} CallCoach AI — Report {operator_name} — {date_part}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"CallCoach AI <{settings.smtp_user}>"
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    logger.info(
        "Sending email to %s — subject: %s", recipients, subject
    )

    await aiosmtplib.send(
        msg,
        hostname=settings.smtp_host,
        port=settings.smtp_port,
        username=settings.smtp_user,
        password=settings.smtp_password,
        use_tls=True,
    )

    logger.info("Email sent successfully to %s", recipients)


# ── HTML report generator ──────────────────────────────────────────────────────

def generate_html_report(
    report: dict,
    appointment_info: dict,
    campaign_info: dict,
) -> str:
    """Render the analysis report as a self-contained HTML email."""

    level = report.get("livello_qualificazione", "corretta")
    level_colors = {
        "eccellente": "#27ae60",
        "corretta": "#2980b9",
        "da_migliorare": "#f39c12",
        "insufficiente": "#e74c3c",
    }
    level_labels = {
        "eccellente": "⭐ ECCELLENTE",
        "corretta": "✅ CORRETTA",
        "da_migliorare": "⚠️ DA MIGLIORARE",
        "insufficiente": "❌ INSUFFICIENTE",
    }
    color = level_colors.get(level, "#2980b9")
    label = level_labels.get(level, level.upper())

    punti_forza = "".join(
        f"<li>{p}</li>" for p in report.get("punti_di_forza", [])
    )
    aree = "".join(
        f"<li>{a}</li>" for a in report.get("aree_di_miglioramento", [])
    )

    suggerimenti_html = ""
    for s in report.get("suggerimenti_pratici", []):
        suggerimenti_html += f"""
        <div style="background:#f8f9fa;border-left:4px solid {color};padding:12px;margin:8px 0;border-radius:4px;">
            <strong>❗ Problema:</strong> {s.get('problema', '')}<br>
            <strong>💡 Suggerimento:</strong> {s.get('suggerimento', '')}<br>
            <strong>📝 Esempio:</strong> <em>"{s.get('esempio', '')}"</em>
        </div>"""

    appt_dt = appointment_info.get("datetime", "N/A")
    raw_code = campaign_info.get("raw", "")

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{ font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto; color: #333; }}
  .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 8px 8px 0 0; }}
  .badge {{ display: inline-block; background: {color}; color: white; padding: 6px 16px; border-radius: 20px; font-weight: bold; font-size: 16px; }}
  .section {{ background: white; border: 1px solid #e0e0e0; border-radius: 8px; padding: 16px; margin: 12px 0; }}
  .section h3 {{ color: {color}; margin-top: 0; }}
  ul {{ padding-left: 20px; line-height: 1.8; }}
  .footer {{ background: #ecf0f1; padding: 12px; border-radius: 0 0 8px 8px; font-size: 12px; color: #666; text-align: center; }}
  .motivazione {{ background: #fff3cd; border: 1px solid #ffc107; padding: 12px; border-radius: 8px; font-style: italic; text-align: center; font-size: 16px; margin: 12px 0; }}
</style>
</head>
<body>
<div class="header">
  <h2 style="margin:0">📞 Report Analisi Chiamata — CallCoach AI</h2>
  <p style="margin:8px 0 0 0; opacity:0.8">Effoncall | Campagna: {raw_code}</p>
</div>

<div class="section">
  <table style="width:100%;border-collapse:collapse;">
    <tr><td style="padding:4px;width:40%"><strong>👤 Operatore:</strong></td><td>{campaign_info.get('agente', 'N/A')}</td></tr>
    <tr><td style="padding:4px"><strong>📅 Appuntamento:</strong></td><td>{appt_dt}</td></tr>
    <tr><td style="padding:4px"><strong>📍 Provincia:</strong></td><td>{campaign_info.get('provincia', 'N/A')}</td></tr>
    <tr><td style="padding:4px"><strong>🏢 Cliente:</strong></td><td>{campaign_info.get('cliente', 'N/A')}</td></tr>
    <tr><td style="padding:4px"><strong>📊 Valutazione:</strong></td><td><span class="badge">{label}</span></td></tr>
  </table>
</div>

<div class="section">
  <h3>📋 Riepilogo Chiamata</h3>
  <p>{report.get('riepilogo_appuntamento', '')}</p>
  <p><em>{report.get('motivazione_livello', '')}</em></p>
</div>

<div class="section">
  <h3 style="color:#27ae60">✅ Punti di Forza</h3>
  <ul>{punti_forza}</ul>
</div>

<div class="section">
  <h3 style="color:#e74c3c">⚠️ Aree di Miglioramento</h3>
  <ul>{aree}</ul>
</div>

<div class="section">
  <h3>💡 Suggerimenti Pratici</h3>
  {suggerimenti_html}
</div>

<div class="motivazione">
  🌟 {report.get('frase_motivazionale', '')}
</div>

<div class="footer">
  Report generato automaticamente da CallCoach AI — Effoncall<br>
  Non rispondere a questa email | callcoach@effoncall.com
</div>
</body>
</html>"""
