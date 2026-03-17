from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    database_url: str
    supabase_service_key: str = ""

    # AI
    anthropic_api_key: str
    anthropic_model: str = "claude-haiku-4-5-20251001"

    # Sidial CRM
    sidial_api_url: str = "https://effoncall.sidial.cloud/api.php"
    sidial_api_token: str

    # Acuity Account 1
    acuity_account1_user_id: str
    acuity_account1_api_key: str
    acuity_account1_webhook_secret: Optional[str] = None

    # Acuity Account 2
    acuity_account2_user_id: str
    acuity_account2_api_key: str
    acuity_account2_webhook_secret: Optional[str] = None

    # Whether to verify Acuity webhook HMAC signatures
    acuity_verify_webhook: bool = False

    # Email — Aruba SMTP (legacy) or Resend API
    smtp_host: str = "smtps.aruba.it"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""

    # Resend transactional email (preferred — bypasses Railway SMTP block)
    resend_api_key: str = ""

    # OpenAI — Whisper API per trascrizione cloud (sostituisce Whisper locale)
    openai_api_key: str = ""

    # Security
    secret_key: str
    admin_password: str

    # Whisper model size: tiny / base / small / medium / large
    # "tiny" (~75 MB RAM, ~2x realtime on CPU) is the only viable choice on
    # Railway shared CPU. "small" takes ~10x realtime → always times out.
    whisper_model_size: str = "tiny"

    # Indirizzo mittente per le email (FROM)
    email_from_address: str = "CallCoach AI <callcoach@effoncall.com>"

    # Fallback email when no campaign recipients are configured
    fallback_email: str = "pierluigi.pontecorvo@effoncall.it"


settings = Settings()
