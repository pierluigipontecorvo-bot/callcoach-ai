from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, Text, DateTime,
    ARRAY, func
)
from sqlalchemy.dialects.postgresql import JSONB
from database import Base


class GlobalDocument(Base):
    __tablename__ = "global_documents"

    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False, server_default="")
    sort_order = Column(Integer, nullable=False, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class PromptSection(Base):
    __tablename__ = "prompt_sections"
    id = Column(Integer, primary_key=True)
    section_key = Column(String(50), unique=True, nullable=False)
    title = Column(String(100), nullable=False)
    content = Column(Text, nullable=False, server_default="")
    sort_order = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Campaign(Base):
    __tablename__ = "campaigns"

    id = Column(Integer, primary_key=True)
    code = Column(String(100), unique=True, nullable=False)
    type = Column(String(20), nullable=False)
    client_name = Column(String(200))
    agent_name = Column(String(100))
    province = Column(String(50))
    numeric_code = Column(String(20))
    is_multisede = Column(Boolean, default=False)
    nome = Column(String(300))              # human-readable label, e.g. "Mailbox – tutte"
    script = Column(Text)
    qualification_params = Column(Text)
    client_info = Column(Text)
    email_recipients = Column(ARRAY(String))
    email_no_operator = Column(Boolean, default=False)   # non inviare all'operatore
    email_disabled    = Column(Boolean, default=False)   # non inviare a nessuno
    notes = Column(Text)                   # internal notes
    prompt_extra = Column(Text)            # per-campaign AI prompt override
    transcription_engine = Column(String, nullable=True)  # None = use global default
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Operator(Base):
    __tablename__ = "operators"

    id = Column(Integer, primary_key=True)
    number = Column(String(10), unique=True, nullable=False)  # XX (e.g. "71")
    display_name = Column(String(100))   # e.g. "STEFANIA M."
    email = Column(String(200))          # e.g. op.71.stefania@effoncall.com
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Setting(Base):
    __tablename__ = "settings"

    key = Column(String(100), primary_key=True)
    value = Column(Text)
    description = Column(Text)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Analysis(Base):
    __tablename__ = "analyses"

    id = Column(Integer, primary_key=True)
    campaign_code = Column(String(100))
    appointment_id = Column(String(100))
    appointment_datetime = Column(DateTime(timezone=True))
    client_phone = Column(String(50))
    client_company = Column(String(300))
    operator_name  = Column(String(100))
    operator_email = Column(String(200))
    acuity_account = Column(Integer)
    acuity_label = Column(String(100))
    sidial_call_id = Column(String(100))
    acuity_form_fields = Column(JSONB)      # tutti i form fields Acuity {nome: valore}
    label_name = Column(String(100))        # e.g. "PRESO"
    label_color = Column(String(50))        # e.g. "cream"
    pipeline_steps = Column(JSONB, default=dict)   # 14-step status tracking
    num_recordings = Column(Integer, default=0)
    total_talk_seconds = Column(Integer, default=0)
    transcript = Column(Text)
    qualification_level = Column(String(50))
    report_json = Column(JSONB)
    report_html = Column(Text)
    email_sent = Column(Boolean, default=False)
    email_sent_at = Column(DateTime(timezone=True))
    processing_status = Column(String(50), default="pending")
    progress = Column(Integer, default=0)
    step_message = Column(String(300))
    error_message = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True)
    campaign_code = Column(String(100))
    filename = Column(String(300), nullable=False)
    file_type = Column(String(20))
    content_extracted = Column(Text)
    storage_path = Column(Text)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
