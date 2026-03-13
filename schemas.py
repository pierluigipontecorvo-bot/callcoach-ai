from datetime import datetime
from typing import Optional, List, Any
from pydantic import BaseModel, EmailStr


# ── Campaign ──────────────────────────────────────────────────────────────────

class CampaignBase(BaseModel):
    code: str                                    # match pattern, e.g. "INTER" or full code
    nome: Optional[str] = None                   # human-readable label
    type: str
    client_name: Optional[str] = None
    agent_name: Optional[str] = None
    province: Optional[str] = None
    numeric_code: Optional[str] = None
    is_multisede: bool = False
    script: Optional[str] = None
    qualification_params: Optional[str] = None
    client_info: Optional[str] = None
    email_recipients: Optional[List[str]] = None
    notes: Optional[str] = None                 # internal notes
    active: bool = True


class CampaignCreate(CampaignBase):
    pass


class CampaignUpdate(BaseModel):
    nome: Optional[str] = None
    type: Optional[str] = None
    client_name: Optional[str] = None
    agent_name: Optional[str] = None
    province: Optional[str] = None
    numeric_code: Optional[str] = None
    is_multisede: Optional[bool] = None
    script: Optional[str] = None
    qualification_params: Optional[str] = None
    client_info: Optional[str] = None
    email_recipients: Optional[List[str]] = None
    notes: Optional[str] = None
    active: Optional[bool] = None


class CampaignOut(CampaignBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ── Analysis ──────────────────────────────────────────────────────────────────

class AnalysisOut(BaseModel):
    id: int
    campaign_code: Optional[str] = None
    appointment_id: Optional[str] = None
    appointment_datetime: Optional[datetime] = None
    client_phone: Optional[str] = None
    operator_name: Optional[str] = None
    acuity_account: Optional[int] = None
    acuity_label: Optional[str] = None
    sidial_call_id: Optional[str] = None
    qualification_level: Optional[str] = None
    report_json: Optional[Any] = None
    email_sent: bool = False
    email_sent_at: Optional[datetime] = None
    processing_status: str
    error_message: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


class AnalysisDetailOut(AnalysisOut):
    transcript: Optional[str] = None
    report_html: Optional[str] = None


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── Manual trigger ────────────────────────────────────────────────────────────

class ManualTriggerRequest(BaseModel):
    appointment_id: str
    phone: str
    appointment_datetime: str
    campaign_code: str
    acuity_account: int = 1
