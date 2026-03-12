-- CallCoach AI — Supabase schema
-- Run this in Supabase → SQL Editor

-- ── Campaigns ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS campaigns (
    id               SERIAL PRIMARY KEY,
    code             VARCHAR(100) UNIQUE NOT NULL,       -- e.g. "INTER-J&A-0000-0091-STEFANO-(SEGRATE)"
    type             VARCHAR(20)  NOT NULL,              -- e.g. "INTER", "AVANZ", "REFER"
    client_name      VARCHAR(200),                       -- e.g. "J&A"
    agent_name       VARCHAR(100),                       -- e.g. "STEFANO"
    province         VARCHAR(50),                        -- e.g. "SEGRATE"
    numeric_code     VARCHAR(20),                        -- e.g. "0000-0091" or "3314"
    is_multisede     BOOLEAN      DEFAULT FALSE,         -- TRUE for 8-digit codes (XXXX-XXXX)
    script           TEXT,                               -- sales/appointment script
    qualification_params TEXT,                           -- AI qualification parameters
    client_info      TEXT,                               -- client background info for AI
    email_recipients TEXT[],                             -- report recipients
    active           BOOLEAN      DEFAULT TRUE,
    created_at       TIMESTAMPTZ  DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  DEFAULT NOW()
);

-- ── Operators ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS operators (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(200),
    active     BOOLEAN     DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ── Analyses ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analyses (
    id                   SERIAL PRIMARY KEY,
    campaign_code        VARCHAR(100),
    appointment_id       VARCHAR(100),
    appointment_datetime TIMESTAMPTZ,
    client_phone         VARCHAR(50),
    operator_name        VARCHAR(100),
    acuity_account       INTEGER,                        -- 1 or 2
    acuity_label         VARCHAR(100),                   -- e.g. "PRESO"
    sidial_call_id       VARCHAR(100),
    transcript           TEXT,
    qualification_level  VARCHAR(50),                    -- eccellente/corretta/da_migliorare/insufficiente
    report_json          JSONB,
    report_html          TEXT,
    email_sent           BOOLEAN     DEFAULT FALSE,
    email_sent_at        TIMESTAMPTZ,
    processing_status    VARCHAR(50) DEFAULT 'pending',  -- pending/processing/completed/error
    error_message        TEXT,
    created_at           TIMESTAMPTZ DEFAULT NOW()
);

-- ── Documents ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS documents (
    id                SERIAL PRIMARY KEY,
    campaign_code     VARCHAR(100),                      -- NULL = global document
    filename          VARCHAR(300) NOT NULL,
    file_type         VARCHAR(20),                       -- pdf/png/txt
    content_extracted TEXT,
    storage_path      TEXT,
    active            BOOLEAN     DEFAULT TRUE,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_analyses_campaign  ON analyses(campaign_code);
CREATE INDEX IF NOT EXISTS idx_analyses_status    ON analyses(processing_status);
CREATE INDEX IF NOT EXISTS idx_analyses_datetime  ON analyses(appointment_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_campaigns_code     ON campaigns(code);
