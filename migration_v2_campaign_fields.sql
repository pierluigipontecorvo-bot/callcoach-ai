-- ─────────────────────────────────────────────────────────────────────────────
-- Migration v2: campaign prefix-matching support
-- Run this once in Supabase SQL Editor
-- ─────────────────────────────────────────────────────────────────────────────

-- 1. Add human-readable name for the config entry (e.g. "Mailbox – tutte")
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS nome TEXT;

-- 2. Add optional internal notes
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS notes TEXT;

-- 3. Ensure updated_at is present (was already in schema, but just in case)
ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();

-- 4. Auto-update updated_at on every row change
CREATE OR REPLACE FUNCTION _set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS campaigns_set_updated_at ON campaigns;
CREATE TRIGGER campaigns_set_updated_at
    BEFORE UPDATE ON campaigns
    FOR EACH ROW EXECUTE FUNCTION _set_updated_at();

-- Done. No data migration needed — existing rows continue to work
-- because the prefix lookup falls back to exact match automatically.
