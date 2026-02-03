import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(".env")
db_url = os.environ["DATABASE_URL"]

engine = create_engine(db_url)

DDL = """
ALTER TABLE deals
ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;

UPDATE deals
SET ingested_at = COALESCE(ingested_at, NOW());

CREATE OR REPLACE FUNCTION set_ingested_at()
RETURNS trigger AS $$
BEGIN
  NEW.ingested_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_ingested_at ON deals;

CREATE TRIGGER trg_set_ingested_at
BEFORE INSERT OR UPDATE ON deals
FOR EACH ROW
EXECUTE FUNCTION set_ingested_at();
"""

with engine.begin() as conn:
    conn.execute(text(DDL))

print("OK: ingested_at added and trigger installed.")
