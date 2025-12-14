import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pyairtable import Table


# Airtable configuration
# - Keep API key in env (secret)
# - Base/Table can be hardcoded (non-secret identifiers)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()

# From your provided Airtable link:
# Base: appoaTzHT7BYJcXOb
# Table: tblHBBta5IyXclFln
BASE_ID = "appoaTzHT7BYJcXOb"
TABLE_NAME = "tblHBBta5IyXclFln"

# Airtable field mapping (Exact Columns)
ID_COL = "instantly_lead_id"  # Search key
STATUS_COL = "status"
LOG_COL = "communication_log"
DATE_COL = "last_interaction_date"
CAMP_COL = "last_campaign_name"
PROV_COL = "email_provider"
GATE_COL = "email_security_gateway"


app = FastAPI(title="Instantly.ai → Airtable Webhook Bridge")


def _require_env() -> None:
    missing = []
    if not AIRTABLE_API_KEY:
        missing.append("AIRTABLE_API_KEY")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def _airtable_table() -> Table:
    _require_env()
    return Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)


def _iso_utc_now_z() -> str:
    # Airtable Date+Time fields accept ISO 8601 strings; 'Z' is commonly accepted.
    dt = datetime.now(timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _log_date_yyyy_mm_dd() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _escape_airtable_string(value: str) -> str:
    # Escape for Airtable formula string literal: double-quotes and backslashes.
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _coalesce_nonempty(new_value: Optional[Any], existing_value: Optional[Any]) -> Optional[Any]:
    # Prevent wiping Airtable fields on webhook events where keys are missing/null/empty.
    if new_value is None:
        return existing_value
    if isinstance(new_value, str) and new_value.strip() == "":
        return existing_value
    return new_value


def _parse_payload(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    # Standard fields (flat payload)
    lead_id = payload.get("lead_id")
    reply_text = payload.get("reply_text")
    lead_status = payload.get("lead_status")
    campaign_name = payload.get("campaign_name")

    # Enrichment fields (may be missing/null)
    email_provider = payload.get("email_provider")
    email_security_gateway = payload.get("email_security_gateway")

    return {
        "lead_id": str(lead_id).strip() if lead_id is not None else None,
        "reply_text": str(reply_text) if reply_text is not None else None,
        "lead_status": str(lead_status) if lead_status is not None else None,
        "campaign_name": str(campaign_name) if campaign_name is not None else None,
        "email_provider": str(email_provider) if email_provider is not None else None,
        "email_security_gateway": str(email_security_gateway) if email_security_gateway is not None else None,
    }


@app.get("/health")
def health() -> Dict[str, str]:
    # Do not force env validation here; useful for basic uptime checks.
    return {"status": "ok"}


@app.post("/webhook/instantly")
async def instantly_webhook(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    parsed = _parse_payload(payload)
    lead_id = parsed["lead_id"]
    if not lead_id:
        raise HTTPException(status_code=400, detail="Missing required field: lead_id")

    table = _airtable_table()

    # Step 1: Strict ID search
    formula = f'{{{ID_COL}}} = "{_escape_airtable_string(lead_id)}"'
    matches = table.all(formula=formula, fields=[ID_COL, STATUS_COL, LOG_COL, CAMP_COL, PROV_COL, GATE_COL])

    if not matches:
        return JSONResponse(status_code=200, content={"message": "Skipped: ID not found in Airtable"})

    record = matches[0]
    record_id = record["id"]
    fields: Dict[str, Any] = record.get("fields", {}) or {}

    # Prepare update fields (always send all mapped fields; don't wipe existing on missing webhook keys)
    lead_status = parsed["lead_status"]
    campaign_name = parsed["campaign_name"]
    email_provider = parsed["email_provider"]
    email_security_gateway = parsed["email_security_gateway"]
    reply_text = parsed["reply_text"] or ""

    update_fields: Dict[str, Any] = {
        STATUS_COL: _coalesce_nonempty(lead_status, fields.get(STATUS_COL)),
        DATE_COL: _iso_utc_now_z(),
        CAMP_COL: _coalesce_nonempty(campaign_name, fields.get(CAMP_COL)),
        PROV_COL: _coalesce_nonempty(email_provider, fields.get(PROV_COL)),
        GATE_COL: _coalesce_nonempty(email_security_gateway, fields.get(GATE_COL)),
    }

    # Append communication log
    existing_log = fields.get(LOG_COL) or ""
    log_entry = f"[{_log_date_yyyy_mm_dd()}] Status: {lead_status or ''} | Reply: {reply_text}"
    new_log = (existing_log + "\n\n" + log_entry).strip() if existing_log else log_entry
    update_fields[LOG_COL] = new_log

    table.update(record_id, update_fields)

    return JSONResponse(status_code=200, content={"message": "Success: Record enriched and updated"})


