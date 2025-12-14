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

def _iso_from_payload_timestamp(value: Optional[str]) -> Optional[str]:
    """
    Convert an incoming timestamp to an ISO 8601 UTC 'Z' string if possible.
    Accepts common webhook formats like '2025-12-14T09:44:18.136Z'.
    """
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        # Handle trailing Z
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


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
    # Note: Instantly payloads vary by event type; we accept common aliases but remain
    # strict about ONLY updating Airtable when we have a real lead_id.
    lead_id = payload.get("lead_id") or payload.get("leadId")
    reply_text = payload.get("reply_text") or payload.get("replyText") or payload.get("reply") or payload.get("message")
    lead_status = (
        payload.get("lead_status")
        or payload.get("leadStatus")
        or payload.get("event_type")
        or payload.get("status")
    )
    campaign_name = payload.get("campaign_name") or payload.get("campaignName")
    event_timestamp = payload.get("timestamp") or payload.get("event_timestamp")

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
        "event_timestamp": str(event_timestamp) if event_timestamp is not None else None,
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
        # Instantly may send events (e.g. lead_interested) without a lead_id.
        # We must remain strict about only updating by instantly_lead_id, so we skip.
        return JSONResponse(
            status_code=200,
            content={
                "message": "Skipped: Missing lead_id in webhook payload (strict ID-only mode)",
            },
        )

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
    event_ts_iso = _iso_from_payload_timestamp(parsed.get("event_timestamp")) or _iso_utc_now_z()

    update_fields: Dict[str, Any] = {
        STATUS_COL: _coalesce_nonempty(lead_status, fields.get(STATUS_COL)),
        DATE_COL: event_ts_iso,
        CAMP_COL: _coalesce_nonempty(campaign_name, fields.get(CAMP_COL)),
        PROV_COL: _coalesce_nonempty(email_provider, fields.get(PROV_COL)),
        GATE_COL: _coalesce_nonempty(email_security_gateway, fields.get(GATE_COL)),
    }

    # Append communication log
    existing_log = fields.get(LOG_COL) or ""
    log_entry = f"[{_log_date_yyyy_mm_dd()}] Status: {lead_status or ''} | Reply: {reply_text}"
    new_log = (existing_log + "\n\n" + log_entry).strip() if existing_log else log_entry
    update_fields[LOG_COL] = new_log

    try:
        table.update(record_id, update_fields)
    except Exception as e:
        # Return a clear error so the failure is diagnosable in Vercel logs.
        raise HTTPException(status_code=500, detail=f"Airtable update failed: {e}")

    return JSONResponse(status_code=200, content={"message": "Success: Record enriched and updated"})


