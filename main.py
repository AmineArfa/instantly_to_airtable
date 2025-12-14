import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pyairtable import Table


# Logging
LOG_LEVEL = (os.getenv("LOG_LEVEL") or "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("instantly_to_airtable")


# Airtable configuration
# - Keep API key in env (secret)
# - Base/Table can be hardcoded (non-secret identifiers)
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "").strip()

# From your provided Airtable link:
# Base: appoaTzHT7BYJcXOb
# Table: tblHBBta5IyXclFln
BASE_ID = "appoaTzHT7BYJcXOb"
TABLE_NAME = "tblHBBta5IyXclFln"

# Instantly configuration
# Keep Instantly key in env (secret)
INSTANTLY_API_KEY = os.getenv("INSTANTLY_API_KEY", "").strip()
INSTANTLY_API_BASE = (os.getenv("INSTANTLY_API_BASE") or "https://api.instantly.ai").strip().rstrip("/")
WEBHOOK_DEBUG = (os.getenv("WEBHOOK_DEBUG") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

# Airtable field mapping (Exact Columns)
ID_COL = "instantly_lead_id"  # Search key
EMAIL_COL = "key_contact_email"  # Fallback search key when lead_id is missing
STATUS_COL = "status"
LOG_COL = "communication_log"
DATE_COL = "last_interaction_date"
CAMP_COL = "last_campaign_name"
PROV_COL = "email_provider"
GATE_COL = "email_security_gateway"


app = FastAPI(title="Instantly.ai → Airtable Webhook Bridge")

# Cache for ESP code -> label mapping (best-effort)
_ESP_MAP_CACHE: Dict[str, str] = {}
_ESP_MAP_CACHE_TS: Optional[datetime] = None
_ESP_MAP_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours


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


def _is_blank(value: Optional[Any]) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _instantly_headers() -> Dict[str, str]:
    # Instantly docs indicate Bearer-token style auth.
    # See `https://developer.instantly.ai/` (API V2 migration mentions Bearer token auth).
    return {"Authorization": f"Bearer {INSTANTLY_API_KEY}"}


async def _instantly_fetch_lead(lead_id: Optional[str], lead_email: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Fetch a lead from Instantly.
    - Prefer GET /api/v2/lead/{id}
    - Else POST /api/v2/lead/list with search=email (returns items[])
    """
    if not INSTANTLY_API_KEY:
        return None

    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        if lead_id:
            r = await client.get(
                f"{INSTANTLY_API_BASE}/api/v2/lead/{lead_id}",
                headers=_instantly_headers(),
            )
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, dict) else None

        if lead_email:
            body = {"search": lead_email, "limit": 1}
            r = await client.post(
                f"{INSTANTLY_API_BASE}/api/v2/lead/list",
                headers=_instantly_headers(),
                json=body,
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict):
                items = data.get("items")
                if isinstance(items, list) and items and isinstance(items[0], dict):
                    return items[0]
            return None

    return None


def _extract_esp_esg_codes(lead_obj: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    esp = lead_obj.get("esp_code")
    esg = lead_obj.get("esg_code")
    esp_code = str(esp).strip() if esp is not None else None
    esg_code = str(esg).strip() if esg is not None else None
    return (esp_code or None, esg_code or None)


async def _instantly_get_esp_map() -> Dict[str, str]:
    """
    Best-effort mapping of esp_code -> provider name.
    Uses GET /api/v2/inbox-placement-test/email-service-provider-option when available.
    """
    global _ESP_MAP_CACHE_TS, _ESP_MAP_CACHE

    now = datetime.now(timezone.utc)
    if _ESP_MAP_CACHE_TS and (now - _ESP_MAP_CACHE_TS).total_seconds() < _ESP_MAP_CACHE_TTL_SECONDS:
        return _ESP_MAP_CACHE

    if not INSTANTLY_API_KEY:
        return {}

    timeout = httpx.Timeout(10.0, connect=5.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(
                f"{INSTANTLY_API_BASE}/api/v2/inbox-placement-test/email-service-provider-option",
                headers=_instantly_headers(),
            )
            r.raise_for_status()
            data = r.json()

        mapping: Dict[str, str] = {}

        # Common shapes (best-effort):
        # - list[{"code": "...", "name"/"label": "..."}]
        # - {"esp": [...]} or {"items": [...]}
        candidates = None
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            if isinstance(data.get("esp"), list):
                candidates = data["esp"]
            elif isinstance(data.get("items"), list):
                candidates = data["items"]

        if isinstance(candidates, list):
            for item in candidates:
                if not isinstance(item, dict):
                    continue
                code = item.get("code") or item.get("value") or item.get("id")
                name = item.get("name") or item.get("label") or item.get("title")
                if code is None or name is None:
                    continue
                code_s = str(code).strip()
                name_s = str(name).strip()
                if code_s and name_s:
                    mapping[code_s] = name_s

        _ESP_MAP_CACHE = mapping
        _ESP_MAP_CACHE_TS = now
        return mapping
    except Exception:
        # If anything goes wrong, just don't map (we'll write esp_code as-is).
        return {}


def _safe_keys(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a small, safe-to-log subset of payload info.
    Never log secrets; only log key presence + a few identifiers.
    """
    return {
        "keys": sorted(list(payload.keys()))[:60],
        "event_type": payload.get("event_type") or payload.get("type"),
        "lead_id_present": bool(payload.get("lead_id") or payload.get("leadId")),
        "lead_email_present": bool(payload.get("lead_email") or payload.get("email") or payload.get("leadEmail")),
        "campaign_name": payload.get("campaign_name") or payload.get("campaignName"),
    }


def _parse_payload(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    # Standard fields (flat payload)
    # Note: Instantly payloads vary by event type; we accept common aliases but remain
    # strict about ONLY updating Airtable when we have a real lead_id.
    lead_id = payload.get("lead_id") or payload.get("leadId")
    lead_email = payload.get("lead_email") or payload.get("email") or payload.get("leadEmail")
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
        "lead_email": str(lead_email).strip() if lead_email is not None else None,
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
    trace_id = uuid.uuid4().hex
    try:
        payload = await request.json()
    except Exception as e:
        logger.warning("trace_id=%s invalid_json error=%s", trace_id, str(e))
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")

    if not isinstance(payload, dict):
        logger.warning("trace_id=%s payload_not_object type=%s", trace_id, type(payload).__name__)
        raise HTTPException(status_code=400, detail="Payload must be a JSON object")

    logger.info("trace_id=%s webhook_received %s", trace_id, _safe_keys(payload))

    parsed = _parse_payload(payload)
    lead_id = parsed["lead_id"]
    lead_email = parsed.get("lead_email")
    if not lead_id and not lead_email:
        # Instantly may send events without a lead_id; for those, we fall back to email if provided.
        logger.info("trace_id=%s skipped reason=missing_lead_id_and_email", trace_id)
        return JSONResponse(
            status_code=200,
            content={"message": "Skipped: Missing lead_id and lead_email in webhook payload", "trace_id": trace_id},
        )

    table = _airtable_table()

    # Step 1: Strict search
    # - Prefer instantly_lead_id when present
    # - Fall back to key_contact_email when lead_id is missing
    if lead_id:
        formula = f'{{{ID_COL}}} = "{_escape_airtable_string(lead_id)}"'
        search_mode = "instantly_lead_id"
    else:
        formula = f'{{{EMAIL_COL}}} = "{_escape_airtable_string(lead_email or "")}"'
        search_mode = "key_contact_email"

    logger.info(
        "trace_id=%s airtable_search mode=%s lead_id=%s lead_email=%s",
        trace_id,
        search_mode,
        (lead_id[:12] + "...") if lead_id else None,
        lead_email,
    )

    matches = table.all(
        formula=formula,
        fields=[ID_COL, EMAIL_COL, STATUS_COL, LOG_COL, CAMP_COL, PROV_COL, GATE_COL],
    )

    if not matches:
        logger.info("trace_id=%s airtable_not_found mode=%s", trace_id, search_mode)
        return JSONResponse(
            status_code=200,
            content={"message": f"Skipped: Record not found in Airtable by {search_mode}", "trace_id": trace_id},
        )

    record = matches[0]
    record_id = record["id"]
    fields: Dict[str, Any] = record.get("fields", {}) or {}

    logger.info(
        "trace_id=%s airtable_found record_id=%s existing_provider_blank=%s existing_gateway_blank=%s",
        trace_id,
        record_id,
        _is_blank(fields.get(PROV_COL)),
        _is_blank(fields.get(GATE_COL)),
    )

    # Prepare update fields (always send all mapped fields; don't wipe existing on missing webhook keys)
    lead_status = parsed["lead_status"]
    campaign_name = parsed["campaign_name"]
    email_provider = parsed["email_provider"]
    email_security_gateway = parsed["email_security_gateway"]
    reply_text = parsed["reply_text"] or ""
    event_ts_iso = _iso_from_payload_timestamp(parsed.get("event_timestamp")) or _iso_utc_now_z()

    # If Airtable provider/gateway are empty AND webhook didn't provide them,
    # try fetching from Instantly after the webhook is called.
    provider_missing = _is_blank(fields.get(PROV_COL))
    gateway_missing = _is_blank(fields.get(GATE_COL))
    need_provider = provider_missing and _is_blank(email_provider)
    need_gateway = gateway_missing and _is_blank(email_security_gateway)

    logger.info(
        "trace_id=%s enrich_decision need_provider=%s need_gateway=%s webhook_provider_present=%s webhook_gateway_present=%s",
        trace_id,
        need_provider,
        need_gateway,
        not _is_blank(email_provider),
        not _is_blank(email_security_gateway),
    )

    if need_provider or need_gateway:
        try:
            logger.info("trace_id=%s instantly_fetch_start", trace_id)
            lead_obj = await _instantly_fetch_lead(lead_id=lead_id, lead_email=lead_email)
            if lead_obj:
                esp_code, esg_code = _extract_esp_esg_codes(lead_obj)
                logger.info(
                    "trace_id=%s instantly_fetch_ok esp_code=%s esg_code=%s",
                    trace_id,
                    esp_code,
                    esg_code,
                )

                if need_provider and esp_code:
                    esp_map = await _instantly_get_esp_map()
                    email_provider = esp_map.get(esp_code) or esp_code
                    logger.info("trace_id=%s provider_filled value=%s", trace_id, email_provider)

                if need_gateway and esg_code:
                    # No known public mapping endpoint for ESG in docs; store the code as-is.
                    email_security_gateway = esg_code
                    logger.info("trace_id=%s gateway_filled value=%s", trace_id, email_security_gateway)
            else:
                logger.info("trace_id=%s instantly_fetch_empty", trace_id)
        except Exception:
            # Never fail the webhook because Instantly enrichment is unavailable.
            logger.exception("trace_id=%s instantly_fetch_failed", trace_id)
            pass

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
        logger.info(
            "trace_id=%s airtable_update_start record_id=%s provider=%s gateway=%s",
            trace_id,
            record_id,
            update_fields.get(PROV_COL),
            update_fields.get(GATE_COL),
        )
        table.update(record_id, update_fields)
        logger.info("trace_id=%s airtable_update_ok record_id=%s", trace_id, record_id)
    except Exception as e:
        # Return a clear error so the failure is diagnosable in Vercel logs.
        logger.exception("trace_id=%s airtable_update_failed record_id=%s", trace_id, record_id)
        raise HTTPException(status_code=500, detail=f"Airtable update failed: {e}")

    response: Dict[str, Any] = {"message": "Success: Record enriched and updated", "trace_id": trace_id}
    if WEBHOOK_DEBUG:
        response["debug"] = {
            "search_mode": search_mode,
            "provider_was_blank": provider_missing,
            "gateway_was_blank": gateway_missing,
            "provider_filled_now": not _is_blank(update_fields.get(PROV_COL)),
            "gateway_filled_now": not _is_blank(update_fields.get(GATE_COL)),
            "instantly_enrichment_attempted": bool(need_provider or need_gateway),
        }
    return JSONResponse(status_code=200, content=response)


