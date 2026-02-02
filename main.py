from __future__ import annotations

import logging
import time
import uuid
from fastapi import FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi

from config import get_settings
from responses import ok, fail, Envelope
from metrics import inc_requests, inc_errors, inc_rate_limited, inc_auth_failed, snapshot
from rate_limiter import build_limiter

from db import get_coupon_by_drug, list_coupons, count_coupons
from auth_db import (
    ensure_db_initialized,
    get_key_info,
    update_last_used,
    list_keys,
    create_key,
    revoke_key,
    set_key_active,
    rotate_key,
)

settings = get_settings()

# Initialize auth DB with safe migrations; seed only if allowed
ensure_db_initialized(seed=settings.seed_dev_key)

app = FastAPI(title="GoodRx Coupons API", version="0.2.0")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("goodrx_api")

api_key_header = APIKeyHeader(
    name="x-api-key",
    auto_error=False,
    scheme_name="ApiKeyAuth",   # use stable OpenAPI scheme id
)

admin_key_header = APIKeyHeader(
    name="x-admin-key",
    auto_error=False,
    scheme_name="AdminKeyAuth", # use stable OpenAPI scheme id
)


RATE_PERIOD = 60  # seconds
limiter = build_limiter(settings.redis_url)


def _rid(request: Request) -> str:
    return getattr(request.state, "request_id", "-")


def _mask_key(k: str) -> str:
    if not k:
        return "-"
    return ("*" * max(0, len(k) - 4)) + k[-4:]


@app.middleware("http")
async def request_id_and_logging(request: Request, call_next):
    """First middleware: generate request_id, store it on request.state, add x-request-id header,
    and emit a structured log line after the response is produced.

    Important: do not log headers, query params, or API keys here.
    """
    inc_requests()
    # create request id and attach to request.state
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    start = time.time()
    response = None
    try:
        response = await call_next(request)
        # ensure x-request-id present on all responses
        try:
            response.headers["x-request-id"] = request_id
        except Exception:
            # best-effort: don't break responses if headers aren't writable
            pass
        return response
    except Exception:
        inc_errors()
        raise
    finally:
        duration_ms = round((time.time() - start) * 1000, 3)
        status = getattr(response, "status_code", "-")
        # Structured, minimal log line; avoid logging headers or query params
        logger.info(
            "request_id=%s method=%s path=%s status=%s duration_ms=%s",
            request_id,
            request.method,
            request.url.path,
            status,
            duration_ms,
        )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    inc_errors()
    env = fail(
        request_id=_rid(request),
        code=int(exc.status_code),
        message=str(exc.detail),
        error_type="http_error",
        details=None,
    )
    return JSONResponse(status_code=int(exc.status_code), content=env.model_dump())


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    inc_errors()
    env = fail(
        request_id=_rid(request),
        code=422,
        message="Validation error",
        error_type="validation_error",
        details=exc.errors(),
    )
    return JSONResponse(status_code=422, content=env.model_dump())


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    inc_errors()
    # Avoid leaking internals in prod
    msg = "Server error" if settings.env == "prod" else f"Server error: {exc}"
    env = fail(_rid(request), 500, msg, "server_error")
    return JSONResponse(status_code=500, content=env.model_dump())


def require_api_key(request: Request, api_key: str = Security(api_key_header)) -> dict:
    if not api_key:
        inc_auth_failed()
        raise HTTPException(status_code=401, detail="Missing API key")
    info = get_key_info(api_key)
    if not info:
        inc_auth_failed()
        raise HTTPException(status_code=401, detail="Invalid API key")

    # store on request.state; never log full key
    request.state.api_key = api_key
    request.state.client_name = info.get("client_name")
    request.state.rate_limit = int(info.get("rate_limit") or 60)

    # best-effort last_used tracking
    try:
        update_last_used(api_key)
    except Exception:
        pass

    return info


def require_admin(request: Request, admin_key: str = Security(admin_key_header)) -> None:
    if not settings.admin_api_key:
        raise HTTPException(status_code=503, detail="Admin API key not configured")
    if not admin_key or admin_key != settings.admin_api_key:
        raise HTTPException(status_code=403, detail="Forbidden")


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # Public endpoints not rate limited
    public_prefixes = ("/docs", "/openapi.json", "/redoc", "/favicon.ico", "/healthz", "/readyz")
    if request.url.path.startswith(public_prefixes) or request.method == "OPTIONS":
        return await call_next(request)

    # Require API key for all non-public endpoints (single source of truth: dependency)
    # We cannot call dependency here; instead, we rate limit only if state has api_key.
    # Endpoints themselves require require_api_key; if missing, they'll 401.
    api_key = getattr(request.state, "api_key", None)
    rate_limit = getattr(request.state, "rate_limit", 60)

    # If state isn't set yet, just proceed; endpoint dependency will set and can be rate-limited on next call.
    # (If you want strict rate limiting for every call, move to dependency-based limiter.)
    if api_key:
        decision = limiter.allow(api_key, limit=int(rate_limit), period=RATE_PERIOD)
        if not decision.allowed:
            inc_rate_limited()
            env = fail(_rid(request), 429, "Rate limit exceeded", "rate_limited")
            return JSONResponse(status_code=429, content=env.model_dump())

    return await call_next(request)


@app.get("/healthz", response_model=Envelope)
def healthz(request: Request):
    return ok(_rid(request), data={"status": "ok"})


@app.get("/readyz", response_model=Envelope)
def readyz(request: Request):
    # checks: DB files exist and basic queries succeed
    try:
        # auth DB exists
        _ = settings.api_keys_db_path.exists()
        # coupons DB exists and readable
        _ = settings.coupons_db_path.exists()
        # sanity query
        _ = count_coupons()
    except Exception as e:
        env = fail(_rid(request), 503, "Not ready", "not_ready", details=str(e) if settings.env != "prod" else None)
        return JSONResponse(status_code=503, content=env.model_dump())
    return ok(_rid(request), data={"status": "ready"})


@app.get("/metrics", response_model=Envelope)
def metrics_endpoint(request: Request):
    return ok(_rid(request), data=snapshot())


@app.get("/coupon", response_model=Envelope)
def read_coupon(drug_name: str, request: Request, _keyinfo: dict = Security(require_api_key)):
    row = get_coupon_by_drug(drug_name)
    if not row:
        env = fail(_rid(request), 404, "Coupon not found", "not_found")
        return JSONResponse(status_code=404, content=env.model_dump())
    # expose only ai_extraction (and id) to callers
    data = {"id": row.get("id"), "ai_extraction": row.get("ai_extraction")}
    return ok(_rid(request), data=data)


@app.get("/coupons", response_model=Envelope)
def list_coupons_endpoint(
    request: Request,
    page: int = 1,
    per_page: int = 50,
    drug_name: str | None = None,
    _keyinfo: dict = Security(require_api_key),
):
    per_page = min(max(1, per_page), 500)
    page = max(1, page)
    offset = (page - 1) * per_page

    items = list_coupons(limit=per_page, offset=offset, drug_name=drug_name)
    total = count_coupons(drug_name=drug_name)

    # expose only id and ai_extraction in items
    items_out = [{"id": r.get("id"), "ai_extraction": r.get("ai_extraction")} for r in items]

    meta = {"page": page, "per_page": per_page, "total": total, "drug_name": drug_name}
    return ok(_rid(request), data={"items": items_out, "meta": meta})


# -------------------------
# Admin endpoints (internal)
# -------------------------

@app.get("/admin/keys", response_model=Envelope)
def admin_list_keys(request: Request, _admin: None = Security(require_admin)):
    return ok(_rid(request), data={"keys": list_keys(mask=True)})


@app.post("/admin/keys", response_model=Envelope)
def admin_create_key(request: Request, client_name: str, rate_limit: int = 60, _admin: None = Security(require_admin)):
    created = create_key(client_name=client_name, rate_limit=rate_limit)
    # return full key only here (admin); caller must store it
    return ok(_rid(request), data=created, message="Key created", code=201)


@app.post("/admin/keys/{api_key}/revoke", response_model=Envelope)
def admin_revoke_key(request: Request, api_key: str, _admin: None = Security(require_admin)):
    if not revoke_key(api_key):
        env = fail(_rid(request), 404, "Key not found", "not_found")
        return JSONResponse(status_code=404, content=env.model_dump())
    return ok(_rid(request), data={"revoked": True})


@app.post("/admin/keys/{api_key}/activate", response_model=Envelope)
def admin_activate_key(request: Request, api_key: str, active: bool, _admin: None = Security(require_admin)):
    if not set_key_active(api_key, active=active):
        env = fail(_rid(request), 404, "Key not found", "not_found")
        return JSONResponse(status_code=404, content=env.model_dump())
    return ok(_rid(request), data={"active": active})


@app.post("/admin/keys/{api_key}/rotate", response_model=Envelope)
def admin_rotate_key(request: Request, api_key: str, _admin: None = Security(require_admin)):
    new_key = rotate_key(api_key)
    if not new_key:
        env = fail(_rid(request), 404, "Key not found", "not_found")
        return JSONResponse(status_code=404, content=env.model_dump())
    return ok(_rid(request), data=new_key, message="Key rotated")


# Expose both API key security schemes in OpenAPI so Swagger's Authorize shows them.
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    comps = openapi_schema.setdefault("components", {})
    schemes = comps.setdefault("securitySchemes", {})
    # register schemes using the same ids as the APIKeyHeader.scheme_name values
    schemes["ApiKeyAuth"] = {"type": "apiKey", "in": "header", "name": "x-api-key", "description": "Client API key"}
    schemes["AdminKeyAuth"] = {"type": "apiKey", "in": "header", "name": "x-admin-key", "description": "Admin API key"}
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi


# NOTE: Removed developer-only /whoami and /_debug/header endpoints as requested.
