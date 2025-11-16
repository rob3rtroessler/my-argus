# app.py
import os
from typing import Optional, Dict, Any, List

import httpx
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

# Databricks SQL connector (PEP 249)
from databricks import sql as dbsql

# Databricks workspace client (used for /api/me in local PAT mode)
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# -------------------------------------------------------------------
# LOCAL DEV ONLY: load .env (ignored in Databricks Apps runtime)
# -------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

# Optional: support numpy → JSON conversion in results
try:
    import numpy as np
except:
    np = None

# -------------------------------------------------------------------
# Create app, enable gzip, and serve static files for UI
# -------------------------------------------------------------------
app = FastAPI()

# Compress large responses to avoid blowing up browser memory
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Serve ./static/* at /static/*
app.mount("/static", StaticFiles(directory="static"), name="static")


# ===================================================================
# CORE AUTH + CONFIG HELPERS
# ===================================================================

def _host() -> str:
    """
    Returns the Databricks workspace URL **always including https://**.
    Works for both:
    - Local: from .env
    - Databricks App: env set through App config
    """
    h = (os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_WORKSPACE_URL") or "").strip().rstrip("/")
    if not h:
        raise RuntimeError("Missing workspace host. Set DATABRICKS_HOST.")
    if not h.startswith(("http://", "https://")):
        h = "https://" + h
    return h


def _http_path() -> str:
    """
    Returns SQL Warehouse HTTP Path.
    - Required in *both* local and app mode
    - Must be explicitly set via env or app config
    """
    p = (os.getenv("DATABRICKS_SQL_HTTP_PATH") or os.getenv("DATABRICKS_HTTP_PATH") or "").strip()
    if not p:
        raise RuntimeError("Set DATABRICKS_SQL_HTTP_PATH to your SQL Warehouse HTTP Path")
    return p


def _token(req: Request) -> Dict[str, Any]:
    """
    Returns **how to authenticate**:

    APP MODE:
        Databricks Apps will forward OAuth access token
        → Comes in header `X-Forwarded-Access-Token`
        → This respects user-level permissions (OBO)

    LOCAL MODE:
        Use developer PAT stored in `.env`: DATABRICKS_TOKEN
    """
    obo = req.headers.get("X-Forwarded-Access-Token")
    if obo:
        return {"mode": "app", "token": obo}
    return {"mode": "local", "token": os.getenv("DATABRICKS_TOKEN")}


async def _get_json(host: str, token: str, path: str) -> Optional[dict]:
    """
    Helper for calling REST APIs — used for `/api/me` when in OBO mode.
    """
    url = f"{host.rstrip('/')}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, headers=headers)
        try:
            return resp.json()
        except:
            return {"status": resp.status_code, "text": resp.text}


def to_jsonable(x):
    """
    Safely convert values from Databricks SQL driver to standard JSON types.
    Prevents browser crashes from binary / numpy / decimals.
    """
    # numpy scalars / arrays
    if np is not None and isinstance(x, np.generic):
        return x.item()
    if np is not None and isinstance(x, np.ndarray):
        return [to_jsonable(v) for v in x.tolist()]

    # simple JSON-safe values
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x

    # dict
    if isinstance(x, dict):
        return {str(k): to_jsonable(v) for k, v in x.items()}

    # lists/tuples
    if isinstance(x, (list, tuple)):
        return [to_jsonable(v) for v in x]

    # dates, decimals, times, etc
    import datetime, decimal
    if isinstance(x, decimal.Decimal):
        return float(x)
    if isinstance(x, (datetime.datetime, datetime.date, datetime.time)):
        return x.isoformat()

    # bytes → try utf8, else hex
    if isinstance(x, (bytes, bytearray, memoryview)):
        try:
            return bytes(x).decode("utf-8")
        except:
            return bytes(x).hex()

    # fallback: string representation
    return str(x)

#==================================================================
# DEBUG ROUTES
#==================================================================

# --- DEBUG: what env + headers does the app see? ---
@app.get("/api/debug/env")
async def debug_env(request: Request):
    # show only the 3 things the SQL connector actually needs
    host = os.getenv("DATABRICKS_HOST") or os.getenv("DATABRICKS_WORKSPACE_URL")
    http_path = os.getenv("DATABRICKS_SQL_HTTP_PATH") or os.getenv("DATABRICKS_HTTP_PATH")
    obo = request.headers.get("X-Forwarded-Access-Token")
    return {
        "host": host,
        "http_path": http_path,
        "obo_token_present": bool(obo),
        "obo_token_len": len(obo or ""),
        "x_forwarded_headers": {
            "user": request.headers.get("X-Forwarded-User"),
            "email": request.headers.get("X-Forwarded-Email"),
            "scopes_hint": request.headers.get("X-Forwarded-Scopes") or None,  # may be empty
        },
    }

# Wrap a function to always return detailed JSON on errors
def _json_500(payload: dict):
    # never throw HTML 500; always return JSON with context
    return JSONResponse(payload, status_code=500)


# ===================================================================
# ROUTES
# ===================================================================

@app.get("/")
async def index():
    """Serve the interactive frontend."""
    return FileResponse("static/index.html")


@app.get("/api/me")
async def me(req: Request):
    """
    Returns the **current user**, both in:
    - Local mode (via PAT and WorkspaceClient)
    - App mode (via OBO token forwarded from Databricks Apps)
    """
    t = _token(req)
    host = _host()

    # App mode: call REST with OBO token (user's identity)
    if t["mode"] == "app":
        me = await _get_json(host, t["token"], "/api/2.0/preview/scim/v2/Me") \
          or await _get_json(host, t["token"], "/api/2.0/preview/iam/current-user")
        return {"mode": "app", "current_user": me}

    # Local mode: workspace SDK with PAT
    if not t["token"]:
        raise HTTPException(401, "No local PAT set (DATABRICKS_TOKEN).")
    w = WorkspaceClient(config=Config(host=host, token=t["token"]))
    me = w.current_user.me()
    return {"mode": "local", "current_user": me.as_dict()}


@app.get("/api/sql/ping")
async def sql_ping(req: Request):
    """Tiny health check to verify SQL Warehouse connectivity."""
    t = _token(req)
    try:
        host, http_path = _host(), _http_path()

        import time
        t0 = time.perf_counter()
        with dbsql.connect(
            server_hostname=host.replace("https://","").replace("http://",""),
            http_path=http_path,
            access_token=t["token"],
        ) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            ok = (cur.fetchone()[0] == 1)
        t1 = time.perf_counter()

        # server-side log
        print(f"[DEBUG] /api/sql/ping ok={ok} query_ms={(t1 - t0)*1000:.1f} mode={t['mode']}")

        return {"mode": t["mode"], "ok": ok, "timing": {"query_ms": round((t1 - t0)*1000, 1)}}

    except Exception as e:
        # Return JSON with enough context to diagnose in the deployed app
        return JSONResponse({
            "mode": t.get("mode"),
            "error": str(e),
            "context": {
                "server_hostname": (os.getenv("DATABRICKS_HOST") or "").replace("https://","").replace("http://",""),
                "http_path": os.getenv("DATABRICKS_SQL_HTTP_PATH") or os.getenv("DATABRICKS_HTTP_PATH"),
                "has_token": bool(t.get("token")),
            },
        }, status_code=500)

# -------------------------------------------------------------------
# Emails API (paged — enables fetching **all** rows client-side)
# -------------------------------------------------------------------
MAX_ROWS = 200000
DEFAULT_LIMIT = 1000
MAX_BYTES = 100_000_000  # Prevent single-response browser meltdown

import time
from fastapi import Request, Query, HTTPException
from fastapi.responses import JSONResponse




@app.get("/api/emails")
async def get_emails(
    req: Request,
    subject: str = Query(default=""),        # case-insensitive LIKE on subject
    from_email: str = Query(default=""),     # case-insensitive LIKE on from_email
    is_read: Optional[bool] = Query(default=None),  # filter by read status
    is_starred: Optional[bool] = Query(default=None),  # filter by starred status
    limit: int = Query(default=100, le=1000),  # max 1000 rows per request
    offset: int = Query(default=0, ge=0),    # pagination offset
):
    """
    Returns emails from dev.core.emails table with optional filters.
    Supports pagination via limit/offset.
    """
    # --- resolve auth/config (local PAT or App OBO) ---
    t = _token(req)
    if not t["token"]:
        raise HTTPException(401, detail=f"Missing token ({t['mode']}).")

    try:
        host = _host()
        http_path = _http_path()

        # --- WHERE + params ---
        where_clauses: List[str] = []
        params: List[Any] = []

        if subject.strip():
            where_clauses.append("upper(subject) LIKE upper(?)")
            params.append(f"%{subject.strip()}%")

        if from_email.strip():
            where_clauses.append("upper(from_email) LIKE upper(?)")
            params.append(f"%{from_email.strip()}%")

        if is_read is not None:
            where_clauses.append("is_read = ?")
            params.append(is_read)

        if is_starred is not None:
            where_clauses.append("is_starred = ?")
            params.append(is_starred)

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        sql_text = f"""
            SELECT
                email_id,
                thread_id,
                subject,
                from_name,
                from_email,
                to_recipients,
                cc_recipients,
                sent_at,
                received_at,
                received_date,
                snippet,
                labels,
                is_read,
                is_starred,
                has_attachments,
                attachments,
                message_size_bytes,
                created_at
            FROM dev.core.emails
            {where_sql}
            ORDER BY received_at DESC
            LIMIT ? OFFSET ?
        """.strip()

        # Add limit and offset to params
        params.extend([limit, offset])

        # --- execute + timing ---
        import time
        t0 = time.perf_counter()
        with dbsql.connect(
            server_hostname=host.replace("https://", "").replace("http://", ""),
            http_path=http_path,
            access_token=t["token"],
        ) as conn, conn.cursor() as cur:
            cur.execute(sql_text, params)
            cols = [d[0] for d in cur.description]
            raw_rows = cur.fetchall()
        t1 = time.perf_counter()

        # --- serialize + timing ---
        t2 = time.perf_counter()
        rows = [{cols[i]: to_jsonable(v) for i, v in enumerate(r)} for r in raw_rows]
        t3 = time.perf_counter()

        print(
            "[DEBUG] /api/emails "
            f"rows={len(rows)} query_ms={(t1 - t0)*1000:.1f} json_ms={(t3 - t2)*1000:.1f} "
            f"mode={t['mode']} limit={limit} offset={offset}"
        )

        return JSONResponse({
            "mode": t["mode"],
            "rows": rows,
            "count": len(rows),
            "limit": limit,
            "offset": offset,
            "timing": {
                "query_ms": round((t1 - t0) * 1000, 1),
                "serialize_ms": round((t3 - t2) * 1000, 1),
                "total_ms": round((t3 - t0) * 1000, 1),
            },
            "sql": {"text": sql_text, "params": params},
        })

    except Exception as e:
        # rich JSON error with context so you see the *real* reason in-app
        return JSONResponse({
            "mode": t.get("mode"),
            "error": str(e),
            "sql": {"text": sql_text if 'sql_text' in locals() else None, "params": params if 'params' in locals() else None},
            "context": {
                "server_hostname": (os.getenv("DATABRICKS_HOST") or "").replace("https://","").replace("http://",""),
                "http_path": os.getenv("DATABRICKS_SQL_HTTP_PATH") or os.getenv("DATABRICKS_HTTP_PATH"),
                "has_token": bool(t.get("token")),
            },
        }, status_code=500)



# -------------------------------------------------------------------
# LOCAL DEV MODE — Databricks Apps will NOT execute this block
# -------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host=os.getenv("UVICORN_HOST", "127.0.0.1"),
        port=int(os.getenv("UVICORN_PORT", "8000")),
        reload=True,
    )
