from fastapi import FastAPI, HTTPException, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError, ConfigDict
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from typing import Optional, List, Dict, Any
import uvicorn
import logging
import os
import json
import copy
import asyncio
import traceback
import httpx
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SN71 Backend API")

# Add CORS middleware to allow frontend to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    body = await request.body()
    logger.error(f"Validation error on {request.method} {request.url}: {exc.errors()}")
    logger.error(f"Request body: {body.decode('utf-8') if body else 'empty'}")
    logger.error(f"Request headers: {dict(request.headers)}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "body": body.decode('utf-8') if body else 'empty'}
    )

# Database connection pool
DB_POOL: AsyncConnectionPool = None

# Buffer high-frequency worker heartbeats in memory and flush to DB in batches.
WORKER_STATUS_BUFFER: Dict[tuple[int, str], Dict[str, Any]] = {}
WORKER_STATUS_BUFFER_LOCK = asyncio.Lock()
WORKER_STATUS_FLUSH_TASK: Optional[asyncio.Task] = None
WORKER_STATUS_FLUSH_INTERVAL_SEC = float(os.getenv("WORKER_STATUS_FLUSH_INTERVAL_SEC", "2"))
WORKER_STATUS_MAX_BUFFER = int(os.getenv("WORKER_STATUS_MAX_BUFFER", "20000"))


async def _flush_worker_status_buffer_once() -> int:
    """Flush buffered worker-status updates to DB with one batched upsert."""
    pending_items: List[tuple[int, str, str, datetime]] = []
    async with WORKER_STATUS_BUFFER_LOCK:
        if not WORKER_STATUS_BUFFER:
            return 0
        for (worker_id, ip), payload in WORKER_STATUS_BUFFER.items():
            pending_items.append((worker_id, ip, payload["status"], payload["updated_at"]))
        WORKER_STATUS_BUFFER.clear()

    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    """
                    INSERT INTO sn71_apollo_worker_status (worker_id, ip, status, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (worker_id, ip) DO UPDATE
                        SET status = EXCLUDED.status,
                            updated_at = EXCLUDED.updated_at
                    """,
                    pending_items,
                )
                await conn.commit()
        return len(pending_items)
    except Exception as e:
        logger.error(f"Error flushing worker status buffer: {e}")
        # Re-queue updates so we don't lose heartbeats if a flush attempt fails.
        async with WORKER_STATUS_BUFFER_LOCK:
            for worker_id, ip, status, updated_at in pending_items:
                key = (worker_id, ip)
                if key not in WORKER_STATUS_BUFFER and len(WORKER_STATUS_BUFFER) >= WORKER_STATUS_MAX_BUFFER:
                    WORKER_STATUS_BUFFER.pop(next(iter(WORKER_STATUS_BUFFER)))
                WORKER_STATUS_BUFFER[key] = {"status": status, "updated_at": updated_at}
        return 0


async def _worker_status_flush_loop():
    while True:
        try:
            await asyncio.sleep(WORKER_STATUS_FLUSH_INTERVAL_SEC)
            await _flush_worker_status_buffer_once()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Unhandled error in worker status flush loop: {e}")


async def _ensure_apollo_worker_status_table(conn):
    async with conn.cursor() as cur:
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS sn71_apollo_worker_status (
                worker_id   INTEGER      NOT NULL,
                ip          TEXT         NOT NULL,
                status      TEXT         NOT NULL,
                updated_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
                PRIMARY KEY (worker_id, ip)
            )
        """)


async def _ensure_apollo_company_columns(conn):
    async with conn.cursor() as cur:
        await cur.execute("""
            ALTER TABLE sn71_company
            ADD COLUMN IF NOT EXISTS created_time TIMESTAMP NULL
        """)
        await cur.execute("""
            ALTER TABLE sn71_company
            ADD COLUMN IF NOT EXISTS modified_time TIMESTAMP NULL
        """)
        await cur.execute("""
            ALTER TABLE sn71_company
            ADD COLUMN IF NOT EXISTS real_time_id INTEGER NULL
        """)


async def _ensure_apollo_realtime_columns(conn):
    async with conn.cursor() as cur:
        await cur.execute("""
            ALTER TABLE sn71_company_nonus
            ADD COLUMN IF NOT EXISTS real_time_id INTEGER NULL
        """)
        await cur.execute("""
            ALTER TABLE sn71_company_apollo_searchurl
            ADD COLUMN IF NOT EXISTS real_time INTEGER DEFAULT 0
        """)


async def _reset_stuck_realtime_at_startup(conn):
    """Reset any REALTIME records left in 'scrapping' state from previous crashes."""
    try:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE sn71_company_apollo_searchurl
                SET search_condition = NULL
                WHERE search_condition = 'scrapping'
                  AND real_time = 1
            """)
            reset_count = cur.rowcount
            if reset_count > 0:
                await conn.commit()
                logger.info(f"Reset {reset_count} stuck REALTIME record(s) at startup")
            return reset_count
    except Exception as e:
        logger.error(f"Error resetting stuck REALTIME records at startup: {e}")
        return 0


@app.on_event("startup")
async def startup_event():
    global DB_POOL, WORKER_STATUS_FLUSH_TASK
    db_url = os.getenv(
        "DATABASE_URL",
        "dbname=mydb user=myuser password=wonvhse1923741indiw83hfbixe92fnsbex9 host=localhost port=5432"
    )
    DB_POOL = AsyncConnectionPool(
        conninfo=db_url,
        min_size=2,
        max_size=20,
        max_idle=300,        # recycle idle connections after 5 min
        max_lifetime=3600,   # recycle every connection after 1 hour
        timeout=30,          # wait up to 30s for a free connection
        kwargs={
            "row_factory": dict_row,
            # Server-side safety: any transaction left idle for 5 min is aborted,
            # so a stuck client can never block the whole table again.
            "options": "-c idle_in_transaction_session_timeout=300000 "
                       "-c statement_timeout=120000",
        },
        open=False,
    )
    await DB_POOL.open(wait=True, timeout=30)

    try:
        async with DB_POOL.connection() as conn:
            # await _ensure_session_pay_date_column(conn)
            logger.info("Ensured sn71_session.pay_date column exists")
            # await _ensure_openrouter_keys_table(conn)
            logger.info("Ensured sn71_openrouter_key table exists")
            await _ensure_apollo_company_columns(conn)
            logger.info("Ensured sn71_company apollo columns (created_time, modified_time, real_time_id)")
            await _ensure_apollo_realtime_columns(conn)
            logger.info("Ensured sn71_company_nonus and sn71_company_apollo_searchurl realtime columns")
            await _reset_stuck_realtime_at_startup(conn)
            logger.info("Reset any stuck REALTIME records from previous crashes")
            # await _ensure_apollo_worker_status_table(conn)
            logger.info("Ensured sn71_apollo_worker_status table exists")
    except Exception as e:
        logger.error(f"Error running startup database bootstrap tasks: {e}")
        raise

    logger.info("Database connection pool initialized")

    WORKER_STATUS_FLUSH_TASK = asyncio.create_task(_worker_status_flush_loop())
    logger.info("Worker status flush loop started")

    # Start the lead rejection-reason scheduler (runs every 12 hours)
    # asyncio.create_task(_rejection_reason_scheduler())
    # logger.info("Lead rejection reason scheduler started (runs every 12 hours)")


@app.on_event("shutdown")
async def shutdown_event():
    global DB_POOL, WORKER_STATUS_FLUSH_TASK
    if WORKER_STATUS_FLUSH_TASK:
        WORKER_STATUS_FLUSH_TASK.cancel()
        try:
            await WORKER_STATUS_FLUSH_TASK
        except asyncio.CancelledError:
            pass
        WORKER_STATUS_FLUSH_TASK = None
    await _flush_worker_status_buffer_once()
    if DB_POOL:
        await DB_POOL.close()
        logger.info("Database connection pool closed")

# ==================== Dashboard Counts API ====================

@app.get("/api/counts/raw-company")
async def get_raw_company_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(id) as count
                    FROM sn71_company
                    WHERE
                        m_description IS NULL
                        AND company_check IS NULL
                        AND contact_info IS NULL
                        AND flag3 IS NULL
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting raw company count: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/counts/scored-company")
async def get_scored_company_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(id) as count
                    FROM sn71_company
                    WHERE
                        m_description IS NULL
                        AND company_check IS NULL
                        AND contact_info IS NULL
                        AND flag3 IS NULL
                        AND resp_score > 18
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting scored company count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/useful-company")
async def get_useful_company_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT COUNT(id) as count
                    FROM sn71_company
                    WHERE
                        m_description IS NULL
                        AND company_check IS NULL
                        AND contact_info IS NOT NULL
                        AND contact_info <> '{}'::jsonb
                        AND country = 'US'
                        AND flag2 IS NULL
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting useful company count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/person-company")
async def get_person_company_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(p.id) as count
                    FROM sn71_person p
                    INNER JOIN sn71_company c ON p.c_website = c.website
                    WHERE p.email IS NULL
                        AND p.seen IS NULL
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting person company count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/true-list")
async def get_true_list_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(p.id) as count
                    FROM sn71_person p
                    INNER JOIN sn71_company c ON p.c_website = c.website
                    WHERE p.email IS NOT NULL
                        AND p.contactout_info IS NOT NULL
                        AND p.email_check = 1
                        AND p.email_duplicate_check IS NOT TRUE
                        AND p.lead_check IS NULL
                        AND p.processing = 0
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting true list count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/checked-company")
async def get_checked_company_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(id) as count
                    FROM sn71_company
                    WHERE
                        (contact_info ->> 'employeesCount')::int > 0
                        and flag1 IS NULL
                        and company_check = 1
                        and country = 'US'
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting checked company count: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/counts/checked-company-detail")
async def get_checked_company_detail():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT
                        COALESCE(source, '(Unknown)') AS source,
                        COUNT(source) AS count
                    FROM sn71_company
                    WHERE
                        (contact_info ->> 'employeesCount')::int < 1000
                        AND (contact_info ->> 'employeesCount')::int > 0
                        AND flag1 IS NULL
                        AND company_check = 1
                        AND country = 'US'
                    GROUP BY COALESCE(source, '(Unknown)')
                    ORDER BY count DESC, source ASC
                """)
                details = await cur.fetchall()
                total = sum(row["count"] for row in details)
                return {"details": details, "total": total}
    except Exception as e:
        logger.error(f"Error getting checked company detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/generated-leads")
async def get_generated_leads_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(id) as count
                    FROM sn71_person
                    WHERE email IS NOT NULL
                        AND email_check = 1
                        AND seen = 302
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting generated leads count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/valued-leads")
async def get_valued_leads_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT count(id) as count
                    FROM sn71_person
                    WHERE seen = 308
                    AND email_duplicate_check IS NOT TRUE
                """)
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting valued leads count: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/counts/valued-leads-detail")
async def get_valued_leads_detail():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT
                        COALESCE(A.source, '(Unknown)') AS source,
                        COUNT(*) AS count
                    FROM sn71_company A
                    LEFT JOIN sn71_person B ON A.website = B.c_website
                    WHERE B.seen = 308
                    GROUP BY COALESCE(A.source, '(Unknown)')
                    ORDER BY count DESC, source ASC
                """)
                details = await cur.fetchall()
                total = sum(row["count"] for row in details)
                return {"details": details, "total": total}
    except Exception as e:
        logger.error(f"Error getting valued leads detail: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/connection-pool")
async def get_connection_pool_count():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT count(*) as count FROM pg_stat_activity WHERE datname = 'mydb'")
                result = await cur.fetchone()
                return {"count": result['count'] if result else 0}
    except Exception as e:
        logger.error(f"Error getting connection pool count: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/counts/max-connections")
async def get_max_connections():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SHOW max_connections")
                result = await cur.fetchone()
                return {"count": int(result['max_connections']) if result else 100}
    except Exception as e:
        logger.error(f"Error getting max connections: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Session CRUD API ====================

@app.get("/api/sessions")
async def list_sessions(
    search: Optional[str] = None,
    sort_by: str = "id",
    order: str = "asc"
):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                query = "SELECT * FROM sn71_session"
                params = []
                
                if search:
                    query += """ WHERE 
                        CAST(id AS TEXT) LIKE %s OR
                        proxy_ip LIKE %s OR
                        CAST(proxy_port AS TEXT) LIKE %s OR
                        username LIKE %s OR
                        "XSRF_TOKEN" LIKE %s OR
                        "contactout_seesion" LIKE %s OR
                        CAST(expires AS TEXT) LIKE %s OR
                        proxy_user LIKE %s OR
                        proxy_passwd LIKE %s OR
                        process LIKE %s OR
                        description LIKE %s OR
                        CAST(pay_date AS TEXT) LIKE %s
                    """
                    search_param = f"%{search}%"
                    params = [search_param] * 12
                
                valid_columns = ['id', 'proxy_ip', 'proxy_port', 'username', 'XSRF_TOKEN', 
                                 'contactout_seesion', 'expires', 'pay_date', 'proxy_user',
                                 'proxy_passwd', 'process', 'description']
                if sort_by in valid_columns:
                    if '_' in sort_by or sort_by.isupper():
                        sort_by = f'"{sort_by}"'
                    query += f" ORDER BY {sort_by} {order.upper()}"
                else:
                    query += " ORDER BY id ASC"
                
                await cur.execute(query, params)
                records = await cur.fetchall()
                return {"records": records}
    except Exception as e:
        logger.error(f"Error listing sessions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/sessions/{record_id}")
async def get_session(record_id: int):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM sn71_session WHERE id = %s", (record_id,))
                record = await cur.fetchone()
                if not record:
                    raise HTTPException(status_code=404, detail="Session not found")
                return record
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting session: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sessions")
async def create_session(data: Dict[str, Any]):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO sn71_session 
                    (proxy_ip, proxy_port, username, "XSRF_TOKEN", "contactout_seesion",
                     co_premium_user, expires, pay_date, proxy_user, proxy_passwd, process, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    data.get('proxy_ip'),
                    data.get('proxy_port'),
                    data.get('username'),
                    data.get('xsrf_token'),
                    data.get('contactout_session'),
                    data.get('co_premium_user', ''),
                    data.get('expires'),
                    data.get('pay_date') or None,
                    data.get('proxy_user'),
                    data.get('proxy_passwd'),
                    data.get('process'),
                    data.get('description')
                ))
                result = await cur.fetchone()
                await conn.commit()
                return {"id": result['id'], "message": "Session created successfully"}
    except Exception as e:
        logger.error(f"Error creating session: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/sessions/{record_id}")
async def update_session(record_id: int, data: Dict[str, Any]):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE sn71_session 
                    SET proxy_ip = %s, proxy_port = %s, username = %s,
                        "XSRF_TOKEN" = %s, "contactout_seesion" = %s, co_premium_user = %s,
                        expires = %s, pay_date = %s,
                        proxy_user = %s, proxy_passwd = %s, process = %s, description = %s
                    WHERE id = %s
                """, (
                    data.get('proxy_ip'),
                    data.get('proxy_port'),
                    data.get('username'),
                    data.get('xsrf_token'),
                    data.get('contactout_session'),
                    data.get('co_premium_user', ''),
                    data.get('expires'),
                    data.get('pay_date'),
                    data.get('proxy_user'),
                    data.get('proxy_passwd'),
                    data.get('process'),
                    data.get('description'),
                    record_id
                ))
                await conn.commit()
                return {"message": "Session updated successfully"}
    except Exception as e:
        logger.error(f"Error updating session: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/sessions/{record_id}")
async def delete_session(record_id: int):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM sn71_session WHERE id = %s", (record_id,))
                await conn.commit()
                return {"message": "Session deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting session: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Process CRUD API ====================

@app.get("/api/processes")
async def list_processes(
    search: Optional[str] = None,
    sort_by: str = "id",
    order: str = "asc"
):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                query = "SELECT * FROM sn71_process"
                params = []
                
                if search:
                    query += """ WHERE 
                        CAST(id AS TEXT) LIKE %s OR
                        process_name LIKE %s OR
                        ip LIKE %s OR
                        process_status LIKE %s OR
                        CAST(monitoring_time AS TEXT) LIKE %s
                    """
                    search_param = f"%{search}%"
                    params = [search_param] * 5
                
                valid_columns = ['id', 'process_name', 'ip', 'process_status', 'monitoring_time']
                if sort_by in valid_columns:
                    query += f" ORDER BY {sort_by} {order.upper()}"
                else:
                    query += " ORDER BY id ASC"
                
                await cur.execute(query, params)
                records = await cur.fetchall()
                return {"records": records}
    except Exception as e:
        logger.error(f"Error listing processes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== Process Monitoring API ====================

class ProcessStatusUpdate(BaseModel):
    process_name: str
    status: str
    ip: str
    
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "process_name": "python",
                "status": "running",
                "ip": "192.168.1.1"
            }
        }
    )

@app.get("/api/processes/by-ip/{ip}")
async def get_processes_by_ip(ip: str):
    """Get all process names for a specific IP address"""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT process_name
                    FROM sn71_process
                    WHERE ip = %s
                """, (ip,))
                rows = await cur.fetchall()
                process_names = [row['process_name'] for row in rows]
                return {"process_names": process_names}
    except Exception as e:
        logger.error(f"Error getting processes by IP: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/processes/update-status")
async def update_process_status(data: ProcessStatusUpdate):
    """Update process status by process_name and ip"""
    try:
        logger.info(f"Received update-status request: process_name={data.process_name}, status={data.status}, ip={data.ip}")
        process_name = data.process_name
        status = data.status
        ip = data.ip
        
        if not status:
            raise HTTPException(status_code=400, detail="Status cannot be empty")
        
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE sn71_process
                    SET process_status = %s,
                        monitoring_time = NOW()
                    WHERE process_name = %s
                      AND ip = %s
                """, (status, process_name, ip))
                rows_affected = cur.rowcount
                await conn.commit()
                if rows_affected == 0:
                    logger.warning(f"No rows updated for process_name={process_name}, ip={ip}")
                logger.info(f"Successfully updated process status: {process_name} -> {status} (rows affected: {rows_affected})")
                return {"message": "Process status updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating process status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/processes/{record_id}")
async def get_process(record_id: int):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM sn71_process WHERE id = %s", (record_id,))
                record = await cur.fetchone()
                if not record:
                    raise HTTPException(status_code=404, detail="Process not found")
                return record
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting process: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/processes")
async def create_process(data: Dict[str, Any]):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM sn71_process")
                result = await cur.fetchone()
                next_id = result['next_id']
                await cur.execute("""
                    INSERT INTO sn71_process 
                    (id, process_name, ip, process_status, monitoring_time)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                """, (next_id, data.get('process_name'), data.get('ip'), 'unknown'))
                result = await cur.fetchone()
                await conn.commit()
                return {"id": result['id'], "message": "Process created successfully"}
    except Exception as e:
        logger.error(f"Error creating process: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/processes/{record_id}")
async def update_process(record_id: int, data: Dict[str, Any]):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE sn71_process 
                    SET process_name = %s, ip = %s
                    WHERE id = %s
                """, (data.get('process_name'), data.get('ip'), record_id))
                await conn.commit()
                return {"message": "Process updated successfully"}
    except Exception as e:
        logger.error(f"Error updating process: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/processes/{record_id}")
async def delete_process(record_id: int):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM sn71_process WHERE id = %s", (record_id,))
                await conn.commit()
                return {"message": "Process deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting process: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check
@app.get("/health")
async def health_check():
    return {"status": "ok"}

# ==================== Data Apollo API ====================

@app.get("/api/data-apollo/sources")
async def list_data_apollo_sources():
    """Get distinct source values for Data Apollo filter (company_check=1, modified_time null)."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT DISTINCT COALESCE(source, '(Unknown)') as source
                    FROM sn71_company
                    WHERE company_check = 1
                      AND modified_time IS NULL
                    ORDER BY source ASC NULLS LAST
                """)
                rows = await cur.fetchall()
                sources = [r["source"] for r in rows]
                return {"sources": sources}
    except Exception as e:
        logger.error(f"Error listing data-apollo sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data-apollo/companies")
async def list_data_apollo_companies(sources: Optional[str] = Query(None, description="Comma-separated source values to filter")):
    """List sn71_company records for Data Apollo workflow.
    company_check=1, modified_time null. Optional filter by sources (comma-separated).
    """
    try:
        source_list = None
        if sources and sources.strip():
            source_list = [s.strip() for s in sources.split(",") if s.strip()]

        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                # if source_list is not None and len(source_list) > 0:
                #     placeholders = ", ".join(["%s"] * len(source_list))
                #     await cur.execute(f"""
                #         SELECT id, business as name, website, source, resp_score as score, created_time, modified_time,
                #                (SELECT count(*) FROM sn71_person p WHERE p.c_website = c.website) as person_count
                #         FROM sn71_company c
                #         WHERE company_check = 1
                #           AND created_time IS NULL
                #           AND modified_time IS NULL
                #           AND COALESCE(source, '(Unknown)') IN ({placeholders})
                #         ORDER BY
                #         resp_score DESC NULLS LAST
                #         LIMIT 1
                #     """, tuple(source_list))
                # else:
                await cur.execute("""
                    SELECT id, business as name, website, source, resp_score as score, created_time, modified_time,
                            (SELECT count(*) FROM sn71_person p WHERE p.c_website = c.website) as person_count
                    FROM sn71_company c
                    WHERE contact_info IS NOT NULL AND contact_info <> '{}'::jsonb AND (contact_info ->> 'employeesCount')::int > 10
                        AND created_time IS NULL
                        AND modified_time IS NULL
                    ORDER BY 
                    resp_score DESC NULLS LAST
                    LIMIT 1
                """)
                records = await cur.fetchall()
                result = []
                ids_to_mark = []
                for r in records:
                    result.append({
                        "id": r["id"],
                        "name": r["name"] or "",
                        "website": r["website"] or "",
                        "source": r["source"] or "",
                        "score": r["score"],
                        "person_count": r["person_count"] or 0,
                        "created_time": r["created_time"].isoformat() if r["created_time"] else None,
                        "modified_time": r["modified_time"].isoformat() if r["modified_time"] else None,
                    })
                    ids_to_mark.append(r["id"])
                if ids_to_mark:
                    await cur.execute(
                        "UPDATE sn71_company SET created_time = NOW() WHERE id = ANY(%s::int[]) AND created_time IS NULL",
                        (ids_to_mark,),
                    )
                    await conn.commit()
                return {"records": result}
    except Exception as e:
        logger.error(f"Error listing data-apollo companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _extract_domain_from_website(website: str) -> str:
    """Extract domain (e.g. falmouthpolice.com) from URL or return as-is if already domain."""
    if not website:
        return ""
    s = (website or "").strip().lower()
    # Remove protocol
    for prefix in ("https://", "http://", "www."):
        if s.startswith(prefix):
            s = s[len(prefix):]
    # Remove trailing path
    if "/" in s:
        s = s.split("/")[0]
    return s or website


@app.post("/api/data-apollo/process")
async def process_apollo_data(payload: Dict[str, Any] = Body(...)):
    """Process Apollo JSON data and insert into sn71_person.
    Uses c_website and c_name from the selected company record.
    Sets created_time on the company record to NOW().
    """
    company_id = payload.get("company_id")
    apollo_json_str = payload.get("apollo_json", "")
    if not company_id:
        raise HTTPException(status_code=400, detail="company_id is required")
    if not apollo_json_str or not apollo_json_str.strip():
        raise HTTPException(status_code=400, detail="apollo_json is required")

    try:
        apollo_data = json.loads(apollo_json_str)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    people = apollo_data.get("people", [])
    if not people:
        raise HTTPException(status_code=400, detail="No people found in Apollo JSON")

    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT website, business as name FROM sn71_company WHERE id = %s",
                    (company_id,)
                )
                row = await cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Company not found")
                c_website_raw = row["website"] or ""
                c_name = row["name"] or ""
                c_website = _extract_domain_from_website(c_website_raw) or c_website_raw

                persons = []
                for p in people:
                    person = copy.deepcopy(p)
                    first_name = person.get("first_name", "")
                    last_name = person.get("last_name", "")
                    if not first_name or not last_name:
                        continue
                    linkedin_url = person.get("linkedin_url", "")
                    city = person.get("city", "")
                    state = person.get("state", "")
                    title = person.get("title", "")
                    if linkedin_url:
                        person["liVanity"] = linkedin_url.rstrip("/").split("/")[-1]
                    else:
                        person["liVanity"] = ""
                    person["countryCode"] = "US"
                    person["locality"] = f"{city}, {state}, United States"
                    person["experience"] = [title] if title else []
                    persons.append(person)

                sql = """
                    INSERT INTO sn71_person (
                        c_website, c_name, sources_domain, first_name, last_name, contactout_info
                    )
                    SELECT %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM sn71_person
                        WHERE c_website = %s AND first_name = %s AND last_name = %s
                    )
                """
                params_list = [
                    (
                        c_website, c_name, "apollo.io",
                        p.get("first_name", ""), p.get("last_name", ""), json.dumps(p),
                        c_website, p.get("first_name", ""), p.get("last_name", ""),
                    )
                    for p in persons
                ]
                inserted = 0
                for params in params_list:
                    await cur.execute(sql, params)
                    inserted += cur.rowcount
                skipped = len(persons) - inserted

                await cur.execute(
                    "UPDATE sn71_company SET created_time = NOW() WHERE id = %s",
                    (company_id,),
                )
                await conn.commit()

        return {
            "message": "Processed successfully",
            "inserted": inserted,
            "skipped": skipped,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing Apollo data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-apollo/end/{company_id}")
async def end_apollo_company(company_id: int):
    """Set modified_time to NOW() for the company record."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET modified_time = NOW() WHERE id = %s",
                    (company_id,),
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Company not found")
                await conn.commit()
        return {"message": "End time set successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting end time: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── get_company queue endpoints ───────────────────────────────────────────────
####  reprocessing valied failed company at com_check
# @app.get("/api/data-apollo/company-queue")
# async def get_company_queue():
#     """Fetch next company for get_company mode.

#     Selects the highest-priority unprocessed company (m_description IS NULL,
#     company_check IS NULL, contact_info IS NULL, country='US', flag3 IS NULL),
#     locks it with FOR UPDATE SKIP LOCKED so concurrent processes never collide,
#     then immediately marks it in-progress (flag3='1').
#     """
#     try:
#         async with DB_POOL.connection() as conn:
#             async with conn.transaction():
#                 async with conn.cursor() as cur:
#                     await cur.execute("""
#                         SELECT id, business AS name, website, source, resp_score AS score, country
#                         FROM sn71_company
#                         WHERE
#                             m_description IS NULL
#                             -- AND company_check IS NULL
#                             AND contact_info IS NULL
#                             AND country = 'US'
#                             AND flag3 IS NULL
#                             AND company_check_reason like 'vali_check_company_base failed'
#                             AND company_check = 0
#                         ORDER BY
#                             CASE
#                                 WHEN source IN (
#                                     'hunter.io-50-200', 'hunter.io-50-500', 'hunter.io-50-1000',
#                                     'hunter.io-50-10000', 'hunter.io-50-10001+'
#                                 ) THEN 0
#                                 ELSE 1
#                             END,
#                             resp_score DESC NULLS LAST
#                         LIMIT 1
#                         FOR UPDATE SKIP LOCKED
#                     """)
#                     row = await cur.fetchone()
#                     if not row:
#                         return {"record": None}

#                     await cur.execute(
#                         "UPDATE sn71_company SET flag3 = '1' WHERE id = %s AND flag3 IS NULL",
#                         (row["id"],),
#                     )

#         return {
#             "record": {
#                 "id": row["id"],
#                 "name": row["name"] or "",
#                 "website": row["website"] or "",
#                 "source": row["source"] or "",
#                 "score": row["score"],
#                 "country": row["country"] or "",
#             }
#         }
#     except Exception as e:
#         logger.error(f"Error fetching company queue: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
# ----------------------------------------------------------------------------------------------------------

############################################################################################################
#####               This is normal mode of get_company with apollo  [close 2026.4.21]                  #####
############################################################################################################ 
# @app.get("/api/data-apollo/company-queue")
# async def get_company_queue():
#     """Fetch next company for get_company mode.

#     Selects the highest-priority unprocessed company (m_description IS NULL,
#     company_check IS NULL, contact_info IS NULL, country='US', flag3 IS NULL),
#     locks it with FOR UPDATE SKIP LOCKED so concurrent processes never collide,
#     then immediately marks it in-progress (flag3='1').
#     """
#     try:
#         async with DB_POOL.connection() as conn:
#             async with conn.transaction():
#                 async with conn.cursor() as cur:
#                     await cur.execute("""
#                         SELECT id, business AS name, website, source, resp_score AS score, country
#                         FROM sn71_company
#                         WHERE 
#                         (flag3 IS NULL OR flag3 < '2') AND (contact_info IS NULL OR contact_info = '{}'::jsonb OR contact_info = 'null'::jsonb)
#                         ORDER BY
#                             CASE
#                                 WHEN source IN (
#                                     'hunter.io-50-200', 'hunter.io-50-500', 'hunter.io-50-1000',
#                                     'hunter.io-50-10000', 'hunter.io-50-10001+'
#                                 ) THEN 0
#                                 ELSE 1
#                             END,
#                             resp_score DESC NULLS LAST
#                         LIMIT 1
#                         FOR UPDATE SKIP LOCKED
#                     """)
#                     row = await cur.fetchone()
#                     if not row:
#                         return {"record": None}

#                     await cur.execute(
#                         "UPDATE sn71_company SET flag3 = '11' WHERE id = %s",
#                         (row["id"],),
#                     )

#         return {
#             "record": {
#                 "id": row["id"],
#                 "name": row["name"] or "",
#                 "website": row["website"] or "",
#                 "source": row["source"] or "",
#                 "score": row["score"],
#                 "country": row["country"] or "",
#             }
#         }
#     except Exception as e:
#         logger.error(f"Error fetching company queue: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


############################################################################################################
#####   This is abnormal mode of get_company with apollo(for source: new_apollo, 1-10) open 2026.4.21  #####
############################################################################################################ 
@app.get("/api/data-apollo/company-queue")
async def get_company_queue():
    """Fetch next company for get_company mode.

    Selects the highest-priority unprocessed company (m_description IS NULL,
    company_check IS NULL, contact_info IS NULL, country='US', flag3 IS NULL),
    locks it with FOR UPDATE SKIP LOCKED so concurrent processes never collide,
    then immediately marks it in-progress (flag3='1').
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT id, business AS name, website, source, resp_score AS score, country
                        FROM sn71_company
                        WHERE 
                        (flag3 IS NULL) AND (contact_info IS NOT NULL and contact_info->>'source' = 'apollo_new')
						ORDER BY (contact_info->>'employeesCount')::int DESC NULLS LAST
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """)
                    row = await cur.fetchone()
                    if not row:
                        return {"record": None}

                    await cur.execute(
                        "UPDATE sn71_company SET source = 'apollo_new', flag3 = '11' WHERE id = %s",
                        (row["id"],),
                    )

        return {
            "record": {
                "id": row["id"],
                "name": row["name"] or "",
                "website": row["website"] or "",
                "source": row["source"] or "",
                "score": row["score"],
                "country": row["country"] or "",
            }
        }
    except Exception as e:
        logger.error(f"Error fetching company queue: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/data-apollo/company-info")
async def save_company_info(payload: Dict[str, Any] = Body(...)):
    """Save scraped Apollo company info.

    Stores the full company payload as JSON in ``contact_info`` and advances
    the row to done (flag3='2').  The caller must include ``company_id`` (the
    database integer id) in the request body alongside the company fields.
    """
    company_id = payload.get("company_id")
    if not company_id:
        raise HTTPException(status_code=400, detail="company_id is required in payload")

    # Store everything except the internal company_id key as the contact data
    contact_data = {k: v for k, v in payload.items() if k != "company_id"}

    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET contact_info = %s WHERE id = %s",
                    (Json(contact_data), company_id),
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Company not found")
                await conn.commit()
        return {"message": "Company info saved", "company_id": company_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error saving company info for id={company_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-apollo/company-queue/{company_id}/end")
async def end_company_queue(company_id: int):
    """Mark a get_company-queue record as done (flag3='2') without saving data.

    Called for skipped or failed companies so they are not retried.
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET flag3 = '2' WHERE id = %s",
                    (company_id,),
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Company not found")
                await conn.commit()
        return {"message": "Company queue entry ended", "company_id": company_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ending company queue for id={company_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── new_person queue endpoints ────────────────────────────────────────────────

@app.get("/api/data-apollo/new-person-queue")
async def get_new_person_queue():
    """Fetch next company for new_person mode.

    Selects the highest-priority company that has contact_info with employeesCount
    between 0 and 1000, flag1 IS NULL, company_check=1, country='US'.
    Locks with FOR UPDATE SKIP LOCKED and immediately marks it in-progress (flag1=-1).
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT id, business AS name, website, source, resp_score AS score, country,
                               contact_info
                        FROM sn71_company
                        WHERE
                            flag1 IS NULL
                            AND company_check = 1
                        ORDER BY
                            (contact_info ->> 'employeesCount')::int ASC NULLS LAST,
                            resp_score DESC NULLS LAST
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """)
                    row = await cur.fetchone()
                    if not row:
                        return {"record": None}

                    await cur.execute(
                        "UPDATE sn71_company SET flag1 = -1 WHERE id = %s AND flag1 IS NULL",
                        (row["id"],),
                    )

        return {
            "record": {
                "id": row["id"],
                "name": row["name"] or "",
                "website": row["website"] or "",
                "source": row["source"] or "",
                "score": row["score"],
                "country": row["country"] or "",
                "contact_info": row["contact_info"] or {},
            }
        }
    except Exception as e:
        logger.error(f"Error fetching new-person queue: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-apollo/new-person-queue/{company_id}/success")
async def end_new_person_queue_success(company_id: int):
    """Mark new_person company as successfully processed (flag1=1)."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET flag1 = 1 WHERE id = %s",
                    (company_id,),
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Company not found")
                await conn.commit()
        return {"message": "Company marked as success", "company_id": company_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking new-person success for id={company_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-apollo/new-person-queue/{company_id}/fail")
async def end_new_person_queue_fail(company_id: int):
    """Mark new_person company as failed (flag1=0)."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET flag1 = 0 WHERE id = %s",
                    (company_id,),
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail="Company not found")
                await conn.commit()
        return {"message": "Company marked as failed", "company_id": company_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error marking new-person fail for id={company_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── worker status endpoints ──────────────────────────────────────────────────

class WorkerStatusUpdate(BaseModel):
    worker_id: int
    ip: str
    status: str  # "running" | "done" | "error"


@app.get("/api/data-apollo/worker-status")
async def get_worker_status():
    """Return latest status for all workers (one row per worker_id)."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT worker_id, ip, status,
                           updated_at AT TIME ZONE current_setting('TimeZone') AT TIME ZONE 'UTC' AS updated_at
                    FROM sn71_apollo_worker_status
                    ORDER BY ip ASC
                """)
                rows = await cur.fetchall()

        merged: Dict[tuple[int, str], Dict[str, Any]] = {}
        for r in rows:
            merged[(r["worker_id"], r["ip"])] = {
                "worker_id": r["worker_id"],
                "ip": r["ip"],
                "status": r["status"],
                "updated_at": r["updated_at"].isoformat() + 'Z' if r["updated_at"] else None,
            }

        async with WORKER_STATUS_BUFFER_LOCK:
            for (worker_id, ip), payload in WORKER_STATUS_BUFFER.items():
                pending_at = payload["updated_at"]
                merged[(worker_id, ip)] = {
                    "worker_id": worker_id,
                    "ip": ip,
                    "status": payload["status"],
                    "updated_at": pending_at.isoformat().replace("+00:00", "Z") if pending_at else None,
                }

        workers = sorted(merged.values(), key=lambda x: x["ip"])
        return {"workers": workers}
    except Exception as e:
        logger.error(f"Error fetching worker status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-apollo/worker-status")
async def upsert_worker_status(data: WorkerStatusUpdate):
    """Buffer worker status updates and flush to DB in batch."""
    valid_statuses = {"running", "done", "error"}
    if data.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"status must be one of {valid_statuses}")
    try:
        key = (data.worker_id, data.ip)
        now_utc = datetime.now(timezone.utc)
        async with WORKER_STATUS_BUFFER_LOCK:
            if key not in WORKER_STATUS_BUFFER and len(WORKER_STATUS_BUFFER) >= WORKER_STATUS_MAX_BUFFER:
                WORKER_STATUS_BUFFER.pop(next(iter(WORKER_STATUS_BUFFER)))
            WORKER_STATUS_BUFFER[key] = {"status": data.status, "updated_at": now_utc}
        return {"message": "Worker status updated", "worker_id": data.worker_id, "status": data.status}
    except Exception as e:
        logger.error(f"Error upserting worker status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/data-apollo/worker-status/{worker_id}/{ip:path}")
async def delete_worker_status(worker_id: int, ip: str):
    """Delete a worker status record by (worker_id, ip)."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    DELETE FROM sn71_apollo_worker_status
                    WHERE worker_id = %s AND ip = %s
                """, (worker_id, ip))
                deleted = cur.rowcount
                await conn.commit()
        if deleted == 0:
            raise HTTPException(status_code=404, detail="Worker record not found")
        return {"message": "Worker record deleted", "worker_id": worker_id, "ip": ip}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting worker status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Data ContactOut API ====================

@app.get("/api/data-contactout/companies")
async def list_data_contactout_companies():
    """Fetch the next batch of companies for ContactOut scraping.

    Selects company_check=1, flag2 IS NULL companies.
    Uses FOR UPDATE SKIP LOCKED so concurrent workers never claim the same row.
    Marks claimed rows as in-progress (flag2=-1) within the same transaction.
    Returns up to 10 records.
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT id, business AS name, website, source, country
                        FROM sn71_company
                        WHERE flag2 IS NULL AND contact_info IS NOT NULL AND contact_info <> '{}'::jsonb
                        ORDER BY id ASC
                        LIMIT 10
                        FOR UPDATE SKIP LOCKED
                    """)
                    rows = await cur.fetchall()
                    if not rows:
                        return {"records": []}

                    ids = [r["id"] for r in rows]
                    await cur.execute(
                        "UPDATE sn71_company SET flag2 = '-1' WHERE id = ANY(%s::int[])",
                        (ids,),
                    )

        records = [
            {
                "id": r["id"],
                "name": r["name"] or "",
                "website": r["website"] or "",
                "source": r["source"] or "",
                "country": r["country"] or "",
            }
            for r in rows
        ]
        return {"records": records}
    except Exception as e:
        logger.error(f"Error fetching data-contactout companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-contactout/process")
async def process_contactout_data(payload: Dict[str, Any] = Body(...)):
    """Process one page of ContactOut search results and upsert into sn71_person.

    Accepts:
        company_id     : int  — database ID of the company
        contactout_json: str  — raw JSON body captured from the ContactOut API response

    De-duplicates by (c_website, first_name, last_name).
    Stores the full raw person object in contactout_info.
    If the ContactOut person includes an email address, sets the email column directly.
    """
    company_id = payload.get("company_id")
    contactout_json_str = payload.get("contactout_json", "")

    if not company_id:
        raise HTTPException(status_code=400, detail="company_id is required")
    if not contactout_json_str or not contactout_json_str.strip():
        raise HTTPException(status_code=400, detail="contactout_json is required")

    try:
        data = json.loads(contactout_json_str)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    # Support common ContactOut response envelope shapes
    people = None
    for key in ("people", "results", "data", "contacts", "profiles", "items"):
        val = data.get(key)
        if isinstance(val, list) and val:
            people = val
            break

    if not people:
        raise HTTPException(status_code=400, detail="No people found in contactout_json")

    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT website, business AS name FROM sn71_company WHERE id = %s",
                    (company_id,),
                )
                row = await cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Company id={company_id} not found")

                c_website = _extract_domain_from_website(row["website"] or "") or (row["website"] or "")
                c_name = row["name"] or ""

                inserted = 0
                skipped = 0

                for person in people:
                    if not isinstance(person, dict):
                        skipped += 1
                        continue

                    # ── Name extraction (handle fullName, snake_case, camelCase) ──
                    full_name = (
                        person.get("fullName")
                        or person.get("full_name")
                        or person.get("name")
                        or ""
                    ).strip()
                    name_parts = full_name.split() if full_name else []
                    first_name = (
                        person.get("first_name")
                        or person.get("firstName")
                        or (name_parts[0] if name_parts else "")
                    ).strip()
                    last_name = (
                        person.get("last_name")
                        or person.get("lastName")
                        or (" ".join(name_parts[1:]) if len(name_parts) > 1 else "")
                    ).strip()

                    # ── Email extraction (ContactOut: contactInfo.emails[].value) ─
                    email = None
                    contact_info_obj = person.get("contactInfo") or {}
                    if isinstance(contact_info_obj, dict):
                        emails_list = contact_info_obj.get("emails") or []
                        for em in emails_list:
                            val = em.get("value") or "" if isinstance(em, dict) else str(em)
                            # Skip masked / placeholder values
                            if val and "***" not in val and "@" in val:
                                email = val.strip()
                                break
                    # Fallback: top-level email keys
                    if not email:
                        email_raw = (
                            person.get("email")
                            or (person.get("emails") or [None])[0]
                            or None
                        )
                        if isinstance(email_raw, dict):
                            email_raw = email_raw.get("email") or email_raw.get("address") or None
                        email = (str(email_raw).strip() if email_raw else None) or None

                    await cur.execute(
                        """
                        INSERT INTO sn71_person (
                            c_website, c_name, sources_domain,
                            first_name, last_name, email, contactout_info
                        )
                        SELECT %s, %s, %s, %s, %s, %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM sn71_person
                            WHERE c_website = %s AND first_name = %s AND last_name = %s
                        )
                        """,
                        (
                            c_website, c_name, "contactout.com",
                            first_name, last_name, email, json.dumps(person),
                            c_website, first_name, last_name,
                        ),
                    )
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1

                await conn.commit()

        return {
            "message": "Processed successfully",
            "inserted": inserted,
            "skipped": skipped,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing ContactOut data for company_id={company_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-contactout/end/{company_id}")
async def end_contactout_company(company_id: int):
    """Mark a ContactOut-scraped company as done (flag2=1)."""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET flag2 = '1' WHERE id = %s",
                    (company_id,),
                )
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail=f"Company id={company_id} not found")
                await conn.commit()
        return {"message": "ContactOut scrape marked as done", "company_id": company_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ending ContactOut scrape for company_id={company_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-contactout/reset-stuck")
async def reset_stuck_contactout_companies():
    """Reset companies stuck in-progress (flag2=-1) back to unprocessed (flag2=NULL).

    Called at scraper startup to recover from crashed/interrupted runs.
    Only resets rows that are genuinely orphaned (flag2=-1), never rows that
    are flag2=1 (completed) or flag2=NULL (already queued).
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE sn71_company SET flag2 = NULL WHERE flag2 = '-1' AND company_check = 1"
                )
                reset_count = cur.rowcount
                await conn.commit()
        return {"message": f"Reset {reset_count} stuck company(ies) to unprocessed", "reset": reset_count}
    except Exception as e:
        logger.error(f"Error resetting stuck ContactOut companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/data-contactout/reset-done")
async def reset_done_contactout_companies(limit: int = Query(10, ge=1, le=1000, description="Max companies to reset")):
    """Reset completed companies (flag2='1') back to unprocessed (flag2=NULL) for re-scraping.

    Useful for testing and re-scraping after bugs are fixed.
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """UPDATE sn71_company SET flag2 = NULL
                       WHERE id IN (
                           SELECT id FROM sn71_company
                           WHERE flag2 = '1' AND company_check = 1
                           ORDER BY id ASC
                           LIMIT %s
                       )""",
                    (limit,),
                )
                reset_count = cur.rowcount
                await conn.commit()
        return {"message": f"Reset {reset_count} done company(ies) to unprocessed", "reset": reset_count}
    except Exception as e:
        logger.error(f"Error resetting done ContactOut companies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Apollo Search URL API (sn71_company_apollo_searchurl) ====================

@app.get("/api/apollo-search/unmodified-records")
async def get_apollo_search_unmodified_records(
    limit: int = Query(10, ge=1, le=100, description="Max records to return"),
    location: str = Query("US", description="Location filter: 'US' or other"),
):
    """Atomically claim unfinished records from sn71_company_apollo_searchurl.

    Uses a CTE with FOR UPDATE SKIP LOCKED so concurrent workers never claim the
    same row. search_condition is used as an in-progress lock flag:
    NULL -> available, 'scrapping' -> currently being processed.
    The claim is made inside the same transaction.
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                if location == "US":
                    await cur.execute("""
                        WITH claimed AS (
                            SELECT id
                            FROM sn71_company_apollo_searchurl
                            WHERE ((total_pages IS NOT NULL AND page < total_pages) OR (total_pages IS NULL AND page IS NULL))
                              AND search_condition IS NULL
                            ORDER BY id ASC
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sn71_company_apollo_searchurl s
                        SET created_date = CURRENT_DATE,
                            search_condition = 'scrapping'
                        FROM claimed
                        WHERE s.id = claimed.id
                        RETURNING s.*
                    """, (limit,))
                elif location == "NONUS":
                    await cur.execute("""
                        WITH claimed AS (
                            SELECT id
                            FROM sn71_company_apollo_searchurl
                            WHERE ((total_pages IS NOT NULL AND page < total_pages) OR (total_pages IS NULL AND page IS NULL))
                              AND search_condition IS NULL
                            ORDER BY id ASC
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sn71_company_apollo_searchurl s
                        SET created_date = CURRENT_DATE,
                            search_condition = 'scrapping'
                        FROM claimed
                        WHERE s.id = claimed.id
                        RETURNING s.*
                    """, (limit,))
                elif location == "REALTIME":
                    await cur.execute("""
                        WITH claimed AS (
                            SELECT id
                            FROM sn71_company_apollo_searchurl
                            WHERE ((total_pages IS NOT NULL AND page < total_pages) OR (total_pages IS NULL AND page IS NULL))
                              AND search_condition IS NULL
                              AND real_time = 1
                            ORDER BY id ASC
                            LIMIT %s
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE sn71_company_apollo_searchurl s
                        SET created_date = CURRENT_DATE,
                            search_condition = 'scrapping'
                        FROM claimed
                        WHERE s.id = claimed.id
                        RETURNING s.*
                    """, (limit,))
                rows = await cur.fetchall()
                await conn.commit()
                result = []
                for r in rows:
                    row_dict = dict(r)
                    for k, v in row_dict.items():
                        if hasattr(v, "isoformat"):
                            row_dict[k] = v.isoformat()
                    result.append(row_dict)
                return {"records": result}
    except Exception as e:
        logger.error(f"Error getting apollo-search unmodified records: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apollo-search/{search_id}/pagination")
async def save_apollo_search_pagination(search_id: int, pagination_info: Dict[str, Any] = Body(...)):
    """Update pagination info for a search URL record."""
    page = pagination_info.get("page")
    total_pages = pagination_info.get("total_pages")
    total_entries = pagination_info.get("total_entries")
    location = pagination_info.get("location", "US")

    if page is None or total_pages is None or total_entries is None:
        raise HTTPException(status_code=400, detail="page, total_pages, total_entries required")

    try:
        if page == total_pages or page == 100:
            if location == "REALTIME":
                sql = """
                    UPDATE sn71_company_apollo_searchurl
                    SET page = %s, total_pages = %s, total_entries = %s,
                        created_date = CURRENT_DATE, modified_date = CURRENT_DATE,
                        real_time = 0
                    WHERE id = %s
                """
            else:
                sql = """
                    UPDATE sn71_company_apollo_searchurl
                    SET page = %s, total_pages = %s, total_entries = %s,
                        created_date = CURRENT_DATE, modified_date = CURRENT_DATE
                    WHERE id = %s
                """
        else:
            sql = """
                UPDATE sn71_company_apollo_searchurl
                SET page = %s, total_pages = %s, total_entries = %s,
                    created_date = CURRENT_DATE
                WHERE id = %s
            """
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (page, total_pages, total_entries, search_id))
                await conn.commit()
                return {"rowcount": cur.rowcount}
    except Exception as e:
        logger.error(f"Error saving apollo-search pagination: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apollo-search/{search_id}/end-time")
async def update_apollo_search_end_time(search_id: int, data: Dict[str, Any] = Body(None)):
    """Set modified_date and clear the in-progress search_condition lock.
    For REALTIME location, also set real_time = 0.
    """
    location = (data.get("location") if data else None) or "US"
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                if location == "REALTIME":
                    await cur.execute("""
                        UPDATE sn71_company_apollo_searchurl
                        SET modified_date = NOW(),
                            search_condition = NULL,
                            real_time = 0
                        WHERE id = %s
                    """, (search_id,))
                else:
                    await cur.execute("""
                        UPDATE sn71_company_apollo_searchurl
                        SET modified_date = NOW(),
                            search_condition = NULL
                        WHERE id = %s
                    """, (search_id,))
                await conn.commit()
                return {"rowcount": cur.rowcount}
    except Exception as e:
        logger.error(f"Error updating apollo-search end time: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apollo-search/reset-stuck-realtime")
async def reset_stuck_realtime_records(timeout_minutes: int = Query(30, ge=1, le=1440, description="Reset records stuck for N minutes")):
    """Reset REALTIME records stuck in-progress (search_condition='scrapping') back to available.

    Called to recover from crashed/interrupted runs.
    Only resets records that have been stuck for longer than timeout_minutes.
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE sn71_company_apollo_searchurl
                    SET search_condition = NULL
                    WHERE search_condition = 'scrapping'
                      AND real_time = 1
                      AND created_date < CURRENT_DATE - INTERVAL '%s minutes'
                """, (timeout_minutes,))
                reset_count = cur.rowcount
                await conn.commit()
        return {"message": f"Reset {reset_count} stuck REALTIME record(s)", "reset": reset_count}
    except Exception as e:
        logger.error(f"Error resetting stuck REALTIME records: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apollo-search/reset-stuck-realtime-force")
async def reset_stuck_realtime_records_force():
    """Force reset ALL stuck REALTIME records immediately (search_condition='scrapping' and real_time=1).

    USE WITH CAUTION - resets all stuck records regardless of how long they've been stuck.
    """
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    UPDATE sn71_company_apollo_searchurl
                    SET search_condition = NULL
                    WHERE search_condition = 'scrapping'
                      AND real_time = 1
                """)
                reset_count = cur.rowcount
                await conn.commit()
        return {"message": f"Force reset {reset_count} stuck REALTIME record(s)", "reset": reset_count}
    except Exception as e:
        logger.error(f"Error force resetting stuck REALTIME records: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apollo-search/organizations")
async def save_apollo_organizations(payload: Dict[str, Any] = Body(...)):
    """
    Save organizations: US companies go to sn71_company, non-US to sn71_company_nonus.
    Orgs missing primary_domain are skipped.
    Optionally accepts real_time_id to associate with organizations for REALTIME mode.
    """
    organizations = payload.get("organizations", []) if isinstance(payload, dict) else payload
    real_time_id = payload.get("real_time_id") if isinstance(payload, dict) else None

    inserted_us = 0
    inserted_nonus = 0

    sql_us_with_realtime = """
        INSERT INTO sn71_company (
            business, website, source, country,
            resp_score, wayback_score, sec_edgar_score,
            whois_dnsbl_score, gdelt_score, companies_house_score,
            apollo_info, real_time_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (website) DO UPDATE SET apollo_info = EXCLUDED.apollo_info,
                                          real_time_id = EXCLUDED.real_time_id
    """
    sql_us = """
        INSERT INTO sn71_company (
            business, website, source, country,
            resp_score, wayback_score, sec_edgar_score,
            whois_dnsbl_score, gdelt_score, companies_house_score,
            apollo_info
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (website) DO UPDATE SET apollo_info = EXCLUDED.apollo_info
    """
    sql_nonus_with_realtime = """
        INSERT INTO sn71_company_nonus (
            business, apollo_id, domain, website, source, country, apollo_info, created_time, real_time_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (website) DO UPDATE SET apollo_info = EXCLUDED.apollo_info,
                                          real_time_id = EXCLUDED.real_time_id
    """
    sql_nonus = """
        INSERT INTO sn71_company_nonus (
            business, apollo_id, domain, website, source, country, apollo_info, created_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (website) DO UPDATE SET apollo_info = EXCLUDED.apollo_info
    """

    try:
        async with DB_POOL.connection() as conn:
            for org in organizations:
                if org.get("primary_domain", "") == "":
                    continue
                try:
                    async with conn.transaction():
                        async with conn.cursor() as cur:
                            primary_domain = org.get("primary_domain", "")
                            snippet = org.get("snippet", {})
                            country = snippet.get("country", "") if snippet else ""

                            if country in ("US", "United States"):
                                if real_time_id is not None:
                                    await cur.execute(sql_us_with_realtime, (
                                        org.get("name", ""),
                                        primary_domain,
                                        "apollo.io",
                                        country,
                                        0, 0, 0, 0, 0, 0,
                                        Json(org),
                                        real_time_id,
                                    ))
                                else:
                                    await cur.execute(sql_us, (
                                        org.get("name", ""),
                                        primary_domain,
                                        "apollo.io",
                                        country,
                                        0, 0, 0, 0, 0, 0,
                                        Json(org),
                                    ))
                                inserted_us += 1
                            else:
                                if real_time_id is not None:
                                    await cur.execute(sql_nonus_with_realtime, (
                                        org.get("name", ""),
                                        org.get("id", ""),
                                        primary_domain,
                                        org.get("website_url", ""),
                                        "apollo.io",
                                        country,
                                        Json(org),
                                        real_time_id,
                                    ))
                                else:
                                    await cur.execute(sql_nonus, (
                                        org.get("name", ""),
                                        org.get("id", ""),
                                        primary_domain,
                                        org.get("website_url", ""),
                                        "apollo.io",
                                        country,
                                        Json(org),
                                    ))
                                inserted_nonus += 1
                except Exception as e:
                    logger.error(f"Error processing organization {org.get('name', '')}: {e}")
                    continue
    except Exception as e:
        logger.error(f"Error in save_apollo_organizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"inserted_us": inserted_us, "inserted_nonus": inserted_nonus}


# ==================== OpenRouter API Credits ====================

def mask_api_key(api_key: str) -> str:
    """Mask an API key for safe UI/API display."""
    if not api_key:
        return ""
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}{'*' * max(len(api_key) - 8, 4)}{api_key[-4:]}"


def normalize_openrouter_credits_payload(payload: Dict[str, Any]) -> Dict[str, float]:
    credits = payload.get("data", {}) if isinstance(payload, dict) else {}
    total_credits = float(credits.get("total_credits", 0) or 0)
    total_usage = float(credits.get("total_usage", 0) or 0)
    remaining = total_credits - total_usage
    return {
        "total_credits": total_credits,
        "total_usage": total_usage,
        "remaining": remaining
    }


@app.get("/api/openrouter/keys")
async def list_openrouter_keys():
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, email, api_key, label, is_active, created_at, updated_at
                    FROM sn71_openrouter_key
                    ORDER BY id DESC
                """)
                rows = await cur.fetchall()

        records = []
        for row in rows:
            records.append({
                "id": row["id"],
                "email": row["email"],
                "label": row["label"],
                "is_active": bool(row["is_active"]),
                "api_key_masked": mask_api_key(row["api_key"]),
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
            })

        return {"records": records}
    except Exception as e:
        logger.error(f"Error listing OpenRouter keys: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/openrouter/keys")
async def create_openrouter_key(data: Dict[str, Any] = Body(...)):
    email = (data.get("email") or "").strip()
    api_key = (data.get("api_key") or "").strip()
    label = (data.get("label") or "").strip() or None
    is_active = bool(data.get("is_active", True))

    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    if not api_key:
        raise HTTPException(status_code=400, detail="api_key is required")

    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    INSERT INTO sn71_openrouter_key (email, api_key, label, is_active)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, email, api_key, label, is_active, created_at, updated_at
                """, (email, api_key, label, is_active))
                row = await cur.fetchone()
                await conn.commit()

        return {
            "message": "OpenRouter key created successfully",
            "record": {
                "id": row["id"],
                "email": row["email"],
                "label": row["label"],
                "is_active": bool(row["is_active"]),
                "api_key_masked": mask_api_key(row["api_key"]),
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
            }
        }
    except Exception as e:
        logger.error(f"Error creating OpenRouter key: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/openrouter/keys/credits")
async def get_openrouter_keys_credits():
    """Get credit information for each active OpenRouter key."""
    try:
        import httpx

        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT id, email, api_key, label, is_active, created_at, updated_at
                    FROM sn71_openrouter_key
                    ORDER BY id DESC
                """)
                rows = await cur.fetchall()

        records = []
        async with httpx.AsyncClient(timeout=10) as client:
            for row in rows:
                record = {
                    "id": row["id"],
                    "email": row["email"],
                    "label": row["label"],
                    "is_active": bool(row["is_active"]),
                    "api_key_masked": mask_api_key(row["api_key"]),
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "total_credits": 0.0,
                    "total_usage": 0.0,
                    "remaining": 0.0,
                    "status": "skipped" if not row["is_active"] else "ok",
                    "error": None
                }

                if row["is_active"]:
                    try:
                        response = await client.get(
                            "https://openrouter.ai/api/v1/credits",
                            headers={"Authorization": f"Bearer {row['api_key']}"}
                        )
                        response.raise_for_status()
                        credits_data = normalize_openrouter_credits_payload(response.json())
                        record.update(credits_data)
                    except httpx.HTTPError as ex:
                        record["status"] = "error"
                        record["error"] = str(ex)
                    except Exception as ex:
                        record["status"] = "error"
                        record["error"] = str(ex)

                records.append(record)

        return {"records": records}
    except Exception as e:
        logger.error(f"Error getting OpenRouter key credits: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/openrouter/keys/{record_id}")
async def update_openrouter_key(record_id: int, data: Dict[str, Any] = Body(...)):
    email = (data.get("email") or "").strip()
    label = (data.get("label") or "").strip() or None
    api_key_raw = data.get("api_key")
    api_key = api_key_raw.strip() if isinstance(api_key_raw, str) else None
    is_active = bool(data.get("is_active", True))

    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                if api_key:
                    await cur.execute("""
                        UPDATE sn71_openrouter_key
                        SET email = %s,
                            label = %s,
                            api_key = %s,
                            is_active = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING id, email, api_key, label, is_active, created_at, updated_at
                    """, (email, label, api_key, is_active, record_id))
                else:
                    await cur.execute("""
                        UPDATE sn71_openrouter_key
                        SET email = %s,
                            label = %s,
                            is_active = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING id, email, api_key, label, is_active, created_at, updated_at
                    """, (email, label, is_active, record_id))

                row = await cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="OpenRouter key not found")
                await conn.commit()

        return {
            "message": "OpenRouter key updated successfully",
            "record": {
                "id": row["id"],
                "email": row["email"],
                "label": row["label"],
                "is_active": bool(row["is_active"]),
                "api_key_masked": mask_api_key(row["api_key"]),
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating OpenRouter key {record_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/openrouter/keys/{record_id}")
async def delete_openrouter_key(record_id: int):
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    DELETE FROM sn71_openrouter_key
                    WHERE id = %s
                    RETURNING id
                """, (record_id,))
                row = await cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="OpenRouter key not found")
                await conn.commit()
        return {"message": "OpenRouter key deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting OpenRouter key {record_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/openrouter/credits")
async def get_openrouter_credits():
    """Get OpenRouter.ai API credits information"""
    try:
        import httpx

        api_key = os.getenv("OPENROUTER_API_KEY", "")
        if not api_key:
            raise HTTPException(status_code=500, detail="OPENROUTER_API_KEY environment variable not set")

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                "https://openrouter.ai/api/v1/credits",
                headers={"Authorization": f"Bearer {api_key}"}
            )
            response.raise_for_status()
            data = response.json()

        credits = data.get("data", {})
        total_credits = credits.get("total_credits", 0)
        total_usage = credits.get("total_usage", 0)
        remaining = total_credits - total_usage

        return {
            "total_credits": total_credits,
            "total_usage": total_usage,
            "remaining": remaining
        }
    except httpx.HTTPError as e:
        logger.error(f"Error fetching OpenRouter credits: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch credits: {str(e)}")
    except Exception as e:
        logger.error(f"Error getting OpenRouter credits: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ScrapingDog API Credits ====================

@app.get("/api/scrapingdog/credits")
async def get_scrapingdog_credits():
    """Get ScrapingDog API credits information"""
    try:
        import httpx

        api_key = os.getenv("SCRAPINGDOG_API_KEY", "")
        if not api_key:
            raise HTTPException(status_code=500, detail="SCRAPINGDOG_API_KEY environment variable not set")

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                f"https://api.scrapingdog.com/account?api_key={api_key}"
            )
            response.raise_for_status()
            data = response.json()

        request_limit = data.get("requestLimit", 0)
        request_used = data.get("requestUsed", 0)
        remaining = request_limit - request_used

        return {
            "request_limit": request_limit,
            "request_used": request_used,
            "remaining": remaining,
            "raw_data": data
        }
    except httpx.HTTPError as e:
        logger.error(f"Error fetching ScrapingDog credits: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch credits: {str(e)}")
    except Exception as e:
        logger.error(f"Error getting ScrapingDog credits: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==================== SN71 Submissions API ====================

@app.get("/api/submissions")
async def get_submissions():
    """Get all submission data from sn71_submission table"""
    try:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT hotkey, submissions, max_submissions, rejections, 
                           max_rejections, reset_at
                    FROM sn71_submission
                """)
                rows = await cur.fetchall()

                submissions_dict = {}
                for row in rows:
                    submissions_dict[row['hotkey']] = {
                        'submissions': row['submissions'],
                        'max_submissions': row['max_submissions'],
                        'rejections': row['rejections'],
                        'max_rejections': row['max_rejections'],
                        'reset_at': row['reset_at'].isoformat() if row['reset_at'] else None
                    }

                return {"submissions": submissions_dict}
    except Exception as e:
        logger.error(f"Error getting submissions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/lead-search/evf/{uid}")
async def get_evf_count(uid: int):
    """Fetch EVF (Email Verification Failed) entries for a miner uid from subnet71 API."""
    import httpx
    async with httpx.AsyncClient(timeout=200) as client:
        resp = await client.get(
            f"https://www.subnet71.com/api/lead-search?uid={uid}&limit=1000"
        )
        resp.raise_for_status()
        raw_data = resp.json()
        data = raw_data.get("results", [])
    if not isinstance(data, list):
        return {"count": 0, "email_hashes": []}
    evf_entries = [e for e in data if isinstance(e, dict) and e.get("rejectionReason") == "Email Verification Failed"]
    email_hashes = [e["emailHash"] for e in evf_entries if e.get("emailHash")]
    seen_1010_count = 0
    if email_hashes:
        async with DB_POOL.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT COUNT(*) AS cnt FROM sn71_person WHERE department = ANY(%s::text[]) AND seen = 1010",
                    (email_hashes,)
                )
                row = await cur.fetchone()
                seen_1010_count = row["cnt"] if row else 0
    return {"count": len(email_hashes), "email_hashes": email_hashes, "seen_1010_count": seen_1010_count}


class MarkEvfRequest(BaseModel):
    email_hashes: list[str]


@app.post("/api/lead-search/mark-evf")
async def mark_evf(req: MarkEvfRequest):
    """Set seen=308 on sn71_person rows where department matches the provided email hashes."""
    if not req.email_hashes:
        return {"updated_count": 0}
    async with DB_POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE sn71_person SET seen = 308 WHERE department = ANY(%s::text[])",
                (req.email_hashes,)
            )
            updated = cur.rowcount
        await conn.commit()
    return {"updated_count": updated}


# ==================== Lead Rejection Reason Scheduler ====================

# Thread pool for blocking bittensor metagraph calls
_METAGRAPH_EXECUTOR = ThreadPoolExecutor(max_workers=1)


async def _sync_lead_rejection_reasons():
    """For each of my miners, fetch lead-search results and save rejectionReason
    to sn71_person.person_location_results (only where currently NULL).
    Only entries with a non-null rejectionReason are processed.
    """
    raw = os.getenv("MY_COLDKEYS", "").strip()
    if not raw:
        logger.warning("MY_COLDKEYS env var not set; skipping rejection reason sync")
        return

    coldkeys = [ck.strip() for ck in raw.split(",") if ck.strip()]
    if not coldkeys:
        logger.warning("MY_COLDKEYS is set but contains no valid entries; skipping")
        return

    print(f"Starting lead rejection reason sync for {len(coldkeys)} coldkey(s)...")

    # Load metagraph once and collect UIDs for all coldkeys
    def _get_all_uids_blocking():
        from bittensor.core.metagraph import Metagraph
        metagraph = Metagraph(netuid=71, network="finney", lite=True, sync=True)
        coldkey_set = set(coldkeys)
        uids = []
        for i in range(len(metagraph.uids)):
            if metagraph.coldkeys[i] in coldkey_set:
                uid = int(metagraph.uids[i].item()) if hasattr(metagraph.uids[i], "item") else int(metagraph.uids[i])
                uids.append(uid)
        print(f"💥💥💥: {uids}")
        return uids

    try:
        loop = asyncio.get_event_loop()
        uids = await loop.run_in_executor(_METAGRAPH_EXECUTOR, _get_all_uids_blocking)
    except Exception as e:
        logger.error(f"Failed to load metagraph for rejection reason sync: {e}\n{traceback.format_exc()}")
        return

    if not uids:
        logger.warning(f"No UIDs found for any of the {len(coldkeys)} coldkey(s); skipping")
        return

    logger.info(f"Rejection reason sync: found {len(uids)} UIDs across {len(coldkeys)} coldkey(s): {uids}")

    total_updated = 0
    async with httpx.AsyncClient(timeout=60) as client:
        for uid in uids:
            try:
                resp = await client.get(
                    f"https://www.subnet71.com/api/lead-search?uid={uid}&limit=1000"
                )
                resp.raise_for_status()
                raw_data = resp.json()
                results = raw_data.get("results", [])

                if not isinstance(results, list):
                    continue

                # Only entries with a non-null rejectionReason and a valid emailHash
                entries = [
                    e for e in results
                    if isinstance(e, dict)
                    and e.get("rejectionReason") is not None
                    and e.get("rejectionReason") != "null"
                    and e.get("emailHash")
                ]

                if not entries:
                    logger.info(f"UID {uid}: no entries with rejectionReason")
                    continue

                hash_to_reason = {e["emailHash"]: e["rejectionReason"] for e in entries}
                logger.info(f"🍖🍖🍖: {uid}")
                uid_updated = 0
                async with DB_POOL.connection() as conn:
                    async with conn.cursor() as cur:
                        for email_hash, reason in hash_to_reason.items():
                            await cur.execute(
                                """
                                UPDATE sn71_person
                                SET person_location_results = %s
                                WHERE department = %s
                                  AND person_location_results IS NULL
                                """,
                                (reason, email_hash),
                            )
                            uid_updated += cur.rowcount
                    await conn.commit()

                total_updated += uid_updated
                logger.info(f"UID {uid}: {len(entries)} entries with rejectionReason, {uid_updated} rows updated")

            except Exception as e:
                logger.error(f"Error syncing rejection reasons for UID {uid}: {e}")
                continue

    logger.info(f"Lead rejection reason sync complete. Total rows updated: {total_updated}")


async def _rejection_reason_scheduler():
    """Run _sync_lead_rejection_reasons() twice a day (every 12 hours).
    Waits 60 seconds after startup before the first run.
    """
    await asyncio.sleep(60)
    while True:
        try:
            await _sync_lead_rejection_reasons()
        except Exception as e:
            logger.error(f"Unhandled error in rejection reason scheduler: {e}\n{traceback.format_exc()}")
        await asyncio.sleep(12 * 60 * 60)  # 12 hours


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9900)
