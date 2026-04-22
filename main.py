import asyncio
import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from database import init_db, get_db, db_has_data
from scheduler import create_scheduler, startup_refresh

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PE Funds API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_scheduler = None


@app.on_event("startup")
async def on_startup():
    global _scheduler
    init_db()

    _scheduler = create_scheduler()
    _scheduler.start()

    was_empty = not db_has_data()
    asyncio.create_task(startup_refresh())

    if was_empty:
        logger.info("Server ready — data loading in background")


@app.on_event("shutdown")
async def on_shutdown():
    if _scheduler:
        _scheduler.shutdown(wait=False)


# ---------------------------------------------------------------------------
# GET /funds
# ---------------------------------------------------------------------------

@app.get("/funds")
async def list_funds(
    search: Optional[str] = None,
    fund_type: Optional[str] = None,
    fund_jurisdiction: Optional[str] = None,
    adviser_state: Optional[str] = None,
    open_only: bool = False,
    min_aum: Optional[float] = None,
    max_aum: Optional[float] = None,
    min_investment_max: Optional[float] = None,
    sort_by: str = "gross_asset_value",
    sort_dir: str = "desc",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
):
    allowed_sort_columns = {
        "gross_asset_value", "fund_name", "adviser_name", "fund_formation_date",
        "fund_filing_date", "min_investment_usd", "beneficial_owner_count",
        "adviser_regulatory_aum", "last_updated",
    }
    if sort_by not in allowed_sort_columns:
        sort_by = "gross_asset_value"
    sort_dir = "ASC" if sort_dir.lower() == "asc" else "DESC"

    params: list = []
    where_clauses: list[str] = []

    if search:
        from_clause = "funds f JOIN funds_fts fts ON f.rowid = fts.rowid"
        where_clauses.append("funds_fts MATCH ?")
        params.append(search)
    else:
        from_clause = "funds f"

    if fund_type:
        where_clauses.append("f.fund_type = ?")
        params.append(fund_type)
    if fund_jurisdiction:
        where_clauses.append("f.fund_jurisdiction = ?")
        params.append(fund_jurisdiction)
    if adviser_state:
        where_clauses.append("f.adviser_state = ?")
        params.append(adviser_state)
    if open_only:
        where_clauses.append("f.open_to_new_investment = 1")
    if min_aum is not None:
        where_clauses.append("f.gross_asset_value >= ?")
        params.append(min_aum)
    if max_aum is not None:
        where_clauses.append("f.gross_asset_value <= ?")
        params.append(max_aum)
    if min_investment_max is not None:
        where_clauses.append("f.min_investment_usd <= ?")
        params.append(min_investment_max)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    offset = (page - 1) * page_size

    count_sql = f"SELECT COUNT(*) as cnt FROM {from_clause} {where_sql}"
    data_sql = (
        f"SELECT f.* FROM {from_clause} {where_sql} "
        f"ORDER BY f.{sort_by} {sort_dir} "
        f"LIMIT ? OFFSET ?"
    )

    con = get_db()
    try:
        total = con.execute(count_sql, params).fetchone()["cnt"]
        rows = con.execute(data_sql, params + [page_size, offset]).fetchall()
        results = [dict(r) for r in rows]
    finally:
        con.close()

    return {"total": total, "page": page, "page_size": page_size, "results": results}


# ---------------------------------------------------------------------------
# GET /funds/{fund_id}
# ---------------------------------------------------------------------------

@app.get("/funds/{fund_id}")
async def get_fund(fund_id: str):
    con = get_db()
    try:
        row = con.execute("SELECT * FROM funds WHERE fund_id = ?", (fund_id,)).fetchone()
    finally:
        con.close()

    if row is None:
        raise HTTPException(status_code=404, detail="Fund not found")

    return dict(row)


# ---------------------------------------------------------------------------
# GET /stats
# ---------------------------------------------------------------------------

@app.get("/stats")
async def get_stats():
    con = get_db()
    try:
        fund_type_counts = [
            dict(r) for r in con.execute(
                "SELECT fund_type, COUNT(*) as count FROM funds GROUP BY fund_type"
            ).fetchall()
        ]
        aum_by_type = [
            dict(r) for r in con.execute(
                "SELECT fund_type, SUM(gross_asset_value) as total_aum FROM funds GROUP BY fund_type"
            ).fetchall()
        ]
        formations_by_year = [
            dict(r) for r in con.execute(
                "SELECT strftime('%Y', fund_formation_date) as year, COUNT(*) as count "
                "FROM funds GROUP BY year ORDER BY year"
            ).fetchall()
        ]
        aum_by_state = [
            dict(r) for r in con.execute(
                "SELECT adviser_state, SUM(gross_asset_value) as total_aum, COUNT(*) as fund_count "
                "FROM funds GROUP BY adviser_state ORDER BY total_aum DESC LIMIT 20"
            ).fetchall()
        ]
        open_vs_closed = [
            dict(r) for r in con.execute(
                "SELECT open_to_new_investment, COUNT(*) as count FROM funds GROUP BY open_to_new_investment"
            ).fetchall()
        ]
        total_funds = con.execute("SELECT COUNT(*) as n FROM funds").fetchone()["n"]
        total_aum = con.execute("SELECT SUM(gross_asset_value) as s FROM funds").fetchone()["s"]
        last_updated = con.execute("SELECT MAX(last_updated) as lu FROM funds").fetchone()["lu"]
    finally:
        con.close()

    return {
        "fund_type_counts": fund_type_counts,
        "aum_by_type": aum_by_type,
        "formations_by_year": formations_by_year,
        "aum_by_state": aum_by_state,
        "open_vs_closed": open_vs_closed,
        "total_funds": total_funds,
        "total_aum": total_aum,
        "last_updated": last_updated,
    }


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    con = get_db()
    try:
        fund_count = con.execute("SELECT COUNT(*) as n FROM funds").fetchone()["n"]
        last_updated = con.execute("SELECT MAX(last_updated) as lu FROM funds").fetchone()["lu"]
    finally:
        con.close()

    db_size_mb = 0.0
    try:
        db_size_mb = os.path.getsize("pe_funds.db") / 1024 / 1024
    except OSError:
        pass

    return {
        "status": "ok",
        "fund_count": fund_count,
        "last_updated": last_updated,
        "db_size_mb": round(db_size_mb, 3),
    }


# ---------------------------------------------------------------------------
# GET /meta/jurisdictions
# ---------------------------------------------------------------------------

@app.get("/meta/jurisdictions")
async def meta_jurisdictions():
    con = get_db()
    try:
        rows = con.execute(
            "SELECT DISTINCT fund_jurisdiction FROM funds "
            "WHERE fund_jurisdiction IS NOT NULL ORDER BY fund_jurisdiction"
        ).fetchall()
    finally:
        con.close()
    return [r["fund_jurisdiction"] for r in rows]


# ---------------------------------------------------------------------------
# GET /meta/states
# ---------------------------------------------------------------------------

@app.get("/meta/states")
async def meta_states():
    con = get_db()
    try:
        rows = con.execute(
            "SELECT DISTINCT adviser_state FROM funds "
            "WHERE adviser_state IS NOT NULL ORDER BY adviser_state"
        ).fetchall()
    finally:
        con.close()
    return [r["adviser_state"] for r in rows]
