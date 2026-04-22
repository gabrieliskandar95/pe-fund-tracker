import sqlite3

DB_PATH = "pe_funds.db"


def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS funds (
            fund_id                      TEXT PRIMARY KEY,
            sec_file_number              TEXT,
            crd_number                   TEXT,
            cik_number                   TEXT,
            lei                          TEXT,
            fund_name                    TEXT,
            fund_type                    TEXT,
            fund_jurisdiction            TEXT,
            fund_formation_date          TEXT,
            fund_filing_date             TEXT,
            gross_asset_value            REAL,
            min_investment_usd           REAL,
            beneficial_owner_count       INTEGER,
            us_persons_pct               REAL,
            inv_company_act_exemption    TEXT,
            is_master_fund               INTEGER,
            is_feeder_fund               INTEGER,
            is_fund_of_funds             INTEGER,
            open_to_new_investment       INTEGER,
            relied_on_reg_d              INTEGER,
            is_audited                   INTEGER,
            auditor_pcaob_registered     INTEGER,
            auditor_name                 TEXT,
            custodian_name               TEXT,
            has_third_party_admin        INTEGER,
            pct_externally_valued        REAL,
            adviser_name                 TEXT,
            adviser_business_name        TEXT,
            adviser_state                TEXT,
            adviser_org_type             TEXT,
            adviser_website              TEXT,
            is_exempt_reporting_adviser  INTEGER,
            adviser_regulatory_aum       REAL,
            adviser_total_employees      INTEGER,
            adviser_investment_employees INTEGER,
            adviser_num_funds            INTEGER,
            adviser_charges_performance_fees INTEGER,
            adviser_has_disclosures      INTEGER,
            adviser_cco_name             TEXT,
            last_updated                 TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_fund_type ON funds(fund_type);
        CREATE INDEX IF NOT EXISTS idx_adviser_state ON funds(adviser_state);
        CREATE INDEX IF NOT EXISTS idx_open ON funds(open_to_new_investment);
        CREATE INDEX IF NOT EXISTS idx_aum ON funds(gross_asset_value);
        CREATE INDEX IF NOT EXISTS idx_formation ON funds(fund_formation_date);
        CREATE INDEX IF NOT EXISTS idx_sec_file ON funds(sec_file_number);

        CREATE VIRTUAL TABLE IF NOT EXISTS funds_fts USING fts5(
            fund_id,
            fund_name,
            adviser_name,
            content=funds,
            content_rowid=rowid
        );
    """)

    con.commit()
    con.close()


def upsert_funds(records: list[dict]):
    if not records:
        return

    columns = [
        "fund_id", "sec_file_number", "crd_number", "cik_number", "lei",
        "fund_name", "fund_type", "fund_jurisdiction", "fund_formation_date",
        "fund_filing_date", "gross_asset_value", "min_investment_usd",
        "beneficial_owner_count", "us_persons_pct", "inv_company_act_exemption",
        "is_master_fund", "is_feeder_fund", "is_fund_of_funds",
        "open_to_new_investment", "relied_on_reg_d", "is_audited",
        "auditor_pcaob_registered", "auditor_name", "custodian_name",
        "has_third_party_admin", "pct_externally_valued", "adviser_name",
        "adviser_business_name", "adviser_state", "adviser_org_type",
        "adviser_website", "is_exempt_reporting_adviser", "adviser_regulatory_aum",
        "adviser_total_employees", "adviser_investment_employees",
        "adviser_num_funds", "adviser_charges_performance_fees",
        "adviser_has_disclosures", "adviser_cco_name", "last_updated",
    ]
    placeholders = ", ".join("?" * len(columns))
    col_list = ", ".join(columns)
    sql = f"INSERT OR REPLACE INTO funds ({col_list}) VALUES ({placeholders})"

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        rows = [[rec.get(c) for c in columns] for rec in records]
        cur.executemany(sql, rows)

        # Keep FTS index in sync
        fund_ids = [rec.get("fund_id") for rec in records if rec.get("fund_id")]
        if fund_ids:
            placeholders_ids = ", ".join("?" * len(fund_ids))
            cur.execute(
                f"DELETE FROM funds_fts WHERE fund_id IN ({placeholders_ids})",
                fund_ids,
            )
            cur.execute(
                f"""
                INSERT INTO funds_fts(fund_id, fund_name, adviser_name)
                SELECT fund_id, fund_name, adviser_name
                FROM funds
                WHERE fund_id IN ({placeholders_ids})
                """,
                fund_ids,
            )

        con.commit()
    finally:
        con.close()


def get_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def db_has_data() -> bool:
    con = get_db()
    try:
        row = con.execute("SELECT COUNT(*) AS cnt FROM funds").fetchone()
        return row["cnt"] > 0
    finally:
        con.close()
