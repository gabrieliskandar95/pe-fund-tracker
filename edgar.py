import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import httpx

from database import upsert_funds

logger = logging.getLogger(__name__)

CUTOFF_DATE = "2018-01-01"
_EDGAR_BASE = "https://www.sec.gov"
_EFTS_BASE = "https://efts.sec.gov/LATEST/search-index"
_TIMEOUT = httpx.Timeout(30.0)
_RATE_DELAY = 0.15
_BATCH_SIZE = 100
_HEADERS = {"User-Agent": "pe-fund-tracker gabrieliskandar@gmail.com"}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def _get(client: httpx.AsyncClient, url: str, **params) -> dict:
    await asyncio.sleep(_RATE_DELAY)
    r = await client.get(url, params=params, timeout=_TIMEOUT, headers=_HEADERS)
    r.raise_for_status()
    return r.json()


async def _get_bytes(client: httpx.AsyncClient, url: str) -> bytes:
    await asyncio.sleep(_RATE_DELAY)
    r = await client.get(url, timeout=_TIMEOUT, headers=_HEADERS)
    r.raise_for_status()
    return r.content


def _safe_float(val) -> float | None:
    try:
        return float(str(val).replace(",", "")) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(str(val).replace(",", "")) if val is not None else None
    except (TypeError, ValueError):
        return None


def _bool_int(val) -> int | None:
    if val is None:
        return None
    return 1 if str(val).strip().upper() in ("Y", "YES", "TRUE", "1") else 0


# ---------------------------------------------------------------------------
# Form D XML parser
# ---------------------------------------------------------------------------

def _parse_form_d(xml_bytes: bytes, cik: str, accession: str) -> list[dict]:
    try:
        xml_str = xml_bytes.decode("utf-8", errors="replace")
        logger.info("Form D XML sample CIK %s: %s", cik, xml_str[:600].replace("\n", " "))
        root = ET.fromstring(xml_str)
    except Exception as e:
        logger.warning("XML parse error CIK %s: %s", cik, e)
        return []

    def strip_ns(tag):
        return tag.split("}")[-1] if "}" in tag else tag

    def get_text(parent, *tags):
        for tag in tags:
            for el in parent.iter():
                if strip_ns(el.tag).lower() == tag.lower() and el.text:
                    return el.text.strip()
        return ""

    # Confirm it's a PE fund
    fund_type = get_text(root, "pooledFundType")
    if "private equity" not in fund_type.lower():
        logger.info("Skipping CIK %s — fund type: %s", cik, fund_type)
        return []

    # Date of first sale
    formation_date = get_text(root, "dateOfFirstSale")
    if formation_date and formation_date < CUTOFF_DATE:
        return []

    fund_name = get_text(root, "issuerName")
    jurisdiction = get_text(root, "jurisdictionOfInc", "stateOrCountry")
    total_offering = _safe_float(get_text(root, "totalOfferingAmount"))
    total_sold = _safe_float(get_text(root, "totalAmountSold"))
    min_investment = _safe_float(get_text(root, "minimumInvestmentAccepted"))
    num_investors = _safe_int(get_text(root, "totalNumberAlreadyInvested"))
    is_fund_of_funds = _bool_int(get_text(root, "isFundOfFunds"))

    # Related persons (GP / adviser)
    adviser_name = ""
    for el in root.iter():
        if strip_ns(el.tag).lower() == "relatedpersoninfo":
            rel_name = get_text(el, "relatedPersonName")
            rel_type = get_text(el, "relatedPersonRelationshipList")
            if rel_name and ("managing" in rel_type.lower() or "general" in rel_type.lower() or "control" in rel_type.lower()):
                adviser_name = rel_name
                break

    filing_date = get_text(root, "submissionType")  # fallback
    for el in root.iter():
        if strip_ns(el.tag).lower() in ("periodofannotation", "dateofreport", "filingdate"):
            if el.text:
                filing_date = el.text.strip()
                break

    return [{
        "fund_id": f"{cik}-{accession}",
        "sec_file_number": accession,
        "crd_number": "",
        "cik_number": cik,
        "lei": "",
        "fund_name": fund_name,
        "fund_type": "Private Equity Fund",
        "fund_jurisdiction": jurisdiction,
        "fund_formation_date": formation_date,
        "fund_filing_date": filing_date,
        "gross_asset_value": total_sold or total_offering,
        "min_investment_usd": min_investment,
        "beneficial_owner_count": num_investors,
        "us_persons_pct": None,
        "inv_company_act_exemption": get_text(root, "federalExemptionsExclusions"),
        "is_master_fund": None,
        "is_feeder_fund": None,
        "is_fund_of_funds": is_fund_of_funds,
        "open_to_new_investment": None,
        "relied_on_reg_d": 1,
        "is_audited": None,
        "auditor_pcaob_registered": None,
        "auditor_name": "",
        "custodian_name": "",
        "has_third_party_admin": None,
        "pct_externally_valued": None,
        "adviser_name": adviser_name,
        "adviser_business_name": "",
        "adviser_state": "",
        "adviser_org_type": "",
        "adviser_website": "",
        "is_exempt_reporting_adviser": None,
        "adviser_regulatory_aum": None,
        "adviser_total_employees": None,
        "adviser_investment_employees": None,
        "adviser_num_funds": None,
        "adviser_charges_performance_fees": None,
        "adviser_has_disclosures": None,
        "adviser_cco_name": "",
        "last_updated": date.today().isoformat(),
    }]


# ---------------------------------------------------------------------------
# Fetch Form D XML from EDGAR
# ---------------------------------------------------------------------------

async def _fetch_form_d(client: httpx.AsyncClient, cik: str, accession: str) -> list[dict]:
    acc_nodash = accession.replace("-", "")
    # Try filing index first to find the primary XML
    try:
        idx = await _get(client, f"{_EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_nodash}/{accession}-index.json")
        files = idx.get("directory", {}).get("item", [])
        xml_files = [f["name"] for f in files if f.get("name", "").lower().endswith(".xml")]
    except Exception:
        xml_files = ["primary-document.xml"]

    for xml_name in xml_files[:3]:
        try:
            url = f"{_EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_nodash}/{xml_name}"
            xml_bytes = await _get_bytes(client, url)
            records = _parse_form_d(xml_bytes, cik, accession)
            if records is not None:
                return records
        except Exception:
            continue
    return []


# ---------------------------------------------------------------------------
# EFTS search for Form D PE fund filings
# ---------------------------------------------------------------------------

async def fetch_fund_filings() -> list[dict]:
    """Search EDGAR full-text for Form D filings mentioning 'private equity fund'."""
    results = []
    seen = set()
    from_offset = 0
    page_size = 50

    async with httpx.AsyncClient() as client:
        while True:
            data = await _get(
                client,
                _EFTS_BASE,
                q='"private equity fund"',
                forms="D,D/A",
                dateRange="custom",
                startdt=CUTOFF_DATE,
                **{"from": from_offset},
            )

            hits_wrapper = data.get("hits", {})
            total_raw = hits_wrapper.get("total", 0)
            total = total_raw if isinstance(total_raw, int) else total_raw.get("value", 0)
            hits = hits_wrapper.get("hits", [])

            if from_offset == 0:
                logger.info("EFTS Form D PE search: %d total hits", total)
                if hits:
                    logger.info("First hit sample: %s", str(hits[0].get("_source", {}))[:300])

            if not hits:
                break

            for hit in hits:
                accession = hit.get("_id", "")
                src = hit.get("_source", {})
                entity_name = src.get("entity_name", "")
                # CIK is the first 10 digits of the accession number
                raw_cik = accession.replace("-", "")[:10].lstrip("0")

                if not raw_cik or raw_cik in seen:
                    continue
                seen.add(raw_cik)
                results.append({
                    "cik": raw_cik,
                    "accession": accession,
                    "entity_name": entity_name,
                })

            from_offset += page_size
            if from_offset >= total or from_offset > 5000:
                break

    logger.info("Found %d unique PE fund Form D filers", len(results))
    return results


# ---------------------------------------------------------------------------
# Refresh entrypoints
# ---------------------------------------------------------------------------

async def run_full_refresh() -> int:
    filings = await fetch_fund_filings()
    logger.info("Starting full refresh — %d Form D filings to process", len(filings))

    total_saved = 0
    buffer = []

    async with httpx.AsyncClient() as client:
        for i, filing in enumerate(filings):
            try:
                records = await _fetch_form_d(client, filing["cik"], filing["accession"])
                buffer.extend(records)
                if len(buffer) >= _BATCH_SIZE:
                    upsert_funds(buffer)
                    total_saved += len(buffer)
                    logger.info("Saved %d funds so far (%d/%d processed)", total_saved, i + 1, len(filings))
                    buffer = []
            except Exception as exc:
                logger.error("Error fetching %s: %s", filing["accession"], exc)

    if buffer:
        upsert_funds(buffer)
        total_saved += len(buffer)

    logger.info("Full refresh complete — %d funds loaded", total_saved)
    return total_saved


async def run_incremental_refresh() -> int:
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    seen: set[str] = set()
    filings: list[dict] = []

    async with httpx.AsyncClient() as client:
        data = await _get(
            client,
            _EFTS_BASE,
            q='"private equity fund"',
            forms="D,D/A",
            dateRange="custom",
            startdt=yesterday,
            **{"from": 0},
        )
        hits = data.get("hits", {}).get("hits", [])
        for hit in hits:
            accession = hit.get("_id", "")
            raw_cik = accession.replace("-", "")[:10].lstrip("0")
            if raw_cik and raw_cik not in seen:
                seen.add(raw_cik)
                filings.append({
                    "cik": raw_cik,
                    "accession": accession,
                    "entity_name": hit.get("_source", {}).get("entity_name", ""),
                })

    logger.info("Starting incremental refresh — %d new Form D filings on %s", len(filings), yesterday)

    total_saved = 0
    buffer = []

    async with httpx.AsyncClient() as client:
        for filing in filings:
            try:
                records = await _fetch_form_d(client, filing["cik"], filing["accession"])
                buffer.extend(records)
                if len(buffer) >= _BATCH_SIZE:
                    upsert_funds(buffer)
                    total_saved += len(buffer)
                    buffer = []
            except Exception as exc:
                logger.error("Error fetching %s: %s", filing["accession"], exc)

    if buffer:
        upsert_funds(buffer)
        total_saved += len(buffer)

    logger.info("Incremental refresh complete — %d funds upserted", total_saved)
    return total_saved
