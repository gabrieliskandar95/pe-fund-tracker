import asyncio
import gzip
import logging
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import httpx

from database import upsert_funds

logger = logging.getLogger(__name__)

CUTOFF_DATE = "2018-01-01"
_EDGAR_BASE = "https://www.sec.gov"
_DATA_BASE = "https://data.sec.gov"
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


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

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
# ADV XML parser
# ---------------------------------------------------------------------------

def _parse_adv_xml(xml_bytes: bytes, cik: str) -> list[dict]:
    try:
        xml_str = xml_bytes.decode("utf-8", errors="replace")
        logger.info("ADV XML sample CIK %s: %s", cik, xml_str[:800].replace("\n", " "))
        root = ET.fromstring(xml_str)
    except Exception as e:
        logger.warning("XML parse error CIK %s: %s", cik, e)
        return []

    def strip_ns(tag):
        return tag.split("}")[-1] if "}" in tag else tag

    def find_all(parent, *tags):
        for tag in tags:
            found = [el for el in parent.iter() if strip_ns(el.tag).lower() == tag.lower()]
            if found:
                return found
        return []

    def get_text(el, *tags):
        for tag in tags:
            els = [e for e in el.iter() if strip_ns(e.tag).lower() == tag.lower()]
            if els and els[0].text:
                return els[0].text.strip()
        return ""

    fund_els = find_all(root, "PrivateFund", "privateFund", "fund", "FundInfo")

    if not fund_els:
        all_tags = sorted(set(strip_ns(el.tag) for el in root.iter()))
        logger.info("No fund elements for CIK %s. Tags: %s", cik, str(all_tags)[:400])
        return []

    logger.info("Found %d fund elements for CIK %s", len(fund_els), cik)

    adviser_name = get_text(root, "FirmName", "firmName", "legalName")
    adviser_state = get_text(root, "StateCode", "stateCode", "state")
    adviser_aum = _safe_float(get_text(root, "RegAUM", "regulatoryAUM", "totalRegAUM"))
    sec_file_num = get_text(root, "SECFileNumber", "secFileNumber", "FileNumber", "fileNumber")

    records = []
    for fund_el in fund_els:
        fund_type = get_text(fund_el, "FundType", "fundType", "type", "TypeOfFund")
        if "private equity" not in fund_type.lower():
            continue

        formation_date = get_text(fund_el, "FundFormationDate", "formationDate", "inceptionDate")
        if formation_date and formation_date < CUTOFF_DATE:
            continue

        fund_id = get_text(fund_el, "FundId", "fundId", "PrivateFundID", "privateFundId", "id")

        records.append({
            "fund_id": fund_id or f"{cik}-{len(records)}",
            "sec_file_number": sec_file_num or f"cik:{cik}",
            "crd_number": "",
            "cik_number": cik,
            "lei": get_text(fund_el, "LEI", "lei"),
            "fund_name": get_text(fund_el, "FundName", "fundName", "name"),
            "fund_type": fund_type,
            "fund_jurisdiction": get_text(fund_el, "FundJurisdiction", "fundJurisdiction", "domicileCountry", "jurisdiction"),
            "fund_formation_date": formation_date,
            "fund_filing_date": get_text(root, "FilingDate", "filingDate", "DateFiled"),
            "gross_asset_value": _safe_float(get_text(fund_el, "GrossAssetValue", "grossAssetValue", "totalGrossAssets")),
            "min_investment_usd": _safe_float(get_text(fund_el, "MinimumInvestmentAmount", "minimumInvestmentAmount")),
            "beneficial_owner_count": _safe_int(get_text(fund_el, "BeneficialOwnerCount", "beneficialOwnerCount")),
            "us_persons_pct": _safe_float(get_text(fund_el, "USPersonsPct", "usPersonsPct")),
            "inv_company_act_exemption": get_text(fund_el, "InvestmentCompanyActExemption", "icaExemption"),
            "is_master_fund": _bool_int(get_text(fund_el, "IsMasterFund", "masterFund")),
            "is_feeder_fund": _bool_int(get_text(fund_el, "IsFeederFund", "feederFund")),
            "is_fund_of_funds": _bool_int(get_text(fund_el, "IsFundOfFunds", "fundOfFunds")),
            "open_to_new_investment": _bool_int(get_text(fund_el, "OpenToNewInvestment", "openToNewInvestment")),
            "relied_on_reg_d": _bool_int(get_text(fund_el, "ReliedOnRegD")),
            "is_audited": _bool_int(get_text(fund_el, "IsAudited", "audited")),
            "auditor_pcaob_registered": _bool_int(get_text(fund_el, "AuditorPCAOBRegistered")),
            "auditor_name": get_text(fund_el, "AuditorName", "auditorFirmName"),
            "custodian_name": get_text(fund_el, "CustodianName"),
            "has_third_party_admin": _bool_int(get_text(fund_el, "HasThirdPartyAdmin")),
            "pct_externally_valued": _safe_float(get_text(fund_el, "PctExternallyValued")),
            "adviser_name": adviser_name,
            "adviser_business_name": "",
            "adviser_state": adviser_state,
            "adviser_org_type": get_text(root, "OrganizationType", "organizationType"),
            "adviser_website": get_text(root, "WebsiteAddress", "website"),
            "is_exempt_reporting_adviser": _bool_int(get_text(root, "IsExemptReportingAdviser")),
            "adviser_regulatory_aum": adviser_aum,
            "adviser_total_employees": _safe_int(get_text(root, "TotalEmployees", "totalEmployees")),
            "adviser_investment_employees": _safe_int(get_text(root, "InvestmentAdvisoryEmployees")),
            "adviser_num_funds": len(fund_els),
            "adviser_charges_performance_fees": _bool_int(get_text(root, "ChargesPerformanceFees")),
            "adviser_has_disclosures": None,
            "adviser_cco_name": get_text(root, "CCOName", "ccoName"),
            "last_updated": date.today().isoformat(),
        })

    return records


# ---------------------------------------------------------------------------
# EDGAR per-CIK fetcher
# ---------------------------------------------------------------------------

async def _get_adv_funds_for_cik(client: httpx.AsyncClient, cik: str) -> list[dict]:
    cik_padded = cik.zfill(10)
    try:
        subs = await _get(client, f"{_DATA_BASE}/submissions/CIK{cik_padded}.json")
        recent = subs.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])

        adv_accession = None
        for form, acc in zip(forms, accessions):
            if form.strip().upper() in ("ADV", "ADV/A"):
                adv_accession = acc
                break

        if not adv_accession:
            return []

        acc_nodash = adv_accession.replace("-", "")
        idx_url = f"{_EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_nodash}/{adv_accession}-index.json"

        try:
            idx = await _get(client, idx_url)
            files = idx.get("directory", {}).get("item", [])
            xml_files = [f["name"] for f in files if f.get("name", "").lower().endswith(".xml")]
        except Exception:
            xml_files = ["primary-document.xml"]

        for xml_name in xml_files[:5]:
            xml_url = f"{_EDGAR_BASE}/Archives/edgar/data/{cik}/{acc_nodash}/{xml_name}"
            try:
                xml_bytes = await _get_bytes(client, xml_url)
                records = _parse_adv_xml(xml_bytes, cik)
                if records:
                    return records
            except Exception:
                continue

        return []

    except httpx.HTTPStatusError as e:
        if e.response.status_code != 404:
            logger.error("HTTP %s for CIK %s", e.response.status_code, cik)
        return []
    except Exception as e:
        logger.error("Error fetching CIK %s: %s", cik, e)
        return []


# ---------------------------------------------------------------------------
# EDGAR quarterly index
# ---------------------------------------------------------------------------

async def fetch_all_adviser_ciks() -> list[dict]:
    results = []
    seen = set()
    today = date.today()
    quarters_done = set()

    async with httpx.AsyncClient() as client:
        for month_offset in range(0, 24, 3):
            mo = today.month - month_offset
            yr = today.year
            while mo <= 0:
                mo += 12
                yr -= 1
            qtr = (mo - 1) // 3 + 1
            if (yr, qtr) in quarters_done:
                continue
            quarters_done.add((yr, qtr))

            url = f"{_EDGAR_BASE}/Archives/edgar/full-index/{yr}/QTR{qtr}/company.gz"
            try:
                r = await client.get(url, timeout=_TIMEOUT, headers=_HEADERS)
                r.raise_for_status()
                content = gzip.decompress(r.content).decode("latin-1")
                before = len(results)
                for line in content.split("\n"):
                    if len(line) < 75:
                        continue
                    form_type = line[62:74].strip().upper()
                    if form_type not in ("ADV", "ADV/A"):
                        continue
                    cik = line[74:86].strip()
                    name = line[:62].strip()
                    if cik and cik not in seen:
                        seen.add(cik)
                        results.append({"sec_file_number": cik, "adviser_name": name})
                logger.info("EDGAR %d QTR%d: %d new ADV filers (total %d)", yr, qtr, len(results) - before, len(results))
            except Exception as e:
                logger.error("Error fetching EDGAR index %d QTR%d: %s", yr, qtr, e)

    return results


# ---------------------------------------------------------------------------
# Refresh entrypoints
# ---------------------------------------------------------------------------

async def run_full_refresh() -> int:
    advisers = await fetch_all_adviser_ciks()
    logger.info("Starting full refresh — %d advisers to process", len(advisers))

    total_saved = 0
    buffer = []

    async with httpx.AsyncClient() as client:
        for i, adviser in enumerate(advisers):
            cik = adviser["sec_file_number"]
            try:
                records = await _get_adv_funds_for_cik(client, cik)
                buffer.extend(records)
                if len(buffer) >= _BATCH_SIZE:
                    upsert_funds(buffer)
                    total_saved += len(buffer)
                    logger.info("Saved %d records so far (%d/%d advisers)", total_saved, i + 1, len(advisers))
                    buffer = []
            except Exception as exc:
                logger.error("Error fetching adviser %s (%s): %s", cik, adviser.get("adviser_name", ""), exc)

    if buffer:
        upsert_funds(buffer)
        total_saved += len(buffer)

    logger.info("Full refresh complete — %d funds loaded", total_saved)
    return total_saved


async def run_incremental_refresh() -> int:
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    yr, mo = int(yesterday[:4]), int(yesterday[5:7])
    qtr = (mo - 1) // 3 + 1

    seen: set[str] = set()
    advisers: list[dict] = []

    async with httpx.AsyncClient() as client:
        url = f"{_EDGAR_BASE}/Archives/edgar/full-index/{yr}/QTR{qtr}/company.gz"
        try:
            r = await client.get(url, timeout=_TIMEOUT, headers=_HEADERS)
            r.raise_for_status()
            content = gzip.decompress(r.content).decode("latin-1")
            for line in content.split("\n"):
                if len(line) < 98:
                    continue
                form_type = line[62:74].strip().upper()
                if form_type not in ("ADV", "ADV/A"):
                    continue
                date_filed = line[86:98].strip()
                if date_filed != yesterday:
                    continue
                cik = line[74:86].strip()
                name = line[:62].strip()
                if cik and cik not in seen:
                    seen.add(cik)
                    advisers.append({"sec_file_number": cik, "adviser_name": name})
        except Exception as e:
            logger.error("Error in incremental refresh index: %s", e)

    logger.info("Starting incremental refresh — %d advisers filed on %s", len(advisers), yesterday)

    total_saved = 0
    buffer = []

    async with httpx.AsyncClient() as client:
        for adviser in advisers:
            cik = adviser["sec_file_number"]
            try:
                records = await _get_adv_funds_for_cik(client, cik)
                buffer.extend(records)
                if len(buffer) >= _BATCH_SIZE:
                    upsert_funds(buffer)
                    total_saved += len(buffer)
                    buffer = []
            except Exception as exc:
                logger.error("Error fetching CIK %s: %s", cik, exc)

    if buffer:
        upsert_funds(buffer)
        total_saved += len(buffer)

    logger.info("Incremental refresh complete — %d funds upserted", total_saved)
    return total_saved
