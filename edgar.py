import asyncio
import logging
from datetime import date, timedelta

import httpx

from database import upsert_funds

logger = logging.getLogger(__name__)

CUTOFF_DATE = "2018-01-01"

_IAPD_SEARCH = "https://api.adviserinfo.sec.gov/search/firm"
_ADVISER_API = "https://api.adviserinfo.sec.gov/api/Firm"
_TIMEOUT = httpx.Timeout(10.0)
_RATE_DELAY = 0.15
_BATCH_SIZE = 500
_HEADERS = {"User-Agent": "pe-fund-tracker gabrieliskandar@gmail.com"}

_PE_SEARCH_TERMS = ["private equity", "buyout", "growth equity", "venture capital"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

async def _get(client: httpx.AsyncClient, url: str, **params) -> dict:
    await asyncio.sleep(_RATE_DELAY)
    r = await client.get(url, params=params, timeout=_TIMEOUT, headers=_HEADERS)
    r.raise_for_status()
    return r.json()


def _bool_int(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, bool):
        return int(val)
    if isinstance(val, str):
        return 1 if val.lower() in ("y", "yes", "true", "1") else 0
    return int(bool(val))


def _safe_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 1. fetch_all_adviser_ciks
# ---------------------------------------------------------------------------

async def fetch_all_adviser_ciks() -> list[dict]:
    """Return deduplicated [{sec_file_number, adviser_name}] for candidate PE-fund advisers."""
    results: list[dict] = []
    seen: set[str] = set()

    async with httpx.AsyncClient() as client:
        for term in _PE_SEARCH_TERMS:
            rows = 100
            start = 0
            while True:
                data = await _get(
                    client,
                    _IAPD_SEARCH,
                    query=term,
                    rows=rows,
                    start=start,
                )

                hits_wrapper = data.get("hits", {})
                total_raw = hits_wrapper.get("total", 0)
                total = total_raw if isinstance(total_raw, int) else total_raw.get("value", 0)
                hits = hits_wrapper.get("hits", [])

                if not hits:
                    break

                for hit in hits:
                    src = hit.get("_source", {})
                    crd = str(src.get("firm_source_id") or "")
                    name = str(src.get("firm_name") or "")
                    scope = str(src.get("firm_scope") or "")

                    if not crd or crd in seen or scope.upper() == "INACTIVE":
                        continue
                    seen.add(crd)
                    results.append({"sec_file_number": crd, "adviser_name": name})

                start += rows
                if start >= total:
                    break

    logger.info("Found %d candidate advisers via IAPD search", len(results))
    return results


# ---------------------------------------------------------------------------
# 2. fetch_adv_filing
# ---------------------------------------------------------------------------

async def _resolve_crd(client: httpx.AsyncClient, sec_file_number: str) -> str | None:
    """Look up the CRD number for a given SEC file number."""
    data = await _get(
        client,
        _SEARCH_BASE,
        q=f'"{sec_file_number}"',
        forms="ADV",
    )
    hits = data.get("hits", {}).get("hits", [])
    for hit in hits:
        src = hit.get("_source", {})
        crd = src.get("crd_number") or src.get("crdNumber")
        if crd:
            return str(crd)
        # sometimes embedded in entity_id
        entity_id = hit.get("_id", "")
        if entity_id.isdigit():
            return entity_id
    return None


def _extract_part1(filing: dict, sec_file_number: str) -> dict:
    """Pull adviser-level fields from the LatestFilingHeaderInfo response."""
    info = filing.get("filingHeaderInfo") or filing.get("Filing") or filing

    def g(*keys):
        for k in keys:
            v = info.get(k)
            if v is not None:
                return v
        return None

    disclosures_raw = g("disclosures", "hasDisclosures", "HasDisclosures")
    charges_pf_raw = g("chargesPerformanceFees", "ChargesPerformanceFees")

    return {
        "sec_file_number": sec_file_number,
        "crd_number": str(g("crdNumber", "CRDNumber", "crd_number") or ""),
        "cik_number": str(g("cikNumber", "CIKNumber", "cik_number") or ""),
        "lei": str(g("lei", "LEI") or ""),
        "adviser_name": str(g("firmName", "FirmName", "legalName", "adviser_name") or ""),
        "adviser_business_name": str(g("primaryBusinessName", "businessName") or ""),
        "adviser_state": str(g("stateCode", "mailingAddressState", "principalOfficeState") or ""),
        "adviser_org_type": str(g("organizationType", "orgType") or ""),
        "adviser_website": str(g("website", "websiteAddress") or ""),
        "is_exempt_reporting_adviser": _bool_int(g("isExemptReportingAdviser", "exemptReportingAdviser")),
        "adviser_regulatory_aum": _safe_float(g("regulatoryAUM", "totalRegAUM")),
        "adviser_total_employees": _safe_int(g("totalEmployees", "numEmployees")),
        "adviser_investment_employees": _safe_int(g("investmentAdvisoryEmployees", "numInvestmentEmployees")),
        "adviser_charges_performance_fees": _bool_int(charges_pf_raw),
        "adviser_has_disclosures": _bool_int(disclosures_raw),
        "adviser_cco_name": str(g("ccoName", "chiefComplianceOfficerName") or ""),
    }


def _extract_fund(fund: dict, adviser_meta: dict, adviser_num_funds: int) -> dict:
    """Map a raw Schedule D fund dict to the schema."""

    def g(*keys):
        for k in keys:
            v = fund.get(k)
            if v is not None:
                return v
        return None

    fund_id = str(g("fundId", "privateFundId", "id") or "")
    return {
        # identity
        "fund_id": fund_id,
        "sec_file_number": adviser_meta.get("sec_file_number", ""),
        "crd_number": adviser_meta.get("crd_number", ""),
        "cik_number": adviser_meta.get("cik_number", ""),
        "lei": str(g("lei", "LEI") or ""),
        # fund basics
        "fund_name": str(g("fundName", "name") or ""),
        "fund_type": str(g("fundType", "type") or ""),
        "fund_jurisdiction": str(g("fundJurisdiction", "domicileCountry", "jurisdiction") or ""),
        "fund_formation_date": str(g("fundFormationDate", "formationDate", "inceptionDate") or ""),
        "fund_filing_date": str(g("filingDate", "latestFilingDate") or ""),
        # financials
        "gross_asset_value": _safe_float(g("grossAssetValue", "totalGrossAssets", "nav")),
        "min_investment_usd": _safe_float(g("minimumInvestmentAmount", "minInvestment")),
        "beneficial_owner_count": _safe_int(g("beneficialOwnerCount", "numBeneficialOwners")),
        "us_persons_pct": _safe_float(g("usPersonsPct", "percentUSPersons")),
        "inv_company_act_exemption": str(g("investmentCompanyActExemption", "icaExemption") or ""),
        # structure flags
        "is_master_fund": _bool_int(g("isMasterFund", "masterFund")),
        "is_feeder_fund": _bool_int(g("isFeederFund", "feederFund")),
        "is_fund_of_funds": _bool_int(g("isFundOfFunds", "fundOfFunds")),
        "open_to_new_investment": _bool_int(g("openToNewInvestment", "isOpenToNewInvestment")),
        "relied_on_reg_d": _bool_int(g("reliedOnRegD", "regulationDExemption")),
        # audit / custody
        "is_audited": _bool_int(g("isAudited", "audited")),
        "auditor_pcaob_registered": _bool_int(g("auditorPCAOBRegistered", "pcaobRegistered")),
        "auditor_name": str(g("auditorName", "auditorFirmName") or ""),
        "custodian_name": str(g("custodianName", "primaryCustodian") or ""),
        "has_third_party_admin": _bool_int(g("hasThirdPartyAdmin", "thirdPartyAdmin")),
        "pct_externally_valued": _safe_float(g("pctExternallyValued", "percentExternallyValued")),
        # adviser fields (merged from Part 1)
        "adviser_name": adviser_meta.get("adviser_name", ""),
        "adviser_business_name": adviser_meta.get("adviser_business_name", ""),
        "adviser_state": adviser_meta.get("adviser_state", ""),
        "adviser_org_type": adviser_meta.get("adviser_org_type", ""),
        "adviser_website": adviser_meta.get("adviser_website", ""),
        "is_exempt_reporting_adviser": adviser_meta.get("is_exempt_reporting_adviser"),
        "adviser_regulatory_aum": adviser_meta.get("adviser_regulatory_aum"),
        "adviser_total_employees": adviser_meta.get("adviser_total_employees"),
        "adviser_investment_employees": adviser_meta.get("adviser_investment_employees"),
        "adviser_num_funds": adviser_num_funds,
        "adviser_charges_performance_fees": adviser_meta.get("adviser_charges_performance_fees"),
        "adviser_has_disclosures": adviser_meta.get("adviser_has_disclosures"),
        "adviser_cco_name": adviser_meta.get("adviser_cco_name", ""),
        "last_updated": date.today().isoformat(),
    }


async def fetch_adv_filing(sec_file_number: str) -> list[dict]:
    """Return merged fund records for one adviser identified by sec_file_number or CRD."""
    async with httpx.AsyncClient() as client:
        if sec_file_number.isdigit():
            crd = sec_file_number
        else:
            crd = await _resolve_crd(client, sec_file_number)
            if not crd:
                logger.warning("Could not resolve CRD for %s — skipping", sec_file_number)
                return []

        # Part 1 — adviser header
        part1_raw = await _get(client, f"{_ADVISER_API}/{crd}/LatestFilingHeaderInfo")
        adviser_meta = _extract_part1(part1_raw, sec_file_number)
        adviser_meta["crd_number"] = crd

        # Schedule D — private funds
        funds_raw = await _get(client, f"{_ADVISER_API}/{crd}/PrivateFunds")

        all_funds: list[dict] = (
            funds_raw.get("privateFunds")
            or funds_raw.get("funds")
            or (funds_raw if isinstance(funds_raw, list) else [])
        )

        adviser_num_funds = len(all_funds)

        qualifying = [
            f for f in all_funds
            if (f.get("fundType") or f.get("type") or "") == "Private Equity Fund"
            and (f.get("fundFormationDate") or f.get("formationDate") or f.get("inceptionDate") or "") >= CUTOFF_DATE
        ]

        return [_extract_fund(f, adviser_meta, adviser_num_funds) for f in qualifying]


# ---------------------------------------------------------------------------
# 3. run_full_refresh
# ---------------------------------------------------------------------------

async def run_full_refresh() -> int:
    advisers = await fetch_all_adviser_ciks()
    logger.info("Starting full refresh — %d advisers to process", len(advisers))

    all_records: list[dict] = []

    for adviser in advisers:
        try:
            records = await fetch_adv_filing(adviser["sec_file_number"])
            all_records.extend(records)
        except Exception as exc:
            logger.error(
                "Error fetching adviser %s (%s): %s",
                adviser["sec_file_number"],
                adviser.get("adviser_name", ""),
                exc,
            )

    for i in range(0, len(all_records), _BATCH_SIZE):
        upsert_funds(all_records[i : i + _BATCH_SIZE])

    total = len(all_records)
    logger.info("Full refresh complete — %d funds loaded", total)
    return total


# ---------------------------------------------------------------------------
# 4. run_incremental_refresh
# ---------------------------------------------------------------------------

async def run_incremental_refresh() -> int:
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    seen: set[str] = set()
    advisers: list[dict] = []
    page_size = 10
    from_offset = 0

    async with httpx.AsyncClient() as client:
        while True:
            data = await _get(
                client,
                _SEARCH_BASE,
                q='"Private Equity Fund"',
                forms="ADV",
                dateRange="custom",
                startdt=yesterday,
                **{"hits.hits._source": "file_num,entity_name"},
                **{"from": from_offset},
            )

            hits_wrapper = data.get("hits", {})
            total_hits = hits_wrapper.get("total", {}).get("value", 0)
            hits = hits_wrapper.get("hits", [])

            if not hits:
                break

            for hit in hits:
                src = hit.get("_source", {})
                file_num = src.get("file_num") or hit.get("_id", "")
                if not file_num or file_num in seen:
                    continue
                seen.add(file_num)
                advisers.append(
                    {
                        "sec_file_number": file_num,
                        "adviser_name": src.get("entity_name", ""),
                    }
                )

            from_offset += page_size
            if from_offset >= total_hits:
                break

    logger.info("Starting incremental refresh — %d advisers changed since %s", len(advisers), yesterday)

    all_records: list[dict] = []

    for adviser in advisers:
        try:
            records = await fetch_adv_filing(adviser["sec_file_number"])
            all_records.extend(records)
        except Exception as exc:
            logger.error(
                "Error fetching adviser %s (%s): %s",
                adviser["sec_file_number"],
                adviser.get("adviser_name", ""),
                exc,
            )

    for i in range(0, len(all_records), _BATCH_SIZE):
        upsert_funds(all_records[i : i + _BATCH_SIZE])

    total = len(all_records)
    logger.info("Incremental refresh complete — %d funds upserted", total)
    return total
