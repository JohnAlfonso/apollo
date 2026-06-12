import argparse
import asyncio
import json
import os
import random
import re
import sys
from pathlib import Path
from urllib.parse import urlparse, quote
import time
import httpx
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

STATE_FILE = "apollo_state.json"
HOME_URL = "https://app.apollo.io/#/home"

REQUEST_LOG_FILE = "apollo_requests.json"
RESPONSE_LOG_FILE = "apollo_responses.json"

BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://95.217.116.91:9900")
DEBUG_LOG_RESPONSES = os.environ.get("APOLLO_DEBUG_LOG", "").lower() in ("1", "true", "yes")
# Set APOLLO_DISCOVER_ENDPOINTS=1 to log ALL Apollo API calls and discover the company info endpoint
DISCOVER_ENDPOINTS = os.environ.get("APOLLO_DISCOVER_ENDPOINTS", "").lower() in ("1", "true", "yes")

# ── Resource blocking (CPU reduction) ───────────────────────────────────────
# Block heavy but non-essential resource types — scripts are intentionally
# kept because Apollo is an SPA and all API interception depends on JS.
BLOCKED_RESOURCE_TYPES = {"image", "media", "font"}

# Third-party tracking / analytics domains that burn CPU but give us nothing.
BLOCKED_DOMAINS = [
    "googletagmanager.com", "google-analytics.com", "doubleclick.net",
    "googlesyndication.com", "googleadservices.com",
    "hotjar.com", "segment.io", "segment.com",
    "amplitude.com", "mixpanel.com",
    "intercom.io", "intercomcdn.com",
    "fullstory.com", "sentry.io", "clarity.ms",
    "facebook.net", "ads.twitter.com",
    "adroll.com", "optimizely.com",
    "heap.io", "heapanalytics.com",
    "cdn.pendo.io", "pendo.io",
    "cdn.appcues.com", "appcues.com",
    "bat.bing.com",
]


class _OverlayShutdown(BaseException):
    """Raised when a persistent overlay/Cloudflare block requires process restart (exit 2).
    Inherits BaseException so broad `except Exception` handlers cannot accidentally swallow it."""


# Mutable one-element list so nested functions/closures can set it without 'nonlocal'.
# When True, all workers stop before their next company and main() exits with code 2.
_shutdown_requested: list[bool] = [False]


def is_interesting_request(url: str, resource_type: str) -> bool:
    url_l = url.lower()
    return (
        "apollo.io" in url_l
        and (
            "api/v1/mixed_people/search" in url_l
            or "api/v1/organizations/" in url_l
            or "api/v1/mixed_companies/search" in url_l
        )
    )


def normalize_domain(domain: str) -> str:
    if not domain:
        return ""
    domain = domain.strip().lower()
    domain = re.sub(r"^https?://", "", domain)
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.split("/")[0]
    return domain


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip()).lower()


def extract_domain_from_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        host = re.sub(r"^www\.", "", host)
        return host
    except Exception:
        return normalize_domain(url)


def build_people_url_from_company_url(company_url: str) -> str | None:
    """
    Convert:
      https://app.apollo.io/#/organizations/<org_id>
    or
      https://app.apollo.io/#/organizations/<org_id>?...
    into:
      https://app.apollo.io/#/organizations/<org_id>/people?page=1&...
    URL format confirmed from real Apollo URLs (no trailing slash before ?).
    """
    if not company_url:
        return None

    m = re.search(r"#/organizations/([^/?]+)", company_url)
    if not m:
        return None

    org_id = m.group(1)
    return (
        f"https://app.apollo.io/#/organizations/{org_id}/people"
        f"?page=1&sortByField=recommendations_score&sortAscending=false"
        f"&personLocations[]=United%20States"
    )


def extract_org_id(company_url: str) -> str | None:
    """Extract the Apollo org_id from any organization URL."""
    if not company_url:
        return None
    m = re.search(r"#/organizations/([^/?]+)", company_url)
    return m.group(1) if m else None


# ── Segmentation constants (used to bypass Apollo's 1000-result per-query cap) ──
# Level-1 segments: one seniority value per query (never grouped).
# Values CONFIRMED from real Apollo UI and URL screenshots.
# All valid values: owner, founder, c_suite, partner, vp, head, director, manager, senior, entry, intern
SENIORITY_SEGMENTS: list[list[str]] = [
    ["owner", "founder", "c_suite"],
    ["partner", "vp"],
    ["head", "director"],
    ["manager"],
    ["senior"],
]

# Level-2 segments: job title groups.
# Parameter name "personTitles[]" — CONFIRMED from real Apollo URL.
# When a seniority segment exceeds 1000 results, it is split by these title groups.
# Each group targets a broad function area; combined they cover the full workforce.
JOB_TITLE_SEGMENTS: list[list[str]] = [
    ["engineer", "developer", "programmer", "architect"],
    ["data", "analyst", "scientist", "researcher"],
    ["sales", "account executive", "account manager", "business development"],
    ["manager", "supervisor", "lead", "head"],
    ["director", "vice president", "president", "chief"],
    ["marketing", "content", "brand", "communications"],
    ["operations", "coordinator", "specialist", "administrator"],
    ["finance", "accountant", "auditor", "controller"],
    ["recruiter", "human resources", "talent", "people"],
    ["product manager", "product owner", "designer", "ux"],
    ["customer success", "support", "service", "consultant"],
    ["legal", "attorney", "counsel", "compliance"],
    ["teacher", "professor", "instructor", "trainer"],
]

PEOPLE_LIMIT = 1000  # Apollo Pro plan cap per query

# ── ICP segments for nonus_person mode ──────────────────────────────────────
# Unimportant seniorities (senior, entry, intern) and non-buyer title groups
# (teachers, customer support, legal, HR/recruiting) are never scraped.
ICP_SENIORITY_SEGMENTS: list[list[str]] = [
    ["owner", "founder", "c_suite"],
    ["partner", "vp"],
    ["head", "director"],
    ["manager"],
]

# Flat list used to pre-filter the base (unsegmented) people URL so people
# outside the ICP seniorities are never fetched, even for small companies.
ICP_SENIORITIES_ALL: list[str] = [s for group in ICP_SENIORITY_SEGMENTS for s in group]

ICP_JOB_TITLE_SEGMENTS: list[list[str]] = [
    ["engineer", "developer", "programmer", "architect"],
    ["data", "analyst", "scientist", "researcher"],
    ["sales", "account executive", "account manager", "business development"],
    ["manager", "supervisor", "lead", "head"],
    ["director", "vice president", "president", "chief"],
    ["marketing", "content", "brand", "communications"],
    ["operations", "coordinator", "specialist", "administrator"],
    ["finance", "accountant", "auditor", "controller"],
    ["product manager", "product owner", "designer", "ux"],
]


def build_people_url_segmented(
    org_id: str,
    seniorities: list[str] | None = None,
    titles: list[str] | None = None,
    locations: tuple[str, ...] | list[str] | None = ("United States",),
) -> str:
    """Build an Apollo people URL with optional seniority and job title filters.
    Both personSeniorities[] and personTitles[] are confirmed from real Apollo URLs.
    locations defaults to United States (US flows); pass None for no location filter.
    """
    url = (
        f"https://app.apollo.io/#/organizations/{org_id}/people"
        f"?page=1&sortByField=recommendations_score&sortAscending=false"
    )
    for loc in (locations or []):
        url += f"&personLocations[]={quote(loc)}"
    for s in (seniorities or []):
        url += f"&personSeniorities[]={s}"
    for t in (titles or []):
        url += f"&personTitles[]={t}"
    return url


_COUNTRY_TO_CODE: dict[str, str] = {
    "united states": "US", "united kingdom": "GB", "canada": "CA",
    "australia": "AU", "germany": "DE", "france": "FR", "india": "IN",
    "china": "CN", "japan": "JP", "brazil": "BR", "mexico": "MX",
    "spain": "ES", "italy": "IT", "netherlands": "NL", "sweden": "SE",
    "norway": "NO", "denmark": "DK", "finland": "FI", "switzerland": "CH",
    "austria": "AT", "belgium": "BE", "portugal": "PT", "poland": "PL",
    "singapore": "SG", "new zealand": "NZ", "south africa": "ZA",
    "israel": "IL", "ireland": "IE", "south korea": "KR", "taiwan": "TW",
}


def _linkedin_slug(url: str) -> str:
    """Extract the company slug from a LinkedIn URL, or return the value as-is."""
    if not url:
        return ""
    return url.rstrip("/").split("/")[-1]


def _build_location(org: dict) -> str:
    """Build a consistently formatted address string from Apollo's individual address fields."""
    parts = [
        org.get("street_address") or "",
        org.get("city") or "",
        org.get("state") or "",
        org.get("postal_code") or "",
        org.get("country") or "",
    ]
    return ", ".join(p for p in parts if p.strip())


def _build_company_payload(org: dict, company_id: int | None) -> dict:
    """Map an Apollo organization dict to the standard company info template."""
    country = (org.get("country") or "").strip()
    return {
        "domain":         org.get("primary_domain") or extract_domain_from_url(org.get("website_url") or ""),
        "source":         "apollo",
        "founded":        org.get("founded_year"),
        "website":        org.get("website_url") or "",
        "industry":       org.get("industry") or "",
        "location":       _build_location(org),
        "city":           org.get("city") or "",
        "state":          org.get("state") or "",
        "country":        org.get("country") or "",
        "companyId":      org.get("id") or "",
        "companyName":    org.get("name") or "",
        "companyType":    "",
        "countryCode":    _COUNTRY_TO_CODE.get(country.lower(), ""),
        "description":    org.get("short_description") or "",
        "linkedin_url":   _linkedin_slug(org.get("linkedin_url") or ""),
        "employeesCount": org.get("estimated_num_employees") or 0,
    }


async def save_company_info(company_id: int, payload: dict) -> dict | None:
    """POST company info payload to backend."""
    url = f"{BACKEND_API_URL}/api/data-apollo/company-info"
    body = {"company_id": company_id, **payload}
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(url, json=body)
        resp.raise_for_status()
        return resp.json()


async def fetch_company_from_queue() -> dict | None:
    """Fetch next company for get_company mode (FOR UPDATE SKIP LOCKED, sets flag3='1')."""
    url = f"{BACKEND_API_URL}/api/data-apollo/company-queue"
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json().get("record")


async def end_company_queue(company_id: int) -> None:
    """Mark a get_company-queue record as done (flag3='2')."""
    url = f"{BACKEND_API_URL}/api/data-apollo/company-queue/{company_id}/end"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


async def fetch_new_person_company() -> dict | None:
    """Fetch next company for new_person mode (FOR UPDATE SKIP LOCKED, sets flag1=-1)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/new-person-queue"
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json().get("record")


async def end_new_person_company(company_id: int, success: bool) -> None:
    """Set flag1=1 (success) or flag1=0 (fail) for a new_person company."""
    state = "success" if success else "fail"
    url = f"{BACKEND_API_URL}/api/data-apollo/new-person-queue/{company_id}/{state}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


async def reset_company_queue_item(company_id: int) -> None:
    """Reset a get_company-queue record back to unprocessed (flag3=NULL)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/company-queue/{company_id}/reset"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


async def reset_new_person_company(company_id: int) -> None:
    """Reset a new_person-queue record back to unprocessed (flag1=NULL)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/new-person-queue/{company_id}/reset"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


# ── nonus_person mode backend helpers (sn71_company_nonus / sn71_person_nonus) ──

async def fetch_nonus_company() -> dict | None:
    """Fetch next nonus company (fetch_flag=1, FOR UPDATE SKIP LOCKED, sets fetch_flag=2)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/nonus-queue"
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json().get("record")


async def end_nonus_company(company_id: int, success: bool) -> None:
    """Set fetch_flag=0 (success/ended) or fetch_flag=-1 (fail) for a nonus company."""
    state = "success" if success else "fail"
    url = f"{BACKEND_API_URL}/api/data-apollo/nonus-queue/{company_id}/{state}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


async def reset_nonus_company(company_id: int) -> None:
    """Reset a nonus-queue record back to unprocessed (fetch_flag=1)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/nonus-queue/{company_id}/reset"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


async def save_nonus_company_info(company_id: int, payload: dict) -> dict | None:
    """POST company info payload to backend (saved to sn71_company_nonus.contact_info)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/nonus-company-info"
    body = {"company_id": company_id, **payload}
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(url, json=body)
        resp.raise_for_status()
        return resp.json()


async def save_nonus_apollo_page(company_id: int, apollo_json: str) -> dict | None:
    """Save Apollo people-page response to backend (inserts into sn71_person_nonus)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/nonus-process"
    payload = {"company_id": company_id, "apollo_json": apollo_json}
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()


async def _get_public_ip() -> str:
    """Fetch the machine's public IP via ipify.org. Returns '0.0.0.0' on failure."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get("https://api.ipify.org")
            return resp.text.strip()
    except Exception as e:
        print(f"[Worker] Could not fetch public IP: {e}")
        return "0.0.0.0"


async def report_worker_status(worker_id: int, ip: str, status: str) -> None:
    """Upsert worker status to backend. Fire-and-forget: never raises."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{BACKEND_API_URL}/api/data-apollo/worker-status",
                json={"worker_id": worker_id, "ip": ip, "status": status},
            )
        print(f"[W{worker_id}] Status reported: {status} (ip={ip})")
    except Exception as e:
        print(f"[W{worker_id}] Failed to report status '{status}': {e}")


async def log_company_info(body_text: str, company_id: int | None, save_func=None) -> None:
    """Parse Apollo org response, map to company template, print and save to backend.
    save_func defaults to save_company_info (sn71_company); nonus mode passes
    save_nonus_company_info (sn71_company_nonus).
    """
    try:
        data = json.loads(body_text)
    except json.JSONDecodeError:
        return

    # Apollo returns either {"organization": {...}} or {"organizations": [...]}
    org = data.get("organization") or (data.get("organizations") or [None])[0]
    if not org:
        return

    payload = _build_company_payload(org, company_id)

    print("\n" + "=" * 80)
    print(f"[COMPANY INFO] company_id={company_id}")
    print("=" * 80)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("=" * 80)

    try:
        save = save_func or save_company_info
        result = await save(company_id, payload)
        print(f"[Backend] Saved company info for company_id={company_id}: {result}")
    except Exception as e:
        print(f"[Backend] Failed to save company info for company_id={company_id}: {e}")



async def dump_json(path: str, data):
    try:
        Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved: {path}")
    except Exception as e:
        print(f"Failed to save {path}: {e}")


# ─────────────────────── human-like behaviour helpers ───────────────────────

async def human_mouse_move(page, target_x: int | None = None, target_y: int | None = None):
    vp = page.viewport_size or {"width": 1440, "height": 900}

    end_x = target_x if target_x is not None else random.randint(80, vp["width"] - 80)
    end_y = target_y if target_y is not None else random.randint(80, vp["height"] - 80)

    # 🔥 REDUCED steps (was 9–18)
    steps = random.randint(3, 6)

    mid_x = (end_x) / 2 + random.randint(-60, 60)
    mid_y = (end_y) / 2 + random.randint(-60, 60)

    for i in range(1, steps + 1):
        t = i / steps
        bx = int((1 - t) ** 2 * 0 + 2 * (1 - t) * t * mid_x + t ** 2 * end_x)
        by = int((1 - t) ** 2 * 0 + 2 * (1 - t) * t * mid_y + t ** 2 * end_y)

        await page.mouse.move(bx, by)
        await page.wait_for_timeout(random.randint(5, 15))  # 🔥 reduced delay


async def human_scroll(page, direction: str = "down"):
    """Scroll the page in small natural increments like a real user would."""
    total = random.randint(150, 500) * (1 if direction == "down" else -1)
    steps = random.randint(5, 10)
    delta = total // steps
    for _ in range(steps):
        await page.mouse.wheel(0, delta)
        await page.wait_for_timeout(random.randint(24, 66))


async def human_idle(page, min_ms: int = 800, max_ms: int = 1500):
    # 🔥 shorter + less aggressive
    if random.random() > 0.3:
        await page.wait_for_timeout(random.randint(min_ms, max_ms))
        return

    end_time = asyncio.get_event_loop().time() + random.uniform(min_ms / 1000, max_ms / 1000)

    while asyncio.get_event_loop().time() < end_time:
        action = random.choice(["move", "pause"])  # 🔥 removed scroll

        if action == "move":
            await human_mouse_move(page)
        else:
            await page.wait_for_timeout(random.randint(100, 250))


# ─────────────────────── cloudflare detection & handling ───────────────────

async def is_cloudflare_blocked(page) -> bool:
    """Return True if the current page is a Cloudflare challenge/block page."""
    try:
        title = (await page.title()).lower()
        if any(x in title for x in [
            "just a moment", "attention required", "checking your",
            "please wait", "verify you are human",
        ]):
            return True
    except Exception:
        pass

    for selector in [
        # Classic JS challenge
        "#challenge-form",
        "#challenge-running",
        "[data-ray]",
        # Turnstile iframe (any path under challenges.cloudflare.com)
        "iframe[src*='challenges.cloudflare.com']",
        # Turnstile container element (rendered before iframe appears)
        ".cf-turnstile",
        "[class*='cf-turnstile']",
        "[data-sitekey]",
        # Full-page Turnstile wrapper
        "#cf-turnstile",
    ]:
        try:
            loc = page.locator(selector)
            if await loc.count() > 0 and await loc.first.is_visible():
                print(f"Cloudflare indicator detected: {selector}")
                return True
        except Exception:
            pass

    return False


async def wait_for_cloudflare(page, max_wait_ms: int = 30000) -> bool:
    """Wait for a JS-auto-solving Cloudflare challenge to resolve on its own."""
    step = 2000
    waited = 0
    while waited < max_wait_ms:
        if not await is_cloudflare_blocked(page):
            print("Cloudflare challenge passed automatically.")
            return True
        print(f"Cloudflare challenge active, waiting... ({waited}ms elapsed)")
        await human_idle(page, min_ms=step, max_ms=step + 500)
        waited += step
    print("Cloudflare challenge did NOT resolve automatically.")
    return False


async def wait_for_manual_cf_solve(page, poll_interval_ms: int = 3000) -> bool:
    """Poll indefinitely until the user solves the Cloudflare Turnstile challenge.

    Unlike page.pause() this does NOT require the Playwright inspector to be open.
    The scraper simply idles until the challenge widget disappears from the DOM.
    """
    print("\n" + "=" * 70)
    print("ACTION REQUIRED: Solve the Cloudflare 'Verify you are human' checkbox")
    print("in the browser window. The scraper will resume automatically.")
    print("=" * 70)

    attempt = 0
    while True:
        await page.wait_for_timeout(poll_interval_ms)
        attempt += 1
        if not await is_cloudflare_blocked(page):
            print(f"Cloudflare challenge solved by user after ~{attempt * poll_interval_ms // 1000}s.")
            return True
        if attempt % 10 == 0:
            elapsed = attempt * poll_interval_ms // 1000
            print(f"Still waiting for Cloudflare solve... ({elapsed}s elapsed). Please check the browser.")


async def handle_cloudflare(page, ctx: dict | None = None) -> bool:
    """
    Full Cloudflare handling flow:
      1. API-body Turnstile (ctx['cf_api_block']) -> raise _OverlayShutdown immediately
      2. Not blocked          -> return True immediately
      3. JS challenge         -> wait up to 15s for auto-resolve
      4. Turnstile/hard block -> raise _OverlayShutdown (exit 2, triggers auto_antibot.py)
    """
    # Cloudflare can return a Turnstile challenge inside an Apollo API (XHR)
    # response body without ever touching the page DOM. handle_response flags
    # this in ctx['cf_api_block']; such a challenge cannot auto-resolve, so we
    # go straight to the anti-bot restart (exit 2).
    if ctx is not None and ctx.get("cf_api_block"):
        ctx["cf_api_block"] = False
        print("Cloudflare Turnstile detected in API response — triggering anti-bot restart (exit 2).")
        raise _OverlayShutdown()

    if not await is_cloudflare_blocked(page):
        return True

    print("WARNING: Cloudflare challenge detected!")

    # Step 1 – try auto-resolve (JS challenge, usually resolves in <10s)
    if await wait_for_cloudflare(page, max_wait_ms=15000):
        return True

    # Step 2 – Turnstile / hard block: cannot auto-solve.
    # Trigger immediate restart so orchestrator runs auto_antibot.py.
    print("Cloudflare Turnstile detected — triggering anti-bot restart (exit 2).")
    raise _OverlayShutdown()


async def fetch_companies(sources: str | None = None) -> list[dict]:
    """Fetch companies from backend API (company_check=1, modified_time IS NULL)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/companies"
    params = {}
    if sources and sources.strip():
        params["sources"] = sources.strip()
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.get(url, params=params or None)
        resp.raise_for_status()
        data = resp.json()
        return data.get("records", [])


async def save_apollo_page(company_id: int, apollo_json: str) -> dict | None:
    """Save Apollo page response to backend (inserts people, sets created_time)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/process"
    payload = {"company_id": company_id, "apollo_json": apollo_json}
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()


async def end_company(company_id: int) -> None:
    """Set modified_time for company in backend."""
    url = f"{BACKEND_API_URL}/api/data-apollo/end/{company_id}"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url)
        resp.raise_for_status()


async def search_company_on_searchtag(page, company_domain: str, company_name: str | None = None):
    print("\nTrying to search company on search tag...")

    target_domain = normalize_domain(company_domain)
    target_name = normalize_text(company_name) if company_name else None

    if not target_domain:
        return False, "Empty company domain", None

    # 1) Find global search input
    search_input = None
    input_selectors = [
        "input[placeholder='Search across Apollo...']",
        "input[placeholder*='Search across Apollo']",
        "input[placeholder*='Search across']",
        "input[aria-label*='Search']",
    ]

    for selector in input_selectors:
        loc = page.locator(selector)
        try:
            count = await loc.count()
            for i in range(count):
                item = loc.nth(i)
                if await item.is_visible():
                    search_input = item
                    ph = await item.get_attribute("placeholder")
                    print(f"Matched search input selector: {selector} | placeholder={ph}")
                    break
            if search_input:
                break
        except Exception as e:
            print(f"Search input selector failed: {selector} -> {e}")

    if not search_input:
        return False, "Search input not found", None

    # 2) Focus and clear old text
    try:
        # Dismiss any Apollo modal/overlay (e.g. "What's new", profile prompt, or any
        # leftover dialog) that would intercept pointer events and make hover time out.
        # Only target elements that are TRUE blocking modals: they have a backdrop attribute.
        # The generic [data-open='true'][role='presentation'] also matches permanent nav/sidebar
        # elements that are always present — those must NOT be treated as blocking.
        try:
            overlay = page.locator("[data-open='true'][role='presentation'][data-backdrop]")
            if await overlay.count() > 0 and await overlay.first.is_visible():
                print("Apollo modal overlay detected — pressing Escape to dismiss...")
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(300)
                # Still present? Click in the top-left corner (always outside any modal)
                if await overlay.count() > 0 and await overlay.first.is_visible():
                    await page.mouse.click(20, 20)
                    await page.wait_for_timeout(300)
                # If still present: exit with code 2 so orchestrator.py can
                # run auto_antibot.py and restart the scraper automatically.
                if await overlay.count() > 0 and await overlay.first.is_visible():
                    print("Overlay persists after all dismissal attempts — signalling graceful shutdown (exit 2).")
                    raise _OverlayShutdown()
        except Exception as _ov_err:
            print(f"Overlay dismiss attempt failed (non-fatal): {_ov_err}")

        await human_mouse_move(page)
        await search_input.hover()
        await page.wait_for_timeout(random.randint(20, 50))
        await search_input.click()
        await page.wait_for_timeout(random.randint(20, 45))

        await search_input.fill("")
        await page.wait_for_timeout(random.randint(15, 35))

        await search_input.click()
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.wait_for_timeout(random.randint(20, 45))
    except Exception as e:
        return False, f"Could not clear search input: {e}", None

    # 3) Type target domain with natural per-character delay
    try:
        await search_input.type(target_domain, delay=random.randint(12, 20))
        await page.wait_for_timeout(random.randint(200, 350))
    except Exception as e:
        return False, f"Could not type search term: {e}", None

    # 4) Wait for dropdown and possible API results.
    #    Single JS evaluate replaces N×3 Playwright IPC calls — one CDP round-trip
    #    does all filtering inside the browser's V8 engine.
    await page.wait_for_timeout(300)

    _JS_FIND = """([targetDomain, targetName]) => {
        const groups = [
            'a[href*="organizations"]',
            'a, [role="option"], [role="link"]',
            'button',
            'div',
        ];
        function norm(t) { return (t || '').replace(/\\s+/g, ' ').trim().toLowerCase(); }
        const seen = new Set();
        for (const sel of groups) {
            const found = [];
            for (const el of document.querySelectorAll(sel)) {
                const raw = el.textContent || '';
                const n = norm(raw);
                if (n.length < 3 || n.length > 300 || seen.has(n)) continue;
                const href = (el.getAttribute('href') || '').toLowerCase();
                if (href && !href.includes('/organizations/') &&
                        (href.includes('#/companies') || href === '/companies')) continue;
                const hasDomain = n.includes(targetDomain);
                const hasName = targetName ? n.includes(targetName) : false;
                const hasOrg = href.includes('/organizations/');
                if (!hasDomain && !hasName && !(hasOrg && n.includes('companies'))) continue;
                const r = el.getBoundingClientRect();
                if (!r.width || !r.height) continue;
                const s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden') continue;
                seen.add(n);
                found.push({ text: raw.trim().slice(0, 400), norm_text: n, href: href,
                             selector: sel, cx: Math.round(r.left + r.width / 2),
                             cy: Math.round(r.top + r.height / 2),
                             area: Math.round(r.width * r.height) });
            }
            if (found.length) return found;
        }
        return [];
    }"""

    candidate_rows = []
    retry = 0
    while True:
        try:
            candidate_rows = await page.evaluate(_JS_FIND, [target_domain, target_name or ""])
        except Exception as e:
            print(f"JS candidate scan failed: {e}")
            candidate_rows = []

        if not candidate_rows:
            if retry > 4:
                return False, "No candidate companies appeared", None
            retry += 1
            await page.wait_for_timeout(500)
        else:
            print(f"Found {len(candidate_rows)} candidates (selector={candidate_rows[0]['selector']})")
            break

    print("\nCandidate companies:")
    for idx, c in enumerate(candidate_rows[:15], start=1):
        print(f"[{idx}] selector={c['selector']}")
        print(c["text"][:400])
        print("-" * 100)

    # 5) Score candidates - only consider rows that link to an organization page
    scored_candidates = []

    for c in candidate_rows:
        href = (c["href"] or "").lower()
        text = c["norm_text"]

        score = 0

        # Domain match is king
        if target_domain == text:
            score += 100
        if target_domain in text:
            score += 50

        # Name helps
        if target_name and target_name in text:
            score += 20

        # Org link is a strong bonus but NOT a hard requirement
        # (Apollo dropdown items may use React onClick with no href)
        if "/organizations/" in href or "#/organizations/" in href:
            score += 15

        # If the row clearly belongs to company results
        if "companies" in text:
            score += 5

        # Must have at least some signal — skip completely unrelated rows
        if score == 0:
            continue

        scored_candidates.append((score, c))

    scored_candidates.sort(key=lambda x: x[0], reverse=True)

    selected = None
    if scored_candidates and scored_candidates[0][0] > 0:
        selected = scored_candidates[0][1]

    if not selected:
        return False, f"No matching company found for domain={target_domain}", None

    print("\nSelected company candidate:")
    print(selected["text"][:500])
    print("Selected href:", selected["href"])

    # 6) Click selected row using stored screen coordinates — no extra IPC needed.
    old_url = page.url
    clicked = False
    cx, cy = selected["cx"], selected["cy"]
    href = selected.get("href", "")

    try:
        await human_mouse_move(page, cx, cy)
        await page.wait_for_timeout(random.randint(25, 60))
        if href and "/organizations/" in href:
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
                await page.mouse.click(cx, cy)
        else:
            await page.mouse.click(cx, cy)
        clicked = True
    except Exception:
        # Fallback: direct coordinate click without navigation wrapper.
        try:
            await page.mouse.click(cx, cy)
            clicked = True
        except Exception as e:
            return False, f"Failed to click selected company: {e}", None

    if not clicked:
        return False, "Failed to click selected company", None

    # 7) Wait for URL or UI to update
    try:
        await page.wait_for_function(
            """oldUrl => window.location.href !== oldUrl""",
            arg=old_url,
            timeout=10000
        )
    except Exception:
        pass

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except PlaywrightTimeoutError:
        print("networkidle timeout after company click; continuing")

    await page.wait_for_timeout(350)

    current_url = page.url
    print("After company click, current URL:", current_url)

    if "/organizations/" in current_url or "#/organizations/" in current_url:
        return True, "Company detail page opened", current_url

    return False, f"Clicked candidate but did not reach company page: {current_url}", None


async def click_next_pagination(page):
    print("\nTrying to click Next button...")

    next_selectors = [
        "button[aria-label='Next']",
        "button[aria-label='next']",
        "button:has-text('Next')",
        "a[aria-label='Next']",
        "a:has-text('Next')",
    ]

    next_button = None
    for selector in next_selectors:
        loc = page.locator(selector)
        try:
            count = await loc.count()
            if count > 0:
                for i in range(count):
                    item = loc.nth(i)
                    if await item.is_visible():
                        next_button = item
                        print(f"Matched Next selector: {selector}")
                        break
            if next_button:
                break
        except Exception as e:
            print(f"Selector failed: {selector} -> {e}")

    if not next_button:
        return False, "Next button not found"

    try:
        disabled = await next_button.get_attribute("disabled")
        aria_disabled = await next_button.get_attribute("aria-disabled")

        if disabled is not None or aria_disabled == "true":
            print("Next button is disabled.")
            return True, "Next button disabled, stop pagination"
    except Exception:
        print("Could not determine if Next button is disabled; attempting click anyway.")

    print("Clicking Next...")

    # Dismiss Apollo API error modal if it is blocking pointer events.
    try:
        error_modal = page.locator("div[role='dialog'][data-ds-legacy-modal='true']")
        if await error_modal.count() > 0 and await error_modal.first.is_visible():
            print("API error modal detected — dismissing before click...")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(400)
            # Fallback: click the modal's close button if Escape didn't work.
            if await error_modal.count() > 0 and await error_modal.first.is_visible():
                close_btn = error_modal.first.locator("button[aria-label='Close'], button[data-icon-name='close'], button.zp_mXUht")
                if await close_btn.count() > 0:
                    await close_btn.first.click()
                    await page.wait_for_timeout(400)
    except Exception as _modal_err:
        print(f"Modal dismissal attempt failed (non-fatal): {_modal_err}")

    try:
        await human_mouse_move(page)
        await next_button.hover()
        await page.wait_for_timeout(random.randint(30, 80))
        async with page.expect_response(
            lambda r: "api/v1/mixed_people/search" in r.url.lower(),
            timeout=20000
        ) as resp_info:
            await next_button.click()

        response = await resp_info.value
        print("Next-page API response status:", response.status)
        print("Next-page API response URL   :", response.url)

        try:
            body = await response.text()
            print("Next-page API response body snippet:")
            print(body[:1000])
        except Exception:
            print("Could not read next-page response body.")

    except Exception as e:
        print(f"Error while waiting for pagination response: {e}")
        return False, f"Pagination click/response error: {e}"

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except PlaywrightTimeoutError:
        print("networkidle timeout after clicking Next; continuing")

    await page.wait_for_timeout(350)
    print("After Next click, current URL:", page.url)

    return False, "Clicked Next successfully"


async def _paginate_people_url(page, people_url: str, ctx: dict, label: str) -> bool:
    """
    Navigate to people_url, paginate until done, return True on success.
    Resets ctx['segment_total_entries'] before navigation so handle_response
    can populate it from the first intercepted API response.
    """
    ctx["segment_total_entries"] = None  # reset for this segment

    print(f"\n[Segment] Navigating: {label}")
    print(f"[Segment] URL: {people_url}")
    await page.goto(people_url, wait_until="domcontentloaded")

    try:
        await page.wait_for_load_state("networkidle", timeout=5000)
    except PlaywrightTimeoutError:
        print("networkidle timeout; continuing")

    if "#/login" in page.url:
        return False

    await human_idle(page, min_ms=120, max_ms=250)
    await handle_cloudflare(page, ctx)

    while True:
        # Risk 3+4: check shutdown flag at each pagination step so a mid-session
        # Cloudflare overlay detected by another worker stops us quickly.
        if _shutdown_requested[0]:
            print("[Segment] Overlay shutdown — aborting pagination early.")
            break
        result, reason = await click_next_pagination(page)
        print("click_next_pagination:", result, reason)
        if "not found" in reason.lower() or "disabled" in reason.lower():
            break
        await asyncio.sleep(0.2)

    try:
        await page.evaluate("() => { window.stop(); }")
    except Exception:
        pass

    return True


async def open_people_page_and_run_old_logic(page, people_url: str, company_id: int, ctx: dict):
    """
    Paginate the people list for a company using conditional segmentation:

      - Always loads the unsegmented URL first and checks total_entries.
      - If total <= 1000: paginate that URL directly (no filters applied).
      - If total > 1000:
          Level 1 – seniority segments
          Level 2 – job title sub-segments (only when seniority segment > 1000)

    Falls back to a single unsegmented pass when the org_id cannot be extracted.
    """
    ctx["current_company_id"] = company_id
    try:
        print(f"Opening people page: {people_url}")

        if "#/login" in page.url:
            return False, "Session expired or invalid"

        org_id = extract_org_id(people_url)
        if not org_id:
            print("[Segment] Could not extract org_id — running single unsegmented pass.")
            ok = await _paginate_people_url(page, people_url, ctx, "unsegmented")
            if not ok:
                return False, "Session expired during pagination"
        else:
            # ── Step 0: load unsegmented URL and check total_entries ─────────────
            ctx["segment_total_entries"] = None
            await page.goto(people_url, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                pass

            if "#/login" in page.url:
                return False, "Session expired"

            await page.wait_for_timeout(700)
            # Detect a Cloudflare Turnstile that fired on the people page (DOM or
            # API body) — raises _OverlayShutdown (exit 2) so auto_antibot.py runs.
            await handle_cloudflare(page, ctx)
            total = ctx.get("segment_total_entries")
            _limit = ctx.get("people_limit", PEOPLE_LIMIT)
            print(f"[Segment] Unsegmented total_entries={total}")

            if total is None or total <= _limit:
                # ── Within limit: paginate the already-loaded unsegmented page ───
                print(f"[Segment] total={total} ≤ {_limit} — paginating without filters")
                await human_idle(page, min_ms=120, max_ms=250)
                while True:
                    if _shutdown_requested[0]:  # Risk 3+4: fast exit mid-pagination
                        print("[Segment] Overlay shutdown — aborting unsegmented pagination.")
                        break
                    result, reason = await click_next_pagination(page)
                    print("click_next_pagination:", result, reason)
                    if "not found" in reason.lower() or "disabled" in reason.lower():
                        break
                    await asyncio.sleep(0.2)
                try:
                    await page.evaluate("() => { window.stop(); }")
                except Exception:
                    pass
            else:
                # ── Over limit: apply Level 1 seniority segmentation ─────────────
                # nonus_person mode overrides the segment lists / location via ctx.
                _seniority_segments = ctx.get("seniority_segments") or SENIORITY_SEGMENTS
                _title_segments = ctx.get("title_segments") or JOB_TITLE_SEGMENTS
                _locations = ctx.get("person_locations", ("United States",))
                print(f"[Segment] total={total} > {_limit} — applying seniority segmentation")
                for seniority_group in _seniority_segments:
                    seg_label = f"seniority={seniority_group}"
                    seg_url = build_people_url_segmented(
                        org_id, seniorities=seniority_group, locations=_locations
                    )

                    ctx["segment_total_entries"] = None
                    await page.goto(seg_url, wait_until="domcontentloaded")
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10000)
                    except PlaywrightTimeoutError:
                        pass

                    if "#/login" in page.url:
                        return False, "Session expired"

                    await page.wait_for_timeout(700)
                    await handle_cloudflare(page, ctx)
                    seg_total = ctx.get("segment_total_entries")
                    print(f"[Segment] {seg_label} → total_entries={seg_total}")

                    if seg_total is not None and seg_total > _limit:
                        # ── Level 2: split by job title ──────────────────────────
                        print(f"[Segment] {seg_label} exceeds {_limit} — splitting by job title")
                        for title_group in _title_segments:
                            title_label = f"seniority={seniority_group} titles={title_group}"
                            title_url = build_people_url_segmented(
                                org_id,
                                seniorities=seniority_group,
                                titles=title_group,
                                locations=_locations,
                            )
                            await _paginate_people_url(page, title_url, ctx, title_label)
                    else:
                        # ── Level 1 only: paginate this seniority group ──────────
                        await human_idle(page, min_ms=120, max_ms=250)
                        while True:
                            if _shutdown_requested[0]:  # Risk 3+4: fast exit mid-pagination
                                print("[Segment] Overlay shutdown — aborting seniority pagination.")
                                break
                            result, reason = await click_next_pagination(page)
                            print("click_next_pagination:", result, reason)
                            if "not found" in reason.lower() or "disabled" in reason.lower():
                                break
                            await asyncio.sleep(0.2)
                        try:
                            await page.evaluate("() => { window.stop(); }")
                        except Exception:
                            pass

        # ── Mark company done ────────────────────────────────────────────────────
        if ctx.get("mode") == "new_person":
            await end_new_person_company(company_id, True)
            print(f"[new_person] Set flag1=1 (success) for company_id={company_id}")
        elif ctx.get("mode") == "nonus_person":
            await end_nonus_company(company_id, True)
            print(f"[nonus_person] Set fetch_flag=0 (success) for company_id={company_id}")
        else:
            await end_company(company_id)
            print(f"Set modified_time for company_id={company_id}")

        print("Segmented scrape finished.")
        return True, "OK"
    finally:
        ctx["current_company_id"] = None
        ctx["segment_total_entries"] = None


def parse_args():
    parser = argparse.ArgumentParser(description="Apollo web scraper with backend API integration")
    parser.add_argument(
        "--sources",
        type=str,
        default=os.environ.get("SOURCES", "hunter.io-50-1000"),
        help="Comma-separated source values to filter companies (optional)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["add_person", "get_company", "new_person", "nonus_person"],
        default=os.environ.get("SCRAPER_MODE", "add_person"),
        help=(
            "add_person  : navigate to each company's people page and scrape contacts (default). "
            "get_company : capture company profile info only (name, phone, industry, address, etc.). "
            "nonus_person: process sn71_company_nonus (fetch_flag=1) via apollo_id directly — "
            "saves company info + ICP-filtered people to the nonus tables."
        ),
    )
    _headless_str = os.environ.get("HEADLESS", "true")
    parser.add_argument(
        "--headless",
        type=lambda v: v.lower() not in ("0", "false", "no"),
        default=_headless_str.lower() not in ("0", "false", "no"),
        metavar="BOOL",
        help="Run browser in headless mode (default: true). Pass --headless false to show UI.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("WORKERS", "1")),
        metavar="N",
        help="Number of parallel scraper workers (default: 1). Workers stagger startup by 30s each.",
    )
    parser.add_argument(
        "--worker_index",
        type=int,
        default=int(os.environ.get("WORKER_INDEX", "0")),
        metavar="N",
        help="Index of the current worker (default: 0). Used for staggering startup.",
    )
    parser.add_argument(
        "--people_limit",
        type=int,
        default=int(os.environ.get("PEOPLE_LIMIT", str(PEOPLE_LIMIT))),
        metavar="N",
        help=f"Apollo result cap per query segment (default: {PEOPLE_LIMIT}). Override when plan limits differ.",
    )
    return parser.parse_args()


async def run_worker(worker_id: int, args) -> None:
    """Single independent scraper worker. Staggered by worker_id * 30s at startup."""
    if worker_id > 0:
        delay = worker_id * 30
        print(f"[W{worker_id}] Waiting {delay}s before starting...")
        await asyncio.sleep(delay)

    print(f"[W{worker_id}] Worker started.")
    ip = await _get_public_ip()
    await report_worker_status(worker_id, ip, "running")

    try:
        request_logs = []
        response_logs = []
        ctx = {
            "current_company_id": None,
            "mode": args.mode,
            "company_info_logged": set(),       # tracks company_ids already logged in get_company mode
            "segment_total_entries": None,      # populated by handle_response for the first page of each segment
            "people_limit": args.people_limit,  # configurable via --people_limit
            "cf_api_block": False,              # set by handle_response when a Turnstile appears in an API body
            "inflight_company_id": None,        # company being processed; survives the people-page finally for reset
        }

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=args.headless,
                slow_mo=0
            )

            context = await browser.new_context(
                storage_state=STATE_FILE,
                viewport={"width": 1440, "height": 900},
                service_workers="block",
            )

            # ── Route-level CPU reduction ────────────────────────────────────────
            # Abort heavy static assets and third-party trackers before they load.
            # This runs at the network layer — zero rendering cost for blocked URLs.
            async def _block_route(route):
                try:
                    rt = route.request.resource_type
                    url = route.request.url.lower()
                    if rt in BLOCKED_RESOURCE_TYPES:
                        await route.abort()
                        return
                    if any(d in url for d in BLOCKED_DOMAINS):
                        await route.abort()
                        return
                    await route.continue_()
                except Exception:
                    pass  # Page/browser closed mid-request — safe to ignore

            await context.route("**/*", _block_route)

            page = await context.new_page()
            print(f"[DEBUG] Open pages: {len(context.pages)}")
            async def handle_request(request):
                try:
                    if not is_interesting_request(request.url, request.resource_type):
                        return

                    headers = await request.all_headers()
                    post_data = request.post_data

                    entry = {
                        "method": request.method,
                        "url": request.url,
                        "resource_type": request.resource_type,
                        "headers": headers,
                        "post_data": post_data,
                    }
                    request_logs.append(entry)

                    if DEBUG_LOG_RESPONSES:
                        print("\n" + "=" * 100)
                        print("REQUEST")
                        print("=" * 100)
                        print("METHOD:", request.method)
                        print("URL   :", request.url)
                        if post_data:
                            print("POST DATA:", post_data[:3000])

                except Exception as e:
                    print("handle_request error:", e)

            async def handle_response(response):
                try:
                    request = response.request

                    # ── Discovery mode: log every Apollo API call to find new endpoints ──
                    if DISCOVER_ENDPOINTS and "apollo.io" in response.url.lower():
                        url_l = response.url.lower()
                        # Only care about XHR / fetch calls, skip static assets
                        if request.resource_type in ("xhr", "fetch") or "/api/" in url_l:
                            try:
                                snippet = (await response.text())[:500]
                            except Exception:
                                snippet = "<unreadable>"
                            print("[DISCOVER] " + "-" * 80)
                            print(f"[DISCOVER] {request.method} {response.status} {response.url}")
                            print(f"[DISCOVER] body snippet: {snippet}")

                    if not is_interesting_request(response.url, request.resource_type):
                        return

                    url_l = response.url.lower()

                    # 🔥 HARD FILTER (only process what you REALLY need)
                    if not (
                        "api/v1/mixed_people/search" in url_l
                        or "api/v1/organizations/" in url_l
                    ):
                        return

                    try:
                        body_text = await response.text()
                    except Exception:
                        return

                    # Cloudflare can serve a Turnstile challenge as the API response
                    # body (XHR) while the page DOM still looks fine. Flag it so the
                    # navigation loop's handle_cloudflare() raises _OverlayShutdown
                    # (exit 2) and the orchestrator runs auto_antibot.py.
                    if body_text and ("cf-turnstile" in body_text or "challenges.cloudflare.com" in body_text):
                        ctx["cf_api_block"] = True
                        print(f"[Cloudflare] Turnstile in API body: {response.url}")
                        return

                    if DEBUG_LOG_RESPONSES:
                        response_logs.append({
                            "url": response.url,
                            "status": response.status,
                            "body_text": body_text[:10000] if body_text else None,
                        })
                        print("\n" + "=" * 100)
                        print("RESPONSE", response.status, response.url)
                        if body_text:
                            print("BODY:", body_text[:3000])

                    company_id = ctx.get("current_company_id")
                    if company_id is None:
                        return

                    url_l = response.url.lower()

                    # ── Branch: company info (org detail or mixed_companies) ──────────────
                    if "api/v1/organizations/" in url_l or "api/v1/mixed_companies/search" in url_l:
                        # Only log in get_company / nonus_person modes, and only once per
                        # company. Add to the set BEFORE awaiting to prevent async race
                        # between multiple concurrent response events for the same company.
                        if company_id not in ctx["company_info_logged"]:
                            if ctx.get("mode") == "get_company":
                                ctx["company_info_logged"].add(company_id)
                                await log_company_info(body_text, company_id)
                            elif ctx.get("mode") == "nonus_person":
                                ctx["company_info_logged"].add(company_id)
                                await log_company_info(body_text, company_id, save_func=save_nonus_company_info)
                        return

                    # ── Branch: people search pages ───────────────────────────────────────
                    # In get_company mode we only want company info — skip people entirely
                    if ctx.get("mode") == "get_company":
                        return

                    try:
                        data = json.loads(body_text)
                    except json.JSONDecodeError:
                        return

                    # Capture total_entries from the first page of each segment so the
                    # navigation logic can decide whether to sub-segment further.
                    pagination = data.get("pagination") or {}
                    total_entries = pagination.get("total_entries")
                    if total_entries is not None and ctx.get("segment_total_entries") is None:
                        ctx["segment_total_entries"] = total_entries
                        print(f"[Segment] total_entries={total_entries} (limit={ctx.get('people_limit', PEOPLE_LIMIT)})")

                    people = data.get("people", [])
                    if not people:
                        return

                    try:
                        if ctx.get("mode") == "nonus_person":
                            result = await save_nonus_apollo_page(company_id, body_text)
                        else:
                            result = await save_apollo_page(company_id, body_text)
                        inserted = result.get("inserted", 0)
                        skipped = result.get("skipped", 0)
                        print(f"[Backend] Saved page for company_id={company_id}: inserted={inserted}, skipped={skipped}")
                    except Exception as e:
                        print(f"[Backend] Failed to save page for company_id={company_id}: {e}")

                except Exception as e:
                    print("handle_response error:", e)

            page.on("request", handle_request)
            page.on("response", handle_response)

            #################################### step1 HOME ####################################
            try:
                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
            except PlaywrightTimeoutError:
                print("networkidle timeout on initial home load; continuing")
            except Exception as _e:
                print(f"page.goto HOME_URL (initial) failed: {_e} — continuing")
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                print("networkidle timeout; continuing")

            print("Current URL:", page.url)

            if "#/login" in page.url:
                print("Session expired or invalid.")
                await browser.close()
                return

            if not await handle_cloudflare(page, ctx):
                print("Cloudflare blocked home page. Aborting.")
                await browser.close()
                return

            await human_idle(page, min_ms=400, max_ms=800)

            batch_num = 0
            while True:
                if _shutdown_requested[0]:
                    print(f"[W{worker_id}] Overlay shutdown — stopping before next batch.")
                    break
                try:
                    if args.mode == "get_company":
                        rec = await fetch_company_from_queue()
                        records = [rec] if rec else []
                    elif args.mode == "new_person":
                        rec = await fetch_new_person_company()
                        records = [rec] if rec else []
                    elif args.mode == "nonus_person":
                        rec = await fetch_nonus_company()
                        records = [rec] if rec else []
                    else:
                        records = await fetch_companies(args.sources or None)
                except Exception as e:
                    print(f"Failed to fetch companies from backend: {e}")
                    break

                if not records:
                    print("No more companies to process.")
                    break

                batch_num += 1
                print(f"\n[W{worker_id}] Batch {batch_num}: fetched {len(records)} companies")

                for idx, rec in enumerate(records, start=1):
                    if _shutdown_requested[0]:
                        print(f"[W{worker_id}] Overlay shutdown — skipping remaining companies in batch.")
                        break
                    # Clear any stale API-body Turnstile flag from a previous company
                    # so it only reflects challenges seen for the current target.
                    ctx["cf_api_block"] = False
                    company_id = rec.get("id")
                    # Reset id that survives open_people_page_and_run_old_logic's finally
                    # (which nulls current_company_id) so the _OverlayShutdown handler can
                    # still reset this company's queue flag on an anti-bot restart.
                    ctx["inflight_company_id"] = company_id
                    website = rec.get("website", "")
                    name = rec.get("name", "")
                    company_source = rec.get("source", "")

                    company_domain = extract_domain_from_url(website)
                    company_name = name.strip() if name else None

                    print("\n" + "#" * 120)
                    print(f"[W{worker_id}] TARGET {idx} (company_id={company_id})")
                    print(f"company_domain = {company_domain}")
                    print(f"company_name   = {company_name}")
                    print(f"company_source = {company_source}")
                    print(f"mode           = {args.mode}")
                    print("#" * 120)

                    # get_company needs current_company_id set before search so handle_response
                    # can capture org API responses during navigation to the org page.
                    # add_person / new_person must NOT set it here: Apollo fires a spurious
                    # mixed_people/search when the org overview page loads during the search,
                    # and handle_response would save those unfiltered results.
                    # open_people_page_and_run_old_logic() sets it just-in-time instead.
                    if args.mode == "get_company":
                        ctx["current_company_id"] = company_id

                    # ── nonus_person: apollo_id is the org_id — no Apollo search needed ──────
                    # ICP filtering: only important seniorities/titles are scraped, and no
                    # personLocations filter is applied (company is outside the US).
                    if args.mode == "nonus_person":
                        apollo_org_id = (rec.get("apollo_id") or "").strip()
                        if not apollo_org_id:
                            print(f"[nonus_person] Missing apollo_id for company_id={company_id} — marking failed.")
                            try:
                                await end_nonus_company(company_id, False)
                            except Exception as e:
                                print(f"Failed to mark company_id={company_id}: {e}")
                            continue

                        ctx["seniority_segments"] = ICP_SENIORITY_SEGMENTS
                        ctx["title_segments"] = ICP_JOB_TITLE_SEGMENTS
                        ctx["person_locations"] = None

                        people_url = build_people_url_segmented(
                            apollo_org_id,
                            seniorities=ICP_SENIORITIES_ALL,
                            locations=None,
                        )
                        print(f"[nonus_person] Direct path: org_id={apollo_org_id}, people_url={people_url}")

                        if not await handle_cloudflare(page, ctx):
                            print(f"Cloudflare blocked people page for company_id={company_id}. Skipping.")
                            ctx["current_company_id"] = None
                            try:
                                await end_nonus_company(company_id, False)
                            except Exception as e:
                                print(f"Failed to mark company_id={company_id}: {e}")
                        else:
                            ok, msg = await open_people_page_and_run_old_logic(page, people_url, company_id, ctx)
                            print("open_people_page_and_run_old_logic:", ok, msg)
                            if not ok:
                                print(f"People page failed for company_id={company_id}: {msg}")
                                try:
                                    await end_nonus_company(company_id, False)
                                except Exception as e:
                                    print(f"Failed to mark company_id={company_id} as failed: {e}")

                        await asyncio.sleep(0.2)
                        print("Returning to home page for next target...")
                        try:
                            await page.goto("about:blank")
                        except Exception:
                            pass
                        try:
                            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
                            await page.wait_for_load_state("networkidle", timeout=5000)
                        except PlaywrightTimeoutError:
                            print("networkidle timeout when returning home; continuing")
                        except Exception as _e:
                            print(f"page.goto HOME_URL failed: {_e} — continuing")
                        await handle_cloudflare(page, ctx)
                        await human_idle(page, min_ms=200, max_ms=500)
                        continue  # skip the search block below

                    # ── new_person fast-path: contact_info.source == "apollo" ─────────────────
                    # When the DB already holds Apollo-sourced company info, the org_id is in
                    # contact_info.companyId.  Build the people URL directly and skip the
                    # Apollo search entirely.
                    if args.mode == "new_person":
                        contact_info = rec.get("contact_info") or {}
                        if isinstance(contact_info, str):
                            try:
                                contact_info = json.loads(contact_info)
                            except Exception:
                                contact_info = {}

                        if contact_info.get("source") == "apollo" and contact_info.get("companyId"):
                            apollo_org_id = contact_info["companyId"]
                            people_url = (
                                f"https://app.apollo.io/#/organizations/{apollo_org_id}/people/"
                                f"?page=1&sortByField=recommendations_score&sortAscending=false"
                                f"&personLocations[]=United%20States"
                            )
                            print(f"[new_person] Apollo fast-path: org_id={apollo_org_id}, people_url={people_url}")

                            if not await handle_cloudflare(page, ctx):
                                print(f"Cloudflare blocked people page for company_id={company_id}. Skipping.")
                                ctx["current_company_id"] = None
                                try:
                                    await end_new_person_company(company_id, False)
                                except Exception as e:
                                    print(f"Failed to mark company_id={company_id}: {e}")
                                await asyncio.sleep(0.2)
                                print("Returning to home page for next target...")
                                try:
                                    await page.goto("about:blank")
                                except Exception:
                                    pass
                                try:
                                    await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
                                    await page.wait_for_load_state("networkidle", timeout=5000)
                                except PlaywrightTimeoutError:
                                    print("networkidle timeout when returning home; continuing")
                                except Exception as _e:
                                    print(f"page.goto HOME_URL failed: {_e} — continuing")
                                await handle_cloudflare(page, ctx)
                                await human_idle(page, min_ms=200, max_ms=500)
                                continue

                            ok, msg = await open_people_page_and_run_old_logic(page, people_url, company_id, ctx)
                            print("open_people_page_and_run_old_logic:", ok, msg)
                            if not ok:
                                print(f"People page failed for company_id={company_id}: {msg}")
                                try:
                                    await end_new_person_company(company_id, False)
                                except Exception as e:
                                    print(f"Failed to mark company_id={company_id} as failed: {e}")

                            await asyncio.sleep(0.2)
                            print("Returning to home page for next target...")
                            try:
                                await page.goto("about:blank")
                            except Exception:
                                pass
                            try:
                                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
                                await page.wait_for_load_state("networkidle", timeout=5000)
                            except PlaywrightTimeoutError:
                                print("networkidle timeout when returning home; continuing")
                            except Exception as _e:
                                print(f"page.goto HOME_URL failed: {_e} — continuing")
                            await handle_cloudflare(page, ctx)
                            await human_idle(page, min_ms=200, max_ms=500)
                            continue  # skip the search block below

                    # Check for Cloudflare before attempting to search
                    if not await handle_cloudflare(page, ctx):
                        print(f"Cloudflare blocked search for company_id={company_id}. Skipping.")
                        ctx["current_company_id"] = None
                        try:
                            if args.mode == "get_company":
                                await end_company_queue(company_id)
                            elif args.mode == "new_person":
                                await end_new_person_company(company_id, False)
                            else:
                                await end_company(company_id)
                        except Exception as e:
                            print(f"Failed to mark company_id={company_id}: {e}")
                        continue

                    _search_retry = 0
                    while True:
                        result, reason, company_url = await search_company_on_searchtag(
                            page,
                            company_domain=company_domain,
                            company_name=company_name
                        )

                        print("search_company_on_searchtag:", result, reason, company_url)

                        if not result and reason == "Search input not found":
                            _search_retry += 1
                            if _search_retry >= 5:
                                print("Search input still not found after 5 retries. Reloading home page...")
                                try:
                                    await page.goto("about:blank")
                                except Exception:
                                    pass
                                try:
                                    await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
                                    await page.wait_for_load_state("networkidle", timeout=10000)
                                except PlaywrightTimeoutError:
                                    print("networkidle timeout when reloading home for search retry; continuing")
                                except Exception as _e:
                                    print(f"page.goto HOME_URL failed on search retry: {_e} — continuing")
                                await handle_cloudflare(page, ctx)
                                _search_retry = 0
                            else:
                                await page.wait_for_timeout(1500)
                        else:
                            break

                    if not result:
                        print(f"No matching company found for {company_domain}. Marking company_id={company_id} to avoid infinite retry.")
                        ctx["current_company_id"] = None
                        try:
                            if args.mode == "get_company":
                                await end_company_queue(company_id)
                            elif args.mode == "new_person":
                                await end_new_person_company(company_id, False)
                            else:
                                await end_company(company_id)
                        except Exception as e:
                            print(f"Failed to mark company_id={company_id}: {e}")
                        continue

                    # ── Mode: get_company ────────────────────────────────────────────────────
                    # Company info was captured automatically by handle_response when Apollo's
                    # api/v1/organizations/ endpoint fired while search_company_on_searchtag
                    # navigated to the org page.  Just wait briefly for in-flight responses,
                    # mark the company done, then move on.
                    if args.mode == "get_company":
                        await page.wait_for_timeout(random.randint(450, 900))
                        try:
                            await end_company_queue(company_id)
                            print(f"[get_company] Marked company_id={company_id} as done.")
                        except Exception as e:
                            print(f"[get_company] Failed to mark company_id={company_id}: {e}")
                        finally:
                            ctx["current_company_id"] = None

                    # ── Mode: add_person / new_person ────────────────────────────────────────
                    else:
                        people_url = build_people_url_from_company_url(company_url or page.url)
                        print("Derived people URL:", people_url)

                        if not people_url:
                            print(f"Could not derive people URL from company page. Marking company_id={company_id} to avoid infinite retry.")
                            ctx["current_company_id"] = None
                            try:
                                if args.mode == "new_person":
                                    await end_new_person_company(company_id, False)
                                else:
                                    await end_company(company_id)
                            except Exception as e:
                                print(f"Failed to mark company_id={company_id}: {e}")
                        else:
                            # Check for Cloudflare before opening people page
                            if not await handle_cloudflare(page, ctx):
                                print(f"Cloudflare blocked people page for company_id={company_id}. Skipping.")
                                ctx["current_company_id"] = None
                                try:
                                    if args.mode == "new_person":
                                        await end_new_person_company(company_id, False)
                                    else:
                                        await end_company(company_id)
                                except Exception as e:
                                    print(f"Failed to mark company_id={company_id}: {e}")
                            else:
                                ok, msg = await open_people_page_and_run_old_logic(page, people_url, company_id, ctx)
                                print("open_people_page_and_run_old_logic:", ok, msg)

                                if not ok:
                                    print(f"People page failed for company_id={company_id}: {msg}")
                                    if args.mode == "new_person":
                                        try:
                                            await end_new_person_company(company_id, False)
                                        except Exception as e:
                                            print(f"Failed to mark company_id={company_id} as failed: {e}")

                    await asyncio.sleep(random.uniform(0.2, 0.6))
                    # Blank the page first to stop all lingering JS/React before reloading home.
                    print("Returning to home page for next target...")
                    try:
                        await page.goto("about:blank")
                    except Exception:
                        pass
                    try:
                        await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
                    except PlaywrightTimeoutError:
                        print("networkidle timeout when returning home; continuing")
                    except Exception as _goto_err:
                        print(f"page.goto HOME_URL failed: {_goto_err} — continuing")
                    else:
                        try:
                            await page.wait_for_load_state("networkidle", timeout=5000)
                        except PlaywrightTimeoutError:
                            print("networkidle timeout when returning home; continuing")

                    await handle_cloudflare(page, ctx)
                    await human_idle(page, min_ms=200, max_ms=500)
                    print(f"[DEBUG] Open pages: {len(context.pages)}")

            if DEBUG_LOG_RESPONSES:
                await dump_json(REQUEST_LOG_FILE, request_logs)
                await dump_json(RESPONSE_LOG_FILE, response_logs)

            print(f"[W{worker_id}] All done.")
            await report_worker_status(worker_id, ip, "done")
            await page.pause()
            try:
                await context.unroute_all(behavior="ignoreErrors")
            except Exception:
                pass
            await browser.close()

    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"[W{worker_id}] Interrupted — reporting done.")
        await report_worker_status(worker_id, ip, "done")
        raise
    except _OverlayShutdown:
        _shutdown_requested[0] = True
        print(f"[W{worker_id}] Overlay/antibot detected — resetting company and killing process immediately.")
        _reset_cid = ctx.get("current_company_id") or ctx.get("inflight_company_id")
        _reset_mode = ctx.get("mode")
        if _reset_cid:
            try:
                if _reset_mode == "get_company":
                    await reset_company_queue_item(_reset_cid)
                    print(f"[W{worker_id}] Reset get_company flag3=NULL for company_id={_reset_cid}")
                elif _reset_mode == "new_person":
                    await reset_new_person_company(_reset_cid)
                    print(f"[W{worker_id}] Reset new_person flag1=NULL for company_id={_reset_cid}")
                elif _reset_mode == "nonus_person":
                    await reset_nonus_company(_reset_cid)
                    print(f"[W{worker_id}] Reset nonus fetch_flag=1 for company_id={_reset_cid}")
            except Exception as _reset_err:
                print(f"[W{worker_id}] Failed to reset company status: {_reset_err}")
        await report_worker_status(worker_id, ip, "done")
        sys.exit(2)  # Kill immediately — stops all workers, orchestrator restarts with antibot.
    except SystemExit as _se:
        print(f"[W{worker_id}] SystemExit({_se.code}) — reporting error.")
        await report_worker_status(worker_id, ip, "error")
        raise
    except Exception as _worker_exc:
        print(f"[W{worker_id}] Unhandled exception: {_worker_exc}")
        await report_worker_status(worker_id, ip, "error")
        raise


async def main():
    args = parse_args()
    if not Path(STATE_FILE).exists():
        raise FileNotFoundError(f"{STATE_FILE} not found. Run save_apollo_state.py first.")

    # Risk 1: reset module-level flag so re-entrant calls (e.g. tests) start clean.
    _shutdown_requested[0] = False

    try:
        if args.workers <= 1:
            await run_worker(0 + args.worker_index, args)
        else:
            print(f"Starting {args.workers} workers with 30s stagger...")
            await asyncio.gather(*(run_worker(i, args) for i in range(args.workers)))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\n[Main] Shutdown requested. All workers reported their final status.")
    except Exception as _main_exc:
        # Risk 2: an unhandled worker exception must not skip the exit-2 check below.
        print(f"[Main] Worker raised unhandled exception: {_main_exc}")

    if _shutdown_requested[0]:
        print("[Main] Overlay/antibot shutdown — exiting with code 2 for orchestrator.")
        sys.exit(2)


if __name__ == "__main__":
    asyncio.run(main())
