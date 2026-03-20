import argparse
import asyncio
import json
import os
import random
import re
from pathlib import Path
from urllib.parse import urlparse
import time
import httpx
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

STATE_FILE = "apollo_state.json"
HOME_URL = "https://app.apollo.io/#/home"

REQUEST_LOG_FILE = "apollo_requests.json"
RESPONSE_LOG_FILE = "apollo_responses.json"

BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://95.217.116.91:9900")
DEBUG_LOG_RESPONSES = os.environ.get("APOLLO_DEBUG_LOG", "").lower() in ("1", "true", "yes")


def is_interesting_request(url: str, resource_type: str) -> bool:
    url_l = url.lower()
    return (
        "apollo.io" in url_l
        and (
            "api/v1/mixed_people/search" in url_l
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
      https://app.apollo.io/#/organizations/<org_id>/people/?page=1&sortByField=recommendations_score&sortAscending=false&personLocations[]=United%20States
    """
    if not company_url:
        return None

    m = re.search(r"#/organizations/([^/?]+)", company_url)
    if not m:
        return None

    org_id = m.group(1)
    return (
        f"https://app.apollo.io/#/organizations/{org_id}/people/"
        f"?page=1&sortByField=recommendations_score&sortAscending=false"
        f"&personLocations[]=United%20States"
    )


async def dump_json(path: str, data):
    try:
        Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved: {path}")
    except Exception as e:
        print(f"Failed to save {path}: {e}")


# ─────────────────────── human-like behaviour helpers ───────────────────────

async def human_mouse_move(page):
    """Move the mouse to a random position on the viewport in natural steps."""
    vp = page.viewport_size or {"width": 1440, "height": 900}
    x = random.randint(80, vp["width"] - 80)
    y = random.randint(80, vp["height"] - 80)
    await page.mouse.move(x, y, steps=random.randint(8, 20))
    await page.wait_for_timeout(random.randint(50, 180))


async def human_scroll(page, direction: str = "down"):
    """Scroll the page in small natural increments like a real user would."""
    total = random.randint(150, 500) * (1 if direction == "down" else -1)
    steps = random.randint(3, 7)
    delta = total // steps
    for _ in range(steps):
        await page.mouse.wheel(0, delta)
        await page.wait_for_timeout(random.randint(60, 180))


async def human_idle(page, min_ms: int = 1500, max_ms: int = 4000):
    """Simulate an idle human: move mouse and randomly scroll while waiting."""
    end_time = asyncio.get_event_loop().time() + random.uniform(min_ms / 1000, max_ms / 1000)
    while asyncio.get_event_loop().time() < end_time:
        action = random.choice(["move", "move", "scroll", "pause"])
        if action == "move":
            await human_mouse_move(page)
        elif action == "scroll":
            await human_scroll(page, direction=random.choice(["down", "down", "up"]))
        else:
            await page.wait_for_timeout(random.randint(300, 800))


# ─────────────────────── cloudflare detection & handling ───────────────────

async def is_cloudflare_blocked(page) -> bool:
    """Return True if the current page is a Cloudflare challenge/block page."""
    try:
        title = (await page.title()).lower()
        if any(x in title for x in ["just a moment", "attention required", "checking your", "please wait"]):
            return True
    except Exception:
        pass

    for selector in [
        "#challenge-form",
        "#challenge-running",
        "[data-ray]",
        "iframe[src*='challenges.cloudflare.com']",
    ]:
        try:
            if await page.locator(selector).count() > 0:
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


async def handle_cloudflare(page) -> bool:
    """
    Full Cloudflare handling flow:
      1. Not blocked          -> return True immediately
      2. JS challenge         -> wait up to 30s for auto-resolve
      3. Turnstile/hard block -> pause and let user solve manually
      4. Still blocked        -> return False (caller should skip the company)
    """
    if not await is_cloudflare_blocked(page):
        return True

    print("WARNING: Cloudflare challenge detected!")

    # Step 1 – try auto-resolve (JS challenge)
    if await wait_for_cloudflare(page, max_wait_ms=30000):
        return True

    # Step 2 – interactive challenge (Turnstile): hand over to user
    print("WARNING: Manual solving required. Complete the challenge in the browser window.")
    await page.pause()  # opens Playwright inspector so user can intervene
    await page.wait_for_timeout(2000)

    if not await is_cloudflare_blocked(page):
        print("Cloudflare challenge solved manually.")
        return True

    print("Could not pass Cloudflare challenge. Skipping this target.")
    return False


async def fetch_companies(sources: str | None = None) -> list[dict]:
    """Fetch companies from backend API (company_check=1, modified_time IS NULL)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/companies"
    params = {}
    if sources and sources.strip():
        params["sources"] = sources.strip()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url, params=params or None)
        resp.raise_for_status()
        data = resp.json()
        return data.get("records", [])


async def save_apollo_page(company_id: int, apollo_json: str) -> dict | None:
    """Save Apollo page response to backend (inserts people, sets created_time)."""
    url = f"{BACKEND_API_URL}/api/data-apollo/process"
    payload = {"company_id": company_id, "apollo_json": apollo_json}
    async with httpx.AsyncClient(timeout=60) as client:
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
        await human_mouse_move(page)
        await search_input.hover()
        await page.wait_for_timeout(random.randint(200, 450))
        await search_input.click()
        await page.wait_for_timeout(random.randint(200, 400))

        await search_input.fill("")
        await page.wait_for_timeout(random.randint(150, 300))

        await search_input.click()
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.wait_for_timeout(random.randint(200, 400))
    except Exception as e:
        return False, f"Could not clear search input: {e}", None

    # 3) Type target domain with natural per-character delay
    try:
        await search_input.type(target_domain, delay=random.randint(80, 150))
        await page.wait_for_timeout(random.randint(2000, 3000))
    except Exception as e:
        return False, f"Could not type search term: {e}", None

    # 4) Wait for dropdown and possible API results
    await page.wait_for_timeout(2500)

    candidate_rows = []
    retry = 0
    while True:
        # Apollo often portals dropdown into body, so search broadly
        row_selectors = [
            "a[href*='organizations']",
            "a",
            "button",
            "[role='option']",
            "[role='link']",
            "div",
        ]

        body = page.locator("body")

        for row_selector in row_selectors:
            try:
                rows = body.locator(row_selector)
                count = await rows.count()
                
                print ("-----------------------------------------------------")
                print (count, "candidates found with selector:", row_selector)
                print ("-----------------------------------------------------")

                temp = []
                for i in range(count):
                    row = rows.nth(i)
                    try:
                        if not await row.is_visible():
                            continue

                        text = await row.inner_text()
                        norm_text = normalize_text(text)
                        if len(norm_text) < 3:
                            continue

                        # Reject container elements — individual dropdown rows are short
                        if len(norm_text) > 300:
                            continue

                        href = (await row.get_attribute("href")) or ""

                        # Reject generic category links (e.g. #/companies) - not company org pages
                        if href and "/organizations/" not in href and "#/organizations/" not in href:
                            if "#/companies" in href or href.strip() in ("#/companies", "/companies"):
                                continue

                        # Keep only rows that look like actual company search results
                        has_domain = target_domain in norm_text
                        # has_name alone is sufficient: Apollo dropdown shows company name, not domain;
                        # sidebar nav items ("People", "Companies") won't contain the target company name
                        has_name = bool(target_name and target_name in norm_text)
                        has_org_link = "/organizations/" in href or "#/organizations/" in href

                        looks_relevant = (
                            has_domain
                            or has_name
                            or (has_org_link and ("companies" in norm_text or target_domain in norm_text))
                        )

                        if not looks_relevant:
                            continue

                        temp.append({
                            "locator": row,
                            "text": text,
                            "norm_text": norm_text,
                            "href": href,
                            "selector": row_selector,
                        })
                    except Exception:
                        continue

                if temp:
                    candidate_rows = temp
                    print(f"Collected {len(candidate_rows)} candidate nodes using selector: {row_selector}")
                    break

            except Exception as e:
                print(f"Row selector failed: {row_selector} -> {e}")

        if not candidate_rows:
            if retry > 4:
                return False, "No candidate companies appeared", None
            retry = retry + 1
            await page.wait_for_timeout(2000)
        else:
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

    # 6) Click selected row
    old_url = page.url
    clicked = False

    try:
        await human_mouse_move(page)
        await selected["locator"].hover()
        await page.wait_for_timeout(random.randint(250, 600))
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
            await selected["locator"].click()
        clicked = True
    except Exception:
        # Apollo is SPA-heavy. Normal navigation often won't fire.
        try:
            await selected["locator"].hover()
            await page.wait_for_timeout(random.randint(150, 350))
            await selected["locator"].click()
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

    await page.wait_for_timeout(3000)

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

    try:
        await human_mouse_move(page)
        await next_button.hover()
        await page.wait_for_timeout(random.randint(300, 700))
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

    await page.wait_for_timeout(3000)
    print("After Next click, current URL:", page.url)

    return False, "Clicked Next successfully"


async def open_people_page_and_run_old_logic(page, people_url: str, company_id: int, ctx: dict):
    """Open people page, paginate, save each page via handle_response, call end_company when done."""
    ctx["current_company_id"] = company_id
    try:
        print(f"Opening people page: {people_url}")
        await page.goto(people_url, wait_until="domcontentloaded")

        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except PlaywrightTimeoutError:
            print("networkidle timeout; continuing")

        print("Current URL:", page.url)

        if "#/login" in page.url:
            return False, "Session expired or invalid"

        print("************* ========= *************")
        # Simulate idle human behaviour while the page loads
        await human_idle(page, min_ms=8000, max_ms=12000)
        print("************* ========= *************")

        while True:
            result, reason = await click_next_pagination(page)
            print("click_next_pagination:", result, reason)

            if "not found" in reason.lower() or "disabled" in reason.lower():
                break

            await asyncio.sleep(random.randint(6, 10))

        await end_company(company_id)
        print(f"Set modified_time for company_id={company_id}")

        await page.screenshot(path="apollo_people_page_after_next.png", full_page=True)
        print("Saved: apollo_people_page_after_next.png")

        print("Old logic finished.")
        return True, "OK"
    finally:
        ctx["current_company_id"] = None


def parse_args():
    parser = argparse.ArgumentParser(description="Apollo web scraper with backend API integration")
    parser.add_argument(
        "--sources",
        type=str,
        default=os.environ.get("SOURCES", "hunter.io-50-1000"),
        help="Comma-separated source values to filter companies (optional)",
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    if not Path(STATE_FILE).exists():
        raise FileNotFoundError(f"{STATE_FILE} not found. Run save_apollo_state.py first.")

    request_logs = []
    response_logs = []
    ctx = {"current_company_id": None}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=100
        )

        context = await browser.new_context(
            storage_state=STATE_FILE,
            viewport={"width": 1440, "height": 900},
            service_workers="block",
        )

        page = await context.new_page()
        time.sleep(30)
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
                if not is_interesting_request(response.url, request.resource_type):
                    return

                try:
                    body_text = await response.text()
                except Exception:
                    body_text = "<could not decode response body>"

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

                try:
                    data = json.loads(body_text)
                except json.JSONDecodeError:
                    return

                people = data.get("people", [])
                if not people:
                    return

                try:
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
        await page.goto(HOME_URL, wait_until="domcontentloaded")

        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except PlaywrightTimeoutError:
            print("networkidle timeout; continuing")

        print("Current URL:", page.url)

        if "#/login" in page.url:
            print("Session expired or invalid.")
            await browser.close()
            return

        if not await handle_cloudflare(page):
            print("Cloudflare blocked home page. Aborting.")
            await browser.close()
            return

        await human_idle(page, min_ms=4000, max_ms=7000)

        batch_num = 0
        while True:
            try:
                records = await fetch_companies(args.sources or None)
            except Exception as e:
                print(f"Failed to fetch companies from backend: {e}")
                break

            if not records:
                print("No more companies to process.")
                break

            batch_num += 1
            print(f"\nBatch {batch_num}: fetched {len(records)} companies")

            for idx, rec in enumerate(records, start=1):
                company_id = rec.get("id")
                website = rec.get("website", "")
                name = rec.get("name", "")
                company_source = rec.get("source", "")

                company_domain = extract_domain_from_url(website)
                company_name = name.strip() if name else None

                print("\n" + "#" * 120)
                print(f"TARGET {idx} (company_id={company_id})")
                print(f"company_domain = {company_domain}")
                print(f"company_name   = {company_name}")
                print(f"company_source = {company_source}")
                print("#" * 120)

                # Check for Cloudflare before attempting to search
                if not await handle_cloudflare(page):
                    print(f"Cloudflare blocked search for company_id={company_id}. Skipping.")
                    try:
                        await end_company(company_id)
                    except Exception as e:
                        print(f"Failed to mark company_id={company_id}: {e}")
                    continue

                while True:
                    result, reason, company_url = await search_company_on_searchtag(
                        page,
                        company_domain=company_domain,
                        company_name=company_name
                    )

                    print("search_company_on_searchtag:", result, reason, company_url)

                    if not result and reason == "Search input not found":
                        await page.wait_for_timeout(2000)
                    else:
                        break

                if not result:
                    print(f"No matching company found for {company_domain}. Marking company_id={company_id} to avoid infinite retry.")
                    try:
                        await end_company(company_id)
                    except Exception as e:
                        print(f"Failed to mark company_id={company_id}: {e}")
                    continue

                people_url = build_people_url_from_company_url(company_url or page.url)
                print("Derived people URL:", people_url)

                if not people_url:
                    print("Could not derive people URL from company page. Marking company_id={company_id} to avoid infinite retry.")
                    try:
                        await end_company(company_id)
                    except Exception as e:
                        print(f"Failed to mark company_id={company_id}: {e}")
                    continue

                # Check for Cloudflare before opening people page
                if not await handle_cloudflare(page):
                    print(f"Cloudflare blocked people page for company_id={company_id}. Skipping.")
                    try:
                        await end_company(company_id)
                    except Exception as e:
                        print(f"Failed to mark company_id={company_id}: {e}")
                    continue

                ok, msg = await open_people_page_and_run_old_logic(page, people_url, company_id, ctx)
                print("open_people_page_and_run_old_logic:", ok, msg)

                if not ok:
                    print(f"People page failed for company_id={company_id}: {msg}")
                await asyncio.sleep(random.randint(8, 15))
                # Go back home before next company search
                print("Returning to home page for next target...")
                await page.goto(HOME_URL, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except PlaywrightTimeoutError:
                    print("networkidle timeout when returning home; continuing")

                await handle_cloudflare(page)
                await human_idle(page, min_ms=2000, max_ms=4000)

        if DEBUG_LOG_RESPONSES:
            await dump_json(REQUEST_LOG_FILE, request_logs)
            await dump_json(RESPONSE_LOG_FILE, response_logs)

        print("All done.")
        await page.pause()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
