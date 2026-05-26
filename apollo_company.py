import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
import copy
import traceback
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import httpx
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://95.217.116.91:9900")

STATE_FILE = "apollo_state.json"
HOME_URL = "https://app.apollo.io/#/home"

# ── Resource blocking (CPU reduction) ────────────────────────────────────────
BLOCKED_RESOURCE_TYPES = {"image", "media", "font"}

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
# When True, all workers stop before their next search URL and main() exits with code 2.
_shutdown_requested: list[bool] = [False]


def is_interesting_request(url: str, resource_type: str) -> bool:
    url_l = url.lower()
    return (
        ("apollo.io" in url_l and "api/v1/mixed_companies/search" in url_l) or
        ("apollo.io" in url_l and "api/v1/organizations/load_snippets" in url_l)
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
        async with page.expect_response(
            lambda r: "api/v1/mixed_companies/search" in r.url.lower(),
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


async def open_people_page_and_run_old_logic(page, people_url: str):
    print(f"Opening people page: {people_url}")
    await page.goto(people_url, wait_until="domcontentloaded")

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except PlaywrightTimeoutError:
        print("networkidle timeout; continuing")

    print("Current URL:", page.url)

    if "#/login" in page.url:
        return False, "Session expired or invalid"

    # Cloudflare detection — signal graceful shutdown; all workers finish current job then exit(2)
    if await is_cloudflare_blocked(page):
        print("Cloudflare overlay detected — signalling graceful shutdown (exit 2).")
        raise _OverlayShutdown()

    await page.wait_for_timeout(10000)

    while True:
        # Risk 3+4: check shutdown flag at each pagination step so a mid-session
        # Cloudflare overlay detected by another worker stops us quickly.
        if _shutdown_requested[0]:
            print("[Pagination] Overlay shutdown — aborting pagination early.")
            break
        result, reason = await click_next_pagination(page)
        print("click_next_pagination:", result, reason)

        if result:
            # Next button disabled — natural end of results.
            break

        if "not found" in reason.lower():
            # Next button missing — either last page or Cloudflare removed the UI.
            if await is_cloudflare_blocked(page):
                print("[Pagination] Cloudflare detected after Next button disappeared — signalling shutdown.")
                raise _OverlayShutdown()
            # Genuine end of results.
            break

        await asyncio.sleep(random.randint(1, 4))

    print("Old logic finished.")
    return True, "OK"


def update_page(url, new_page):
    parsed = urlparse(url)

    # parse fragment instead of query
    fragment = parsed.fragment
    path, _, query_string = fragment.partition("?")

    query = parse_qs(query_string)
    query["page"] = [str(new_page)]

    new_query = urlencode(query, doseq=True)
    new_fragment = f"{path}?{new_query}"

    return urlunparse(parsed._replace(fragment=new_fragment))


# ── Cloudflare detection ──────────────────────────────────────────────────────

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
        "#challenge-form",
        "#challenge-running",
        "[data-ray]",
        "iframe[src*='challenges.cloudflare.com']",
        ".cf-turnstile",
        "[class*='cf-turnstile']",
        "[data-sitekey]",
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


# ── Worker status ─────────────────────────────────────────────────────────────

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


# ── Backend API helpers ───────────────────────────────────────────────────────

async def claim_search_url(location: str = "US"):
    """Claim one unfinished search URL record from backend.

    The backend uses FOR UPDATE SKIP LOCKED + sets created_date atomically,
    so concurrent workers can never claim the same record.

    Returns (record_id, search_url, start_page) resuming from backend page,
    or (None, None, None) when no eligible records remain.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{BACKEND_API_URL}/api/apollo-search/unmodified-records",
            params={"limit": 1, "location": location},
        )
        resp.raise_for_status()
        records = resp.json().get("records", [])

    if not records:
        return None, None, None

    row = records[0]
    record_id = row["id"]
    # Resume from backend-provided page. Fall back to page 1 when missing/invalid.
    try:
        start_page = int(row.get("page") or 1)
    except Exception:
        start_page = 1
    if start_page < 1:
        start_page = 1

    search_url = update_page(row["search_url"], start_page)
    return record_id, search_url, start_page


async def end_search_url_with_realtime(record_id: int, location: str = "US", retry_count: int = 3) -> bool:
    """Mark search URL as done and set real_time=0 for REALTIME location.
    Retries on failure to ensure search_condition is reset.
    Returns True on success, False on failure after retries.
    """
    for attempt in range(retry_count):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{BACKEND_API_URL}/api/apollo-search/{record_id}/end-time",
                    json={"location": location},
                )
                resp.raise_for_status()
            print(f"[Backend] Successfully marked search_id={record_id} as done with realtime")
            return True
        except Exception as e:
            print(f"[Backend] Attempt {attempt + 1}/{retry_count} failed to mark search_id={record_id}: {e}")
            if attempt < retry_count - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                print(f"[Backend] FAILED to reset search_id={record_id} after {retry_count} attempts")
                return False
    return False


async def end_search_url(record_id: int) -> None:
    """Mark a search URL record as fully processed (sets modified_date)."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(
                f"{BACKEND_API_URL}/api/apollo-search/{record_id}/end-time"
            )
    except Exception as e:
        print(f"[Backend] Failed to mark search_id={record_id} as done: {e}")


# ── Worker ────────────────────────────────────────────────────────────────────

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
        # Per-worker state — each worker has its own copies so asyncio tasks
        # running in the same process never share these structures.
        organizations_search: list = []
        pagination_search: dict = {}
        # search_id_ref is a one-element list so handle_response (a closure) can
        # observe reassignments made in the outer loop without needing 'nonlocal'.
        search_id_ref: list = [None]

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=args.headless,
                slow_mo=100,
            )

            context = await browser.new_context(
                storage_state=STATE_FILE,
                viewport={"width": 1440, "height": 900},
                service_workers="block",
            )

            # Block heavy assets and third-party trackers at the network layer.
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
                    pass  # page/browser closed mid-request — safe to ignore

            await context.route("**/*", _block_route)

            page = await context.new_page()

            async def handle_response(response):
                try:
                    request = response.request
                    if not is_interesting_request(response.url, request.resource_type):
                        return

                    if "api/v1/mixed_companies/search" in response.url:
                        try:
                            body_text = await response.text()
                        except Exception:
                            return
                        try:
                            search_result = json.loads(body_text)
                        except json.JSONDecodeError:
                            # Non-JSON body — almost certainly a Cloudflare challenge page.
                            if "cf-turnstile" in body_text or "challenges.cloudflare.com" in body_text:
                                print(f"[W{worker_id}] Cloudflare challenge detected in API response — signalling shutdown.")
                                _shutdown_requested[0] = True
                            else:
                                print(f"[W{worker_id}] Non-JSON response from mixed_companies/search (ignored).")
                            return
                        pagination = search_result.get("pagination", {})
                        if pagination:
                            pagination_search["page"]          = pagination["page"]
                            pagination_search["total_entries"] = pagination["total_entries"]
                            pagination_search["total_pages"]   = pagination["total_pages"]

                            organizations = search_result.get("organizations", [])
                            print(f"[W{worker_id}] Found {len(organizations)} organizations on this page.")
                            # Replace list contents in-place so the closure reference stays valid.
                            del organizations_search[:]
                            organizations_search.extend(organizations)
                    else:
                        # organizations/load_snippets response
                        try:
                            body_text = await response.text()
                        except Exception:
                            return
                        try:
                            search_result = json.loads(body_text)

                            print("=" * 88)
                            print(f"[W{worker_id}] organizations_search count: {len(organizations_search)}")
                            print(f"[W{worker_id}] pagination_search: {pagination_search}")

                            organizations_snippets = search_result.get("organizations", [])
                            organizations = copy.deepcopy(organizations_search)
                            for item in organizations:
                                for snippet in organizations_snippets:
                                    if item.get("id") == snippet.get("id"):
                                        item["snippet"] = snippet
                                        break

                            payload = {"organizations": organizations}
                            if args.location == "REALTIME" and search_id_ref[0] is not None:
                                payload["real_time_id"] = search_id_ref[0]

                            async with httpx.AsyncClient(timeout=60) as client:
                                await client.post(
                                    f"{BACKEND_API_URL}/api/apollo-search/organizations",
                                    json=payload,
                                )

                            current_search_id = search_id_ref[0]
                            if current_search_id is not None:
                                pagination = copy.deepcopy(pagination_search)
                                async with httpx.AsyncClient(timeout=30) as client:
                                    await client.post(
                                        f"{BACKEND_API_URL}/api/apollo-search/{current_search_id}/pagination",
                                        json={**pagination, "location": args.location},
                                    )
                        except Exception as e:
                            print(f"[W{worker_id}] Error processing response body: {e}")
                            traceback.print_exc()

                except Exception as e:
                    print(f"[W{worker_id}] handle_response error: {e}")

            page.on("response", handle_response)

            await page.wait_for_timeout(5000)

            while True:
                if _shutdown_requested[0]:
                    print(f"[W{worker_id}] Overlay shutdown — stopping before next search URL.")
                    break

                record_id, search_url, start_page = await claim_search_url(location=args.location)

                if record_id is not None and start_page is not None and start_page > 100:
                    print(
                        f"[W{worker_id}] Skipping search_id={record_id}: start page {start_page} > 100."
                    )
                    if args.location == "REALTIME":
                        await end_search_url_with_realtime(record_id, location=args.location)
                    else:
                        await end_search_url(record_id)
                    continue

                if not search_url:
                    sleep_time = 5 if args.location == "REALTIME" else 10
                    print(f"[W{worker_id}] " + "=" * 10 + f" no records, sleeping {sleep_time}s " + "=" * 10)
                    await asyncio.sleep(sleep_time)
                    continue

                search_id_ref[0] = record_id

                print("\n" + "#" * 120)
                print(f"[W{worker_id}] SEARCH_URL = {search_url}")
                print("#" * 120)

                try:
                    ok, msg = await open_people_page_and_run_old_logic(page, search_url)
                    print(f"[W{worker_id}] open_people_page_and_run_old_logic: {ok}, {msg}")
                except _OverlayShutdown:
                    _shutdown_requested[0] = True
                    print(f"[W{worker_id}] Overlay during scrape — graceful shutdown for search_id={record_id}.")
                    if args.location == "REALTIME":
                        await end_search_url_with_realtime(record_id, location=args.location)
                    else:
                        await end_search_url(record_id)
                    search_id_ref[0] = None
                    break

                # Mark record fully processed regardless of pagination outcome.
                # The backend pagination endpoint already sets modified_date on the
                # last page; end_search_url is a safe idempotent call for cases where
                # pagination ended early (e.g. 0-result search URL).
                if args.location == "REALTIME":
                    await end_search_url_with_realtime(record_id, location=args.location)
                else:
                    await end_search_url(record_id)
                search_id_ref[0] = None

                await page.goto(HOME_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(5000)

            await browser.close()

    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"[W{worker_id}] Interrupted — reporting done.")
        await report_worker_status(worker_id, ip, "done")
        raise
    except _OverlayShutdown:
        _shutdown_requested[0] = True
        print(f"[W{worker_id}] Overlay/antibot detected — graceful shutdown initiated.")
        await report_worker_status(worker_id, ip, "done")
        # Do NOT re-raise: return normally so asyncio.gather does not cancel sibling workers.
    except SystemExit as _se:
        print(f"[W{worker_id}] SystemExit({_se.code}) — reporting error.")
        await report_worker_status(worker_id, ip, "error")
        raise
    except Exception as _exc:
        print(f"[W{worker_id}] Unhandled exception: {_exc}")
        await report_worker_status(worker_id, ip, "error")
        raise


# ── CLI & entry point ─────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Apollo company search scraper")
    parser.add_argument(
        "--location",
        type=str,
        default=os.environ.get("LOCATION", "US"), # US, NONUS, REALTIME
        help="Location filter for search URLs (default: US)",
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
        help="Index offset for this process's workers (default: 0). Used when launching multiple processes.",
    )
    return parser.parse_args()


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
            # 40% of workers → "US", 60% → "NONUS".
            # round() gives the closest integer; clamped so both sides get ≥ 0.
            us_count = max(0, min(args.workers, round(args.workers * 0.4)))
            nonus_count = args.workers - us_count
            print(
                f"[Main] Starting {args.workers} workers with 30s stagger: "
                f"{us_count} US + {nonus_count} NONUS..."
            )
            worker_tasks = []
            for i in range(args.workers):
                worker_args = copy.copy(args)
                # worker_args.location = "US" if i < us_count else "NONUS"
                worker_tasks.append(run_worker(i + args.worker_index, worker_args))
            await asyncio.gather(*worker_tasks)
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
