import asyncio
import json
import random
import re
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

STATE_FILE = "apollo_state.json"
HOME_URL = "https://app.apollo.io/#/home"

REQUEST_LOG_FILE = "apollo_requests.json"
RESPONSE_LOG_FILE = "apollo_responses.json"


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
        await search_input.click()
        await page.wait_for_timeout(300)

        await search_input.fill("")
        await page.wait_for_timeout(200)

        await search_input.click()
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.wait_for_timeout(300)
    except Exception as e:
        return False, f"Could not clear search input: {e}", None

    # 3) Type target domain
    try:
        await search_input.type(target_domain, delay=random.randint(60, 120))
        await page.wait_for_timeout(1500)
    except Exception as e:
        return False, f"Could not type search term: {e}", None

    # 4) Wait for dropdown
    await page.wait_for_timeout(1500)

    candidate_rows = []
    retry = 0
    while True:
        # Apollo often portals dropdown into body, so search broadly
        row_selectors = [
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
                # for i in range(min(count, 200)):
                for i in range(count):
                    row = rows.nth(i)
                    try:
                        if not await row.is_visible():
                            continue

                        text = await row.inner_text()
                        norm_text = normalize_text(text)
                        if len(norm_text) < 3:
                            continue

                        # Keep only things that look like search results
                        looks_relevant = (
                            target_domain in norm_text
                            or "companies" in norm_text
                            or (target_name and target_name in norm_text)
                        )

                        if not looks_relevant:
                            continue

                        href = await row.get_attribute("href")
                        temp.append({
                            "locator": row,
                            "text": text,
                            "norm_text": norm_text,
                            "href": href,
                            "selector": row_selector,
                        })
                    except Exception:
                        continue

                # Use the first selector that gives plausible candidates
                if temp:
                    candidate_rows = temp
                    print(f"Collected {len(candidate_rows)} candidate nodes using selector: {row_selector}")
                    break

            except Exception as e:
                print(f"Row selector failed: {row_selector} -> {e}")

        if not candidate_rows:
            if retry > 3:
                return False, "No candidate companies appeared", None
            retry = retry + 1
            await page.wait_for_timeout(1500)
        else:
            break

    print("\nCandidate companies:")
    for idx, c in enumerate(candidate_rows[:15], start=1):
        print(f"[{idx}] selector={c['selector']}")
        print(c["text"][:400])
        print("-" * 100)

    # 5) Score candidates
    scored_candidates = []

    for c in candidate_rows:
        score = 0
        text = c["norm_text"]
        href = (c["href"] or "").lower()

        # Domain match is king
        if target_domain == text:
            score += 100
        if target_domain in text:
            score += 50

        # Name helps, but not enough by itself
        if target_name and target_name in text:
            score += 20

        # Company/org links get bonus
        if "/organizations/" in href or "#/organizations/" in href:
            score += 15

        # If the row clearly belongs to company results
        if "companies" in text:
            score += 5

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
        async with page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
            await selected["locator"].click()
        clicked = True
    except Exception:
        # Apollo is SPA-heavy. Normal navigation often won't fire.
        try:
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

    print("************* ========= *************")
    await page.wait_for_timeout(10000)
    print("************* ========= *************")

    while True:
        result, reason = await click_next_pagination(page)
        print("click_next_pagination:", result, reason)

        if result:
            break

        await asyncio.sleep(random.randint(4, 8))

    await page.screenshot(path="apollo_people_page_after_next.png", full_page=True)
    print("Saved: apollo_people_page_after_next.png")

    print("Old logic finished.")
    return True, "OK"


async def main():
    if not Path(STATE_FILE).exists():
        raise FileNotFoundError(f"{STATE_FILE} not found. Run save_apollo_state.py first.")

    request_logs = []
    response_logs = []

    # Put your search targets here
    targets = [
        {"company_domain": "empirefoods.com", "company_name": "Empire Marketing Strategies"},
        {"company_domain": "ethosrisk.com", "company_name": "Ethos"},
    ]

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

                print("\n" + "=" * 100)
                print("REQUEST")
                print("=" * 100)
                print("METHOD:", request.method)
                print("URL   :", request.url)
                print("TYPE  :", request.resource_type)
                print("HEADERS:")
                print(json.dumps(headers, indent=2, ensure_ascii=False))

                if post_data:
                    print("POST DATA:")
                    print(post_data[:3000])

            except Exception as e:
                print("handle_request error:", e)

        async def handle_response(response):
            try:
                request = response.request
                if not is_interesting_request(response.url, request.resource_type):
                    return

                headers = await response.all_headers()

                try:
                    body_text = await response.text()
                except Exception:
                    body_text = "<could not decode response body>"

                entry = {
                    "url": response.url,
                    "status": response.status,
                    "status_text": response.status_text,
                    "resource_type": request.resource_type,
                    "request_method": request.method,
                    "headers": headers,
                    "body_text": body_text[:10000] if body_text else None,
                }
                response_logs.append(entry)

                print("\n" + "=" * 100)
                print("RESPONSE")
                print("=" * 100)
                print("STATUS:", response.status, response.status_text)
                print("URL   :", response.url)
                print("TYPE  :", request.resource_type)
                print("HEADERS:")
                print(json.dumps(headers, indent=2, ensure_ascii=False))

                if body_text:
                    print("BODY:")
                    print(body_text[:3000])

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

        await page.wait_for_timeout(5000)

        for idx, target in enumerate(targets, start=1):
            raw_domain = target.get("company_domain", "")
            raw_name = target.get("company_name", "")

            company_domain = extract_domain_from_url(raw_domain)
            company_name = raw_name.strip() if raw_name else None

            print("\n" + "#" * 120)
            print(f"TARGET {idx}")
            print(f"company_domain = {company_domain}")
            print(f"company_name   = {company_name}")
            print("#" * 120)

            while True:
                result, reason, company_url = await search_company_on_searchtag(
                    page,
                    company_domain=company_domain,
                    company_name=company_name
                )

                print("search_company_on_searchtag:", result, reason, company_url)
                
                if not result and reason == "Search input not found":
                    page.wait_for_timeout(2000)
                else:
                    break

            if not result:
                print(f"No matching company found for {company_domain}. Skipping.")
                continue

            people_url = build_people_url_from_company_url(company_url or page.url)
            print("Derived people URL:", people_url)

            if not people_url:
                print("Could not derive people URL from company page. Skipping.")
                continue

            ok, msg = await open_people_page_and_run_old_logic(page, people_url)
            print("open_people_page_and_run_old_logic:", ok, msg)

            # Go back home before next company search
            print("Returning to home page for next target...")
            await page.goto(HOME_URL, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                print("networkidle timeout when returning home; continuing")

            await page.wait_for_timeout(3000)

        await dump_json(REQUEST_LOG_FILE, request_logs)
        await dump_json(RESPONSE_LOG_FILE, response_logs)

        print("All done.")
        await page.pause()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())