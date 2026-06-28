import argparse
import asyncio
import json
import os
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

DEFAULT_STATE_FILE = "apollo_state.json"
PEOPLE_API_ENDPOINT = "api/v1/mixed_people/search"
NEXT_BUTTON_SELECTORS = [
    "button[aria-label='Next']",
    "button[aria-label='next']",
    "button:has-text('Next')",
    "a[aria-label='Next']",
    "a:has-text('Next')",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Apollo people search test extractor")
    parser.add_argument(
        "url",
        type=str,
        help="Apollo people search URL to extract person info from",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=os.environ.get("APOLLO_STATE_FILE", DEFAULT_STATE_FILE),
        help=f"Playwright storage state file for Apollo login session (default: {DEFAULT_STATE_FILE})",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=False,
        help="Run browser in headless mode",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Maximum number of pages to paginate when extracting results (0 = all pages)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="apollo_people_results.json",
        help="File path to save extracted person JSON",
    )
    return parser.parse_args()


async def capture_people_responses(page, captured):
    async def handle_response(response):
        url = response.url.lower()
        if PEOPLE_API_ENDPOINT in url:
            try:
                body = await response.text()
                data = json.loads(body)
                captured.append({
                    "url": response.url,
                    "status": response.status,
                    "data": data,
                })
                print(f"Captured people response: {response.url} status={response.status}")
            except Exception as exc:
                print(f"Failed to parse people response from {response.url}: {exc}")

    page.on("response", handle_response)


async def find_next_button(page):
    for selector in NEXT_BUTTON_SELECTORS:
        try:
            loc = page.locator(selector)
            count = await loc.count()
            if count == 0:
                continue
            for i in range(count):
                btn = loc.nth(i)
                if await btn.is_visible():
                    return btn
        except Exception:
            continue
    return None


async def click_next_page(page):
    next_button = await find_next_button(page)
    if not next_button:
        return False
    try:
        disabled = await next_button.get_attribute("disabled")
        aria_disabled = await next_button.get_attribute("aria-disabled")
        if disabled is not None or aria_disabled == "true":
            return False
    except Exception:
        pass
    await next_button.scroll_into_view_if_needed()
    await page.wait_for_timeout(200)
    await next_button.click()
    return True


def flatten_people(captured):
    people = []
    for item in captured:
        data = item.get("data") or {}
        page_people = data.get("people") or data.get("contacts") or []
        if isinstance(page_people, list):
            people.extend(page_people)
    return people


async def run_extraction(url, state_file, headless, max_pages, output_path):
    if not Path(state_file).exists():
        raise FileNotFoundError(
            f"Storage state file not found: {state_file}.\n" \
            "Log in manually via Playwright and save the state to this file before running."
        )

    captured_responses = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=state_file, viewport={"width": 1440, "height": 900})
        page = await context.new_page()
        await capture_people_responses(page, captured_responses)

        print(f"Navigating to: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeoutError:
            print("networkidle timeout; continuing")
        await page.wait_for_timeout(4000)

        page_index = 2
        while True:
            if max_pages > 0 and page_index > max_pages:
                print(f"Reached maximum requested pages: {max_pages}")
                break

            next_button = await find_next_button(page)
            if not next_button:
                print("Next button not found on page", page_index - 1)
                break

            try:
                async with page.expect_response(lambda r: PEOPLE_API_ENDPOINT in r.url.lower(), timeout=20000):
                    await next_button.click()
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except PlaywrightTimeoutError:
                    print("networkidle timeout after pagination; continuing")
                await page.wait_for_timeout(2500)
                print(f"Moved to page {page_index}")
                page_index += 1
            except PlaywrightTimeoutError:
                print(f"Timeout waiting for people response on page {page_index}")
                break
            except Exception as exc:
                print(f"Error clicking next page on page {page_index}: {exc}")
                break

        people = flatten_people(captured_responses)
        print(f"Extracted {len(people)} people records from {len(captured_responses)} API responses.")

        if output_path:
            with open(output_path, "w", encoding="utf-8") as out_file:
                json.dump(people, out_file, indent=2, ensure_ascii=False)
            print(f"Saved extracted people to {output_path}")

        if people:
            print("\nSample person fields:")
            sample = people[0]
            for key in ["id", "first_name", "last_name", "name", "title", "organization_name", "email", "email_status", "linkedin_url"]:
                if key in sample:
                    print(f"  {key}: {sample.get(key)}")

        await browser.close()


def main():
    args = parse_args()
    asyncio.run(run_extraction(args.url, args.state, args.headless, args.max_pages, args.output))


if __name__ == "__main__":
    main()
