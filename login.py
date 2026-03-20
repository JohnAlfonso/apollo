import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

LOGIN_URL = "https://app.apollo.io/#/login?redirectTo=https%3A%2F%2Fapp.apollo.io%2F%23%2Fhome"
STATE_FILE = "apollo_state.json"

EMAIL = "Neuron@lgame.space"
PASSWORD = "123qwe!@#QWE"


async def first_visible(page, selectors):
    for selector in selectors:
        loc = page.locator(selector)
        try:
            count = await loc.count()
            if count > 0:
                for i in range(count):
                    item = loc.nth(i)
                    if await item.is_visible():
                        print(f"Matched selector: {selector}")
                        return item
        except Exception as e:
            print(f"Selector failed: {selector} -> {e}")
    return None


async def fill_login_form(page, email, password):
    email_selectors = [
        "input[name='email']",
        "input[type='email']",
        "input[autocomplete='email']",
        "input[placeholder*='Email' i]",
    ]

    password_selectors = [
        "input[name='password']",
        "input[type='password']",
        "input[autocomplete='current-password']",
        "input[placeholder*='Password' i]",
    ]

    login_button_selectors = [
        "button[type='submit']",
        "button:has-text('Log In')",
        "button:has-text('Login')",
    ]

    email_locator = await first_visible(page, email_selectors)
    password_locator = await first_visible(page, password_selectors)
    login_button = await first_visible(page, login_button_selectors)

    if not email_locator:
        raise RuntimeError("Could not find email input.")
    if not password_locator:
        raise RuntimeError("Could not find password input.")
    if not login_button:
        raise RuntimeError("Could not find login button.")

    await email_locator.click()
    await email_locator.fill(email)
    print("Filled email")

    await password_locator.click()
    await password_locator.fill(password)
    print("Filled password")

    return login_button


async def wait_until_logged_in(page, timeout_ms=180000):
    """
    Wait until login appears successful.
    We treat leaving the login page and/or landing on an app page as success.
    """
    print("Waiting for manual verification and successful login...")

    end_time = asyncio.get_event_loop().time() + timeout_ms / 1000

    while asyncio.get_event_loop().time() < end_time:
        current_url = page.url
        print(f"Current URL: {current_url}")

        if "#/home" in current_url or "/home" in current_url:
            print("Detected Apollo home page.")
            return True

        if "#/login" not in current_url:
            print("Detected navigation away from login page.")
            return True

        await page.wait_for_timeout(2000)

    return False


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=100,
        )

        context = await browser.new_context(
            viewport={"width": 1440, "height": 900}
        )
        page = await context.new_page()

        print(f"Opening: {LOGIN_URL}")
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")

        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except PlaywrightTimeoutError:
            print("networkidle timeout; continuing")

        await page.screenshot(path="apollo_login_page.png", full_page=True)
        print("Saved: apollo_login_page.png")

        login_button = await fill_login_form(page, EMAIL, PASSWORD)

        await page.screenshot(path="apollo_login_filled.png", full_page=True)
        print("Saved: apollo_login_filled.png")

        print("Clicking login button...")
        await login_button.click()

        await page.screenshot(path="apollo_after_login_click.png", full_page=True)
        print("Saved: apollo_after_login_click.png")

        print("\nManual step required:")
        print("1. Complete the verification in the opened browser.")
        print("2. Finish login.")
        print("3. This script will wait and save session automatically if login succeeds.\n")

        ok = await wait_until_logged_in(page, timeout_ms=180000)

        if not ok:
            print("Timed out waiting for successful login.")
            print("Session was not saved.")
        else:
            await context.storage_state(path=STATE_FILE)
            print(f"Saved logged-in session to: {STATE_FILE}")

            await page.screenshot(path="apollo_logged_in.png", full_page=True)
            print("Saved: apollo_logged_in.png")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
