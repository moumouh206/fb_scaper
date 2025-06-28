import time
from playwright.sync_api import sync_playwright

# The file where the authentication state will be saved
AUTH_FILE = 'playwright_auth_state.json'

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()

    # Go to the Facebook login page
    page.goto('https://www.facebook.com')

    print("\n" + "="*50)
    print("ACTION REQUIRED: Please log in to Facebook in the browser window.")
    print("After you have successfully logged in, close the browser window manually.")
    print("="*50 + "\n")

    # Use page.pause() to stop the script and wait for user interaction.
    # The script will continue ONLY when you click the "Resume" button in the Playwright Inspector.
    page.pause()

    # NOTE: After you are logged in and have browsed a bit,
    # click the "Resume" button (play icon) in the little "Playwright Inspector" window.
    # The script will then save your state and exit.

    print("Saving authentication state to file...")
    # Save the storage state to the file.
    context.storage_state(path=AUTH_FILE)
    print(f"Authentication state saved to {AUTH_FILE}")

    browser.close()