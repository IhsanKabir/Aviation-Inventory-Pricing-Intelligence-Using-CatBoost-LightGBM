import json
from pathlib import Path
from playwright.sync_api import sync_playwright

GRAPHQL_URL = "https://booking.biman-airlines.com/api/graphql"

def send_biman_request_playwright(payload, cookies):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()

        # Apply cookies if needed
        if cookies:
            context.add_cookies(cookies)

        page = context.new_page()

        headers = {
            "Accept": "*/*",
            "Content-Type": "application/json",
            "x-sabre-storefront": "BGDX",     # REQUIRED
            "Application-Id": "SWS1:SBR-GCPDCShpBk:2ceb6478a8",
            "Adrum": "isAjax=true",
            "Origin": "https://booking.biman-airlines.com",
            "Referer": "https://booking.biman-airlines.com/dx/BGDX/"
        }

        response = page.request.post(
            GRAPHQL_URL,
            headers=headers,
            data=json.dumps(payload)
        )

        status = response.status

        if status != 200:
            print(f"[PW] BAD STATUS {status}")
            try:
                print(response.text())
            except:
                pass
            return None

        return response.json()
