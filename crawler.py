from playwright.sync_api import sync_playwright

base_url = 'https://families.decal.ga.gov/ChildCare/detail/'

df = pd.read_csv

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        executable_path='chrome-headless-shell-mac-arm64/chrome-headless-shell'
    )
    page = browser.new_page()
    page.goto('https://example.com')
    print(page.title())
    browser.close()
