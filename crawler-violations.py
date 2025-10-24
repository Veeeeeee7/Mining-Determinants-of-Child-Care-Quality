from playwright.sync_api import sync_playwright
import numpy as np
import pandas as pd
import traceback
import os
import time

LOG_FILE = '/Users/victorli/Documents/GitHub/Mining Determinants of Child Care Quality/crawler-violation-log.txt'
def log(message, file=LOG_FILE):
    with open(file, 'a') as f:
        f.write(message + '\n')
        print(message)

df = pd.read_csv('data/crawled_data/cleaned_complete_scraped_data.csv')

# years = [2025, 2024, 2023]
# suffixes = [
#     'total_rule_violations',
#     'total_rules_met',
#     'activities_and_equipment_rules_met',
#     'childrens_records_rules_met',
#     'facility_rules_met',
#     'food_service_rules_met',
#     'health_and_hygiene_rules_met',
#     'policies_and_procedures_rules_met',
#     'staff_records_rules_met',
#     'licensure_rules_met',
#     'safety_and_discipline_rules_met',
#     'staff_children_ratios_and_supervision_rules_met'
# ]
# columns = ['id'] + [f"{year}_compliance_{s}" for year in years for s in suffixes]

crawled_df = pd.DataFrame(columns=['id', '2025_compliance_total_rule_violations', '2025_compliance_total_rules_met', '2024_compliance_total_rule_violations', '2024_compliance_total_rules_met', '2023_compliance_total_rule_violations', '2023_compliance_total_rules_met'])
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            executable_path='chrome-headless-shell-mac-arm64/chrome-headless-shell'
        )
        page = browser.new_page()

        for i in range(0, df.shape[0]):
            url = df.loc[i, 'url'].replace('ChildCare/detail', 'Provider/Details')
            provider_id = df.loc[i, 'Provider_Number']
            page.goto(url)
            if page.url != url:
                log(f"Provider {provider_id} redirected to {page.url}")
                continue

            for div_id in ['Content_Main_idYear1', 'Content_Main_idYear2', 'Content_Main_idYear3']:
                if div_id == 'Content_Main_idYear1':
                    div = page.query_selector(f"#{div_id}").query_selector_all(":scope > div")[1].query_selector(":scope > div")
                else: div = page.query_selector(f"#{div_id}").query_selector_all(":scope > div")[1]

                year = int(page.query_selector(f"#{div_id}").query_selector_all(":scope > div")[0].query_selector_all("span")[1].inner_text().strip())
                suffix = f"{year}_compliance_"

                inspection_rules_met_row = div.query_selector_all(":scope > div")[0]
                inspection_rules_met_ratio = inspection_rules_met_row.query_selector_all(":scope > div")[0].inner_text().strip().split('/')
                total_rules_met = int(inspection_rules_met_ratio[0])
                total_rules_total = int(inspection_rules_met_ratio[1])

                rule_violation_rows = div.query_selector_all(":scope > div")[1]
                rule_violations = int(rule_violation_rows.query_selector_all("div")[0].query_selector(":scope > div").inner_text().strip())
                state_avg_rule_violations = rule_violation_rows.query_selector_all(":scope > div")[1].query_selector("span").inner_text().strip()
                state_avg_rule_violations = np.nan if state_avg_rule_violations == '' else int(state_avg_rule_violations)

                crawled_df.loc[i, 'id'] = provider_id
                crawled_df.loc[i, suffix + 'total_rule_violations'] = rule_violations
                crawled_df.loc[i, suffix + 'total_rules_met'] = total_rules_met

                trs = div.query_selector_all(":scope > div")[2].query_selector_all("tr")
                for tr in trs[1:]:
                    tds = tr.query_selector_all("td")
                    rule_name = tds[0].inner_text().strip().lower().replace(' ', '_').replace('&', 'and').replace('/', '_').split(":_")[1]
                    ratio = tds[2].inner_text().strip().split(": ")[1].split(' of ')
                    rules_met = int(ratio[0])
                    rules_total = int(ratio[1])

                    met_column = suffix + rule_name + '_rules_met'
                    if met_column not in crawled_df.columns:
                        crawled_df[met_column] = np.nan
                    crawled_df.loc[i, met_column] = rules_met

                    total_column = suffix + rule_name + '_rules_total'
                    if total_column not in crawled_df.columns:
                        crawled_df[total_column] = np.nan
                    crawled_df.loc[i, total_column] = rules_total

            log(f"Successfully retrieved violations data for Provider {provider_id} at {url}")
except Exception as e:
    crawled_df.to_csv('data/crawled_data/scraped_violations0.csv', index=False)
    traceback.print_exc()

crawled_df.to_csv('data/crawled_data/scraped_violations0.csv', index=False)