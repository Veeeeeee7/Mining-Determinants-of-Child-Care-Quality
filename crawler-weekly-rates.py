import numpy as np
import pandas as pd
import os
import time
import traceback
from playwright.sync_api import sync_playwright

# LOG_FILE = '/Users/victorli/Documents/GitHub/Mining Determinants of Child Care Quality/crawler-weekly-rates-log.txt'
# def log(message, file=LOG_FILE):
#     with open(file, 'a') as f:
#         f.write(message + '\n')
#         print(message)

# df = pd.read_csv('data/crawled_data/scraped_provider_data.csv')
# try:
#     with sync_playwright() as p:
#         browser = p.chromium.launch(
#             headless=True,
#             executable_path='chrome-headless-shell-mac-arm64/chrome-headless-shell'
#         )
#         page = browser.new_page()

#         columns = ['id',
#                 'weekly_full_day_under_1_year', 'weekly_before_school_under_1_year', 'weekly_after_school_under_1_year', 'vacancies_under_1_year', '#_of_rooms_under_1_year', 'staff_child_ratio_under_1_year', 'daily_drop_in_care_under_1_year', 'day_camp_min_under_1_year', 'day_camp_max_under_1_year',
#                 'weekly_full_day_1_year', 'weekly_before_school_1_year', 'weekly_after_school_1_year', 'vacancies_1_year', '#_of_rooms_1_year', 'staff_child_ratio_1_year', 'daily_drop_in_care_1_year', 'day_camp_min_1_year', 'day_camp_max_1_year',
#                 'weekly_full_day_2_years', 'weekly_before_school_2_years', 'weekly_after_school_2_years', 'vacancies_2_years', '#_of_rooms_2_years', 'staff_child_ratio_2_years', 'daily_drop_in_care_2_years', 'day_camp_min_2_years', 'day_camp_max_2_years',
#                 'weekly_full_day_3_years', 'weekly_before_school_3_years', 'weekly_after_school_3_years', 'vacancies_3_years', '#_of_rooms_3_years', 'staff_child_ratio_3_years', 'daily_drop_in_care_3_years', 'day_camp_min_3_years', 'day_camp_max_3_years',
#                 'weekly_full_day_4_years', 'weekly_before_school_4_years', 'weekly_after_school_4_years', 'vacancies_4_years', '#_of_rooms_4_years', 'staff_child_ratio_4_years', 'daily_drop_in_care_4_years', 'day_camp_min_4_years', 'day_camp_max_4_years',
#                 'weekly_full_day_5_years_kindergarten', 'weekly_before_school_5_years_kindergarten', 'weekly_after_school_5_years_kindergarten', 'vacancies_5_years_kindergarten', '#_of_rooms_5_years_kindergarten', 'staff_child_ratio_5_years_kindergarten', 'daily_drop_in_care_5_years_kindergarten', 'day_camp_min_5_years_kindergarten', 'day_camp_max_5_years_kindergarten',
#                 'weekly_full_day_5_years_and_older', 'weekly_before_school_5_years_and_older', 'weekly_after_school_5_years_and_older', 'vacancies_5_years_and_older', '#_of_rooms_5_years_and_older', 'staff_child_ratio_5_years_and_older', 'daily_drop_in_care_5_years_and_older', 'day_camp_min_5_years_and_older', 'day_camp_max_5_years_and_older']

#         weekly_rates = pd.DataFrame(columns=columns)
#         for i in range(0, df.shape[0]):
#             url = df.loc[i, 'url']
#             provider_id = df.loc[i, 'Provider_Number']
#             page.goto(url)
#             time.sleep(1)
#             weekly_rates.loc[i, 'id'] = provider_id

#             weekly_rates_container = page.query_selector("#Content_Main_gvFacilityRates")
#             if weekly_rates_container is not None:
#                 trs = weekly_rates_container.query_selector_all("tr")
#                 for j in range(1, len(trs)):
#                     tr = trs[j]
#                     tds = tr.query_selector_all("td")
#                     row = tds[0].inner_text().strip().lower().replace('(', '').replace(')', '').replace(' ', '_').replace('/', '_').replace('&', 'and')
#                     for k in range(1, len(tds)):
#                         if tds[k].query_selector("span").inner_text().strip() == 'Day Camp (Min-Max):':
#                             try:
#                                 values = float(tds[k].query_selector("div").inner_text().strip().replace('$', '').split('-'))
#                                 weekly_rates.loc[i, 'day_camp_min' + '_' + row] = values[0]
#                                 weekly_rates.loc[i, 'day_camp_max' + '_' + row] = values[1]
#                             except:
#                                 weekly_rates.loc[i, 'day_camp_min' + '_' + row] = np.nan
#                                 weekly_rates.loc[i, 'day_camp_max' + '_' + row] = np.nan
#                         else:
#                             col = tds[k].query_selector("span").inner_text().strip().lower().replace(' ', '_').replace('/', '_')[:-1]
#                             try:
#                                 weekly_rates.loc[i, col + '_' + row] = float(tds[k].query_selector("div").inner_text().strip().replace('$', ''))
#                             except:
#                                 weekly_rates.loc[i, col + '_' + row] = np.nan
#                 log(f"Successfully retrieved rates table for index {i} at {url}")
#             else:
#                 log(f"No rates table found for index {i} at {url}")
# except:
#     weekly_rates.to_csv('data/crawled_data/scraped_weekly_rates0.csv', index=False)
#     print(traceback.format_exc())
# weekly_rates.to_csv('data/crawled_data/scraped_weekly_rates0.csv', index=False)
# base_dir = 'data/crawled_data/scraped_weekly_rates'
# df = pd.DataFrame()
# for i in range(4):
#     dir = base_dir + str(i) + '.csv'
#     current_df = pd.read_csv(dir)
#     df = pd.concat([df, current_df], axis=0)

# df = df.reset_index(drop=True)
# df.to_csv('data/crawled_data/scraped_weekly_rates.csv', index=False)

df1 = pd.read_csv('data/crawled_data/scraped_provider_data.csv')
df2 = pd.read_csv('data/crawled_data/scraped_weekly_rates.csv')
merged_df = pd.concat([df1, df2], axis=1)
merged_df.to_csv('data/crawled_data/complete_scraped_data.csv', index=False)
