import numpy as np
import pandas as pd
import os
import time
import traceback
from playwright.sync_api import sync_playwright

log_file = '/Users/victorli/Documents/GitHub/Mining Determinants of Child Care Quality/crawler-found-url-log.txt'
def log(message, file=log_file):
    with open(file, 'a') as f:
        f.write(message + '\n')
        print(message)

# df = pd.read_csv('data/scraped_invalid_urls.csv')
# 
# SEARCH_URL = 'https://families.decal.ga.gov/ChildCare/Search'
# FOUND_URL_BASE = 'https://families.decal.ga.gov/ChildCare/'
# with sync_playwright() as p:
#     browser = p.chromium.launch(
#         headless=True,
#         executable_path='chrome-headless-shell-mac-arm64/chrome-headless-shell'
#     )
#     page = browser.new_page()
#     for i in range(df.shape[0]):
#         provider_number = df['ids'][i]
#         page.goto(SEARCH_URL)
#         page.fill('input[id="Content_Main_ProviderSearch_txtLocationName"]', provider_number)
#         page.click('input[id="Content_Main_ProviderSearch_btnSearch"]')
#         time.sleep(1)
#         if page.locator('p[id="lblTotalRecords"]').inner_text() == '':
#             log(f'No results found for {provider_number}')
#             continue
#         elif page.locator('p[id="lblTotalRecords"]').inner_text().split(' ')[-1] != '1':
#             log(f'Multiple results found for {provider_number}, needs manual check')
#             continue
#         else:
#             view_button = page.locator('a[class="lId button btn green btn-block no-print track-action"]')
#             href = view_button.get_attribute('href').strip()
#             log(f'URL for {provider_number}: {FOUND_URL_BASE + href}')
#             df.at[i, 'found_url'] = FOUND_URL_BASE + href

# df.to_csv('data/scraped_invalid_urls.csv', index=False)

DOWNLOAD_BASE_PATH = 'data/downloads/'
df_found = pd.read_csv('data/scraped_found_urls.csv')
df_found = df_found.dropna(subset=['found_url'])

known_ids = {'Content_Main_lblLicenseNumber': 'Provider_Number',
             'Content_Main_lblAdmin': 'admin_name',
             'Content_Main_lblCapacity': 'capacity',
             'Content_Main_lblLiabilityInsurance': 'liability_insurance',
             'Content_Main_lblExemptAgesServed': 'exempt_ages_served',
             'Content_Main_lblFacilityName': 'location',
             'Content_Main_lblAddress': 'address',
             'Content_Main_lblCity': 'city',
             'Content_Main_lblState': 'state',
             'Content_Main_lblZip': 'zip_code',
             'Content_Main_lblPhone': 'phone',
             'Content_Main_lblMonthsOfOperation': 'operation_month',
             'Content_Main_lblDaysOfOperation': 'operation_day',
             'Content_Main_lblHoursOfOperation': 'operation_hours',
             'Content_Main_lblMailStreet': 'mailing_address',
             'Content_Main_lblMailCityStateZip': 'mailing_city_state_zip',
             'Content_Main_lblProgramType': 'program_type',
             'Content_Main_lblRegistrationFee': 'registration_fee',
             'Content_Main_lblActivityFee': 'activity_fee',
             'Content_Main_lblCurrentProgramStatus': 'current_program_status',
             'Content_Main_lblAccreditation': 'accreditation_status',
             'Content_Main_lblActivities': 'activities',
             'Content_Main_lblOtherChildCareType': 'other_child_care_type',
             'Content_Main_lblFinancialInformation': 'financial_information',
             'Content_Main_lblLanguages': 'languages',
             'Content_Main_lblSpecialHours': 'special_hours',
             'Content_Main_lblCurriculum': 'curriculum',
             'Content_Main_lblFamilyEngagement': 'family_engagement',
             'Content_Main_lblVisitMessage': 'visit_message',

             'Content_Main_ctl00': 'service_provided',
             'Content_Main_chkIsAcceptingNewChildren': 'is_accepting_new_children',
             'Content_Main_cblAgesServed_0': 'infant_0_to_12_months',
             'Content_Main_cblAgesServed_1': 'toddler_13mos_to_2yrs',
             'Content_Main_cblAgesServed_2': 'preschool_3yrs_to_4yrs',
             'Content_Main_cblAgesServed_3': 'pre_k_served',
             'Content_Main_cblAgesServed_4': 'school_age_5yrs_plus',
             'Content_Main_cblTransportation_0': 'has_transport_tofrom_home',
             'Content_Main_cblTransportation_1': 'has_transport_tofrom_school',
             'Content_Main_cblTransportation_2': 'has_transport_afterschool_only',
             'Content_Main_cblTransportation_3': 'has_transport_georgiaprek_only',
             'Content_Main_cblTransportation_4': 'has_transport_nearpublictransport',
             'Content_Main_cblTransportation_5': 'has_transport_schoolbus',
             'Content_Main_cblTransportation_6': 'has_transport_fieldtrips',
             'Content_Main_cblTransportation_7': 'has_transport_beforeafterschool',
             'Content_Main_cblMeals_0': 'has_breakfast',
             'Content_Main_cblMeals_1': 'has_lunch',
             'Content_Main_cblMeals_2': 'has_dinner',
             'Content_Main_cblMeals_3': 'has_amsnacks',
             'Content_Main_cblMeals_4': 'has_pmsnacks',
             'Content_Main_cblMeals_5': 'has_specialdiets',
             'Content_Main_cblMeals_6': 'has_infantmeals',
             'Content_Main_cblMeals_7': 'parents_provide_meals',
             'Content_Main_cblCampCare_0': 'has_summercamp',
             'Content_Main_cblCampCare_1': 'has_beforecampcare',
             'Content_Main_cblCampCare_2': 'has_aftercampcare',
             'Content_Main_cblAcceptingChildrenType_0': 'accepts_fulltime_children',
             'Content_Main_cblAcceptingChildrenType_1': 'accepts_parttime_children',
             'Content_Main_cblServicesProvided_0': 'has_caps',
             'Content_Main_cblServicesProvided_5': 'has_headstart',
             'Content_Main_cblServicesProvided_1': 'has_afterschool_only',
             'Content_Main_cblServicesProvided_6': 'has_religionbased',
             'Content_Main_cblServicesProvided_2': 'has_cacfp',
             'Content_Main_cblServicesProvided_7': 'has_schoolagesummercare',
             'Content_Main_cblServicesProvided_3': 'has_drop_in_care',
             'Content_Main_cblServicesProvided_8': 'has_SFSP',
             'Content_Main_cblServicesProvided_4': 'has_evening_care',
             'Content_Main_cblEnvironment_0': 'has_nopets',
             'Content_Main_cblEnvironment_3': 'provide_own_equipment',
             'Content_Main_cblEnvironment_6': 'has_sportsfields',
             'Content_Main_cblEnvironment_8': 'has_videosurveillance',
             'Content_Main_cblEnvironment_1': 'has_outdoorplayareas',
             'Content_Main_cblEnvironment_4': 'has_security',
             'Content_Main_cblEnvironment_7': 'has_tenniscourts',
             'Content_Main_cblEnvironment_9': 'has_webcam',
             'Content_Main_cblEnvironment_2': 'has_pool',
             'Content_Main_cblEnvironment_5': 'smoke_free',
             }

start = 0
# start = df.index[df['Provider_Number'] == 'EX-43141'][0] + 1
rows = []
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            executable_path='chrome-headless-shell-mac-arm64/chrome-headless-shell'
        )
        page = browser.new_page()
        for url in df_found['found_url'].iloc[start:]:
            provider_id = df_found[df_found['found_url'] == url]['ids'].values[0]
            row = {k: None for k in known_ids.keys()}
            unknown = {}

            page.goto(url)
            time.sleep(1)
            if page.url != url:
                log(f'Error loading page for URL {url}, got {page.url} instead')
                continue
            else:
                row['url'] = url

            elements_with_id = page.locator('[id^="Content_Main"]')
            count = elements_with_id.count()

            # load page information based on ids
            for i in range(count):
                element = elements_with_id.nth(i)
                element_id = element.get_attribute("id")
                element_tag = element.evaluate("el => el.tagName.toLowerCase()")
                known_id = known_ids.get(element_id)

                if element_tag == 'span':
                    text = element.inner_text().replace('\n', '\t').strip()
                    if known_id is not None:
                        row[known_id] = text
                    elif element_id is None:
                        continue
                    else:
                        unknown[element_id] = text
                elif element_tag == 'input':
                    input_checked = bool(element.get_attribute("checked"))
                    if known_id is not None:
                        row[known_id] = int(input_checked)
                    else:
                        unknown[element_id] = input_checked

            # load downloadable files
            links = page.locator('a[href^="javascript:__doPostBack"]')
            row['num_downloadable_files'] = links.count()
            for i in range(links.count()):
                link = links.nth(i)
                href = (link.get_attribute("href") or "")
                download_path = DOWNLOAD_BASE_PATH + str(provider_id) + '/'
                if "Content_Main" not in href:
                    continue
                elif "Report" in href:
                    tr = link.locator('xpath=ancestor::tr[1]') 
                    tds = tr.locator('xpath=./td')
                    report_date = tds.nth(1).inner_text().replace(" ", "_").replace(",", "")
                    report_type = tds.nth(4).inner_text().replace(" ", "_")
                    download_path += report_date + '_' + report_type + '.pdf'
                elif "Enforcement" in href:
                    tr = link.locator('xpath=ancestor::tr[1]') 
                    tds = tr.locator('xpath=./td')
                    report_date = tds.nth(3).inner_text().replace(" ", "_").replace(",", "")
                    report_type = tds.nth(1).inner_text().replace(' ','_')
                    download_path += report_date + '_' + report_type + '.pdf'
                else:
                    tr = link.locator('xpath=ancestor::tr[1]')
                    tds = tr.locator('xpath=./td')
                    for j in range(tds.count()):
                        download_path += tds.nth(j).inner_text() + '_'
                    download_path += '.pdf'

                try:
                    with page.expect_download() as dl:
                        link.click()
                    download = dl.value
                    suggested = download.suggested_filename
                    download.save_as(download_path)
                except Exception as e:
                    log(f'Error downloading files for Provider Number {provider_id}')
                    traceback.print_exc()

            row['unknown_ids'] = unknown
            rows.append(row)
            log(f'Scraped Provider Number {provider_id}')

        browser.close()
except Exception as e:
    traceback.print_exc()

out_cols = list(known_ids.values()) + ['unknown_ids'] + ['url', 'num_downloadable_files']
out_df = pd.DataFrame(rows, columns=out_cols)
out_df.to_csv('data/scraped_provider_data_sample10.csv', index=False)




