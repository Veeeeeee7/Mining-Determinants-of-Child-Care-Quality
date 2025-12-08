import numpy as np
import pandas as pd
import json
import time
import os
import traceback
from playwright.sync_api import sync_playwright

def create_log_file(path='crawler_log.txt'):
    if os.path.exists(path):
        os.remove(path)
    with open(path, 'w') as f:
        f.write('')

def log(message, file='crawler_log.txt'):
    with open(file, 'a') as f:
        f.write(message + '\n')
        print(message)

def crawler(provider_ids, provider_base_url, search_url, html_ids_dict, downloads_folder):
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, executable_path='chrome-headless-shell-mac-arm64/chrome-headless-shell')
        
        for provider_id in provider_ids:
            row = {}
            errors = ''

            row['provider_id'] = provider_id
            provider_url = provider_base_url + provider_id.split('-')[1]
            compliance_url = provider_url.replace('ChildCare/Detail', 'Provider/Details')

            page = browser.new_page()
            page.goto(provider_url)
            time.sleep(1)
            if page.url != provider_url:
                errors += 'provider_page, '
                page.goto(search_url)
                time.sleep(1)
                found_url = find_url(page, provider_id)
                if found_url == None:
                    row['provider_url'] = None
                    span_data = create_empty_crawled_span_row(html_ids_dict['spans'])
                    checkmark_data = create_empty_crawled_checkmark_row(html_ids_dict['checkmarks'])
                    list_data = create_empty_crawled_list_row(html_ids_dict['lists'])
                    program_type_data = create_empty_crawled_program_type_row()
                    rates_table_data = create_empty_crawled_rates_table_row()
                    downloads_data = create_empty_crawled_downloads_row()
                    compliance_data = create_empty_crawled_compliance_row()
                    row = row | span_data | checkmark_data | list_data | program_type_data | rates_table_data | downloads_data | compliance_data
                    rows.append(row)
                    log(f'Failed to crawl data for provider ID {provider_id}, errors in: [{errors[:-2]}], at URL {provider_url} and {compliance_url}')
                    continue
                page.goto(provider_base_url + found_url)
                time.sleep(1)
            
            row['provider_url'] = page.url

            span_html_ids_dict = html_ids_dict['spans']
            checkmark_html_ids_dict = html_ids_dict['checkmarks']
            list_html_ids_dict = html_ids_dict['lists']

            try:
                span_data = crawl_span(page, span_html_ids_dict)
            except Exception:
                errors += 'spans, '
                span_data = create_empty_crawled_span_row(span_html_ids_dict)
            try:
                checkmark_data = crawl_checkmarks(page, checkmark_html_ids_dict)
            except Exception:
                errors += 'checkmarks, '
                checkmark_data = create_empty_crawled_checkmark_row(checkmark_html_ids_dict)
            try:
                list_data = crawl_list(page, list_html_ids_dict)
            except Exception:
                errors += 'lists, '
                list_data = create_empty_crawled_list_row(list_html_ids_dict)
            try:
                program_type_data = crawl_program_type(page)
            except Exception:
                errors += 'program_type, '
                program_type_data = create_empty_crawled_program_type_row()
            try:
                rates_table_data = crawl_rates_table(page)
            except Exception:
                errors += 'rates_table, '
                rates_table_data = create_empty_crawled_rates_table_row()
            try:
                downloads_data = crawl_pdfs(page, provider_id, downloads_folder)
            except Exception:
                errors += 'downloads, '
                downloads_data = create_empty_crawled_downloads_row()

            page.goto(compliance_url)
            time.sleep(1)
            if page.url != compliance_url:
                page.goto(search_url)
                time.sleep(1)
                found_url = find_url(page, provider_id)
                if found_url == None:
                    errors += 'compliance, '
                    print(row)
                    compliance_data = create_empty_crawled_compliance_row()
                    row = row | span_data | checkmark_data | list_data | program_type_data | rates_table_data | downloads_data | compliance_data
                    log(f'Partially crawled data for provider ID {provider_id}, errors in: [{errors[:-2]}], at URL {provider_url} and {compliance_url}')
                    continue

                page.goto(provider_url.replace('ChildCare/detail', 'Provider/Details') + found_url)
                time.sleep(1)
                if page.url != compliance_url:
                    errors += 'compliance, '
                    compliance_data = create_empty_crawled_compliance_row()
                    row = row | span_data | checkmark_data | list_data | program_type_data | rates_table_data | downloads_data | compliance_data
                    rows.append(row)
                    log(f'Partially crawled data for provider ID {provider_id}, errors in: [{errors[:-2]}], at URL {provider_url} and {compliance_url}')
                    continue
                    
            try:
                compliance_data = crawl_compliance(page)
                row['compliance_url'] = page.url
            except Exception:
                errors += 'compliance, '
                compliance_data = create_empty_crawled_compliance_row()

            if errors != '':
                log(f'Partially crawled data for provider ID {provider_id}, errors in: [{errors[:-2]}], at URL {provider_url} and {compliance_url}')

            row = row | span_data | checkmark_data | list_data | program_type_data | rates_table_data | downloads_data | compliance_data
            rows.append(row)
            log(f'Successfully crawled data for provider ID {provider_id} at URL {provider_url} and {compliance_url}')
            page.close()
            time.sleep(1)

        browser.close()

    return pd.DataFrame(rows)

def find_url(page, provider_id):
    page.fill('input[id="Content_Main_ProviderSearch_txtLocationName"]', provider_id)
    page.click('input[id="Content_Main_ProviderSearch_btnSearch"]')
    time.sleep(1)
    if page.locator('p[id="lblTotalRecords"]').inner_text() == '':
        # log(f'No url found for {provider_id}')
        return None
    elif page.locator('p[id="lblTotalRecords"]').inner_text().split(' ')[-1] != '1':
        # log(f'Multiple urls found for {provider_id}, needs manual check')
        return None
    else:
        view_button = page.locator('a[class="lId button btn green btn-block no-print track-action"]')
        href = view_button.get_attribute('href').strip()

    return href.split('/')[1]

def create_empty_crawled_span_row(html_ids_dict):
    return {k: None for k in html_ids_dict.values()}

def crawl_span(page, html_ids_dict):
    row = create_empty_crawled_span_row(html_ids_dict)
    for html_id, column in html_ids_dict.items():
        element = page.query_selector(f'#{html_id}')
        if element is None:
            continue

        text = element.inner_text().strip().strip(',')
        row[column] = text
    return row

def create_empty_crawled_checkmark_row(html_ids_dict):
    return {k: None for k in html_ids_dict.values()}

def crawl_checkmarks(page, html_ids_dict):
    row = create_empty_crawled_checkmark_row(html_ids_dict)
    for html_id, column in html_ids_dict.items():
        element = page.query_selector(f'#{html_id}')
        if element is None:
            continue

        row[column] = bool(element.get_attribute('checked'))
    return row

def create_empty_crawled_list_row(html_ids_dict):
    return {k: None for k in html_ids_dict.values()}

def crawl_list(page, html_ids_dict):
    row = create_empty_crawled_list_row(html_ids_dict)
    for html_id, column in html_ids_dict.items():
        element = page.query_selector(f'#{html_id}')
        if element is None:
            continue

        str = ''
        ul = element.query_selector('ul')
        if ul is None:
            continue
        lis = ul.query_selector_all('li')
        for li in lis:
            str += li.inner_text().strip() + '\t'
        str = str.strip('\t')
        row[column] = str

    return row

def create_empty_crawled_program_type_row():
    return {"program_type": None, "program_subtype": None}

def crawl_program_type(page, html_id='Content_Main_lblProgramType'):
    row = create_empty_crawled_program_type_row()
    element = page.query_selector(f'#{html_id}')
    if element is None:
        return row
    row['program_type'] = element.inner_text().strip()

    subtype_elements = element.query_selector_all('div')
    str = ''
    for subtype_element in subtype_elements:
        i = subtype_element.query_selector('i')
        str += i.inner_text().strip() + '\t'
    str = str.strip('\t')
    row['program_subtype'] = str

    return row

def create_empty_crawled_rates_table_row():
    columns = ['weekly_full_day_under_1_year', 'weekly_before_school_under_1_year', 'weekly_after_school_under_1_year', 'vacancies_under_1_year', '#_of_rooms_under_1_year', 'staff_child_ratio_under_1_year', 'daily_drop_in_care_under_1_year', 'day_camp_min_under_1_year', 'day_camp_max_under_1_year',
                'weekly_full_day_1_year', 'weekly_before_school_1_year', 'weekly_after_school_1_year', 'vacancies_1_year', '#_of_rooms_1_year', 'staff_child_ratio_1_year', 'daily_drop_in_care_1_year', 'day_camp_min_1_year', 'day_camp_max_1_year',
                'weekly_full_day_2_years', 'weekly_before_school_2_years', 'weekly_after_school_2_years', 'vacancies_2_years', '#_of_rooms_2_years', 'staff_child_ratio_2_years', 'daily_drop_in_care_2_years', 'day_camp_min_2_years', 'day_camp_max_2_years',
                'weekly_full_day_3_years', 'weekly_before_school_3_years', 'weekly_after_school_3_years', 'vacancies_3_years', '#_of_rooms_3_years', 'staff_child_ratio_3_years', 'daily_drop_in_care_3_years', 'day_camp_min_3_years', 'day_camp_max_3_years',
                'weekly_full_day_4_years', 'weekly_before_school_4_years', 'weekly_after_school_4_years', 'vacancies_4_years', '#_of_rooms_4_years', 'staff_child_ratio_4_years', 'daily_drop_in_care_4_years', 'day_camp_min_4_years', 'day_camp_max_4_years',
                'weekly_full_day_5_years_kindergarten', 'weekly_before_school_5_years_kindergarten', 'weekly_after_school_5_years_kindergarten', 'vacancies_5_years_kindergarten', '#_of_rooms_5_years_kindergarten', 'staff_child_ratio_5_years_kindergarten', 'daily_drop_in_care_5_years_kindergarten', 'day_camp_min_5_years_kindergarten', 'day_camp_max_5_years_kindergarten',
                'weekly_full_day_5_years_and_older', 'weekly_before_school_5_years_and_older', 'weekly_after_school_5_years_and_older', 'vacancies_5_years_and_older', '#_of_rooms_5_years_and_older', 'staff_child_ratio_5_years_and_older', 'daily_drop_in_care_5_years_and_older', 'day_camp_min_5_years_and_older', 'day_camp_max_5_years_and_older']
    weekly_rates_data = {k: None for k in columns}

    return weekly_rates_data

def crawl_rates_table(page, html_id='Content_Main_gvFacilityRates'):
    weekly_rates_data = create_empty_crawled_rates_table_row()

    weekly_rates_container = page.query_selector(f'#{html_id}')
    if weekly_rates_container is None:
        return weekly_rates_data
    
    trs = weekly_rates_container.query_selector_all("tr")
    for j in range(1, len(trs)):
        tr = trs[j]
        tds = tr.query_selector_all("td")
        row = tds[0].inner_text().strip().lower().replace('(', '').replace(')', '').replace(' ', '_').replace('/', '_').replace('&', 'and')
        for k in range(1, len(tds)):
            if tds[k].query_selector("span").inner_text().strip() == 'Day Camp (Min-Max):':
                try:
                    values = float(tds[k].query_selector("div").inner_text().strip().replace('$', '').split('-'))
                    weekly_rates_data['day_camp_min' + '_' + row] = values[0]
                    weekly_rates_data['day_camp_max' + '_' + row] = values[1]
                except:
                    weekly_rates_data['day_camp_min' + '_' + row] = np.nan
                    weekly_rates_data['day_camp_max' + '_' + row] = np.nan
            else:
                col = tds[k].query_selector("span").inner_text().strip().lower().replace(' ', '_').replace('/', '_')[:-1]
                try:
                    weekly_rates_data[col + '_' + row] = float(tds[k].query_selector("div").inner_text().strip().replace('$', ''))
                except:
                    weekly_rates_data[col + '_' + row] = np.nan

    return weekly_rates_data

def create_empty_crawled_downloads_row():
    return {"num_downloadable_files": None, "download_path": None}

def crawl_pdfs(page, provider_id, downloads_folder):
    links = page.locator('a[href^="javascript:__doPostBack"]')
    num_downloadable_files = links.count()
    if num_downloadable_files == 0:
        return {"num_downloadable_files": 0, "download_path": None}

    for i in range(links.count()):
        download_path = downloads_folder + str(provider_id) + '/'
        link = links.nth(i)
        href = (link.get_attribute("href") or "")
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

        with page.expect_download() as dl:
            link.click()
        download = dl.value
        download.save_as(download_path)

    return {"num_downloadable_files": num_downloadable_files, "download_path": downloads_folder + str(provider_id) + '/'}

def create_empty_crawled_compliance_row():
    years = [2025, 2024, 2023]
    suffixes = [
        'total_rule_violations',
        'total_rules_met',
        'activities_and_equipment_rules_met',
        'childrens_records_rules_met',
        'facility_rules_met',
        'food_service_rules_met',
        'health_and_hygiene_rules_met',
        'policies_and_procedures_rules_met',
        'staff_records_rules_met',
        'licensure_rules_met',
        'safety_and_discipline_rules_met',
        'staff_children_ratios_and_supervision_rules_met'
    ]
    columns = ['id'] + [f"{year}_compliance_{s}" for year in years for s in suffixes]
    compliance_data = {k: None for k in columns}
    compliance_data['compliance'] = None

    return compliance_data

def crawl_compliance(page):
    compliance_data = create_empty_crawled_compliance_row()

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

        compliance_data[suffix + 'total_rule_violations'] = rule_violations
        compliance_data[suffix + 'total_rules_met'] = total_rules_met

        trs = div.query_selector_all(":scope > div")[2].query_selector_all("tr")
        for tr in trs[1:]:
            tds = tr.query_selector_all("td")
            rule_name = tds[0].inner_text().strip().lower().replace(' ', '_').replace('&', 'and').replace('/', '_').split(":_")[1]
            ratio = tds[2].inner_text().strip().split(": ")[1].split(' of ')
            rules_met = int(ratio[0])
            rules_total = int(ratio[1])

            met_column = suffix + rule_name + '_rules_met'
            compliance_data[met_column] = rules_met

            total_column = suffix + rule_name + '_rules_total'
            compliance_data[total_column] = rules_total

    compliance_img = page.query_selector("#Content_Main_imgCompliance")
    compliance = compliance_img.get_attribute('src').split('/')[-1].split('_FINAL.png')[0]
    compliance_data['compliance'] = compliance

    return compliance_data

if __name__ == '__main__':
    create_log_file()
    
    provider_base_url = 'https://families.decal.ga.gov/ChildCare/Detail/'
    search_url = 'https://families.decal.ga.gov/ChildCare/Search'
    downloads_folder = 'data/crawled_data/downloads/'
    with open('ids.json', 'r') as f:
        ids_dict = json.load(f)

    addditional_data_df = pd.read_csv('data/additional_data/All_Provider_Data.csv')[:20]
    provider_ids = addditional_data_df['Provider_Number'].tolist()

    crawled_df = crawler(provider_ids, provider_base_url, search_url, ids_dict['crawled_columns'], downloads_folder)
    crawled_df.to_csv('data/crawled_data/scraped_provider_data.csv', index=False)
    log(f'crawled_df shape: {crawled_df.shape}')