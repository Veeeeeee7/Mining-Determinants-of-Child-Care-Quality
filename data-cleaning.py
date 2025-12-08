import numpy as np
import pandas as pd
import os
import json, ast
import re

df = pd.read_csv('data/original_crawled_data/complete_scraped_data.csv')

def reorder_compliance_columns(col_name):
    """
    Reorders column names from {year}_compliance_... to compliance_{year}_...
    e.g. 2025_compliance_total_rules_met -> compliance_2025_total_rules_met
    """
    pattern = r"^(\d{4})_compliance_(.+)$"  # matches 4-digit year at start
    match = re.match(pattern, col_name)
    if match:
        year, rest = match.groups()
        return f"compliance_{year}_{rest}"
    return col_name  # leave columns that don't match unchanged

def _unknown_to_nan(val):
    if isinstance(val, str) and val.strip().upper() == 'UNKNOWN':
        return pd.NA
    return val

def merge_single_column(df, df_additional, col_df, col_additional, key='Provider_Number'):
    merged_df = df.copy()
    
    if key not in merged_df.columns or key not in df_additional.columns:
        raise ValueError(f"Key column '{key}' must exist in both dataframes.")
    
    merged_df[key] = df[key].astype(str).str.split('-', n=1).str[-1]
    
    value_map = df_additional.set_index(key)[col_additional]
    
    merged_df[col_df] = merged_df.apply(
        lambda row: row[col_df] if pd.notnull(row[col_df]) and row[col_df] != '' else value_map.get(row[key], row[col_df]),
        axis=1
    )
    
    merged_df[key] = df[key]
    return merged_df

def extract_flag(row, key, boolean=True):
    try:
        data = ast.literal_eval(row)
        if not boolean:
            return data.get(key, pd.NA)
        return bool(data.get(key, False))
    except (ValueError, SyntaxError):
        print("ERROR parsing row:", row)
        return None

def is_one_hot(col):
    unique_vals = set(col.dropna().unique())
    return unique_vals.issubset({0, 1, True, False})

def convert_onehot_to_bool(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")
        
    df[column_name] = df[column_name].astype(bool)
    return df


# extract non_profit and for_profit from unknown_ids
df['non_profit'] = df['unknown_ids'].apply(lambda x: extract_flag(x, 'Content_Main_rblForProfit_1'))
df['for_profit'] = df['unknown_ids'].apply(lambda x: extract_flag(x, 'Content_Main_rblForProfit_0'))
df['slots_available'] = df['unknown_ids'].apply(lambda x: extract_flag(x, 'Content_Main_lblSlotsAvailable', boolean=False))
df['vacancies'] = df['unknown_ids'].apply(lambda x: extract_flag(x, 'Content_Main_lblFCCLHTotalVacancies', boolean=False))


# convert one-hot encoded columns to boolean
one_hot_cols = [col for col in df.columns if is_one_hot(df[col])]
df[one_hot_cols] = df[one_hot_cols].map(lambda x: bool(x) if pd.notna(x) else x)


# remove commas and spaces in address and city
df['address'] = df['address'].str.replace(',', '', regex=False).str.strip()
df['city'] = df['city'].str.replace(',', '', regex=False).str.strip()

city_state_zip = df['mailing_city_state_zip'].fillna('').astype(str).str.split(', ', n=1, expand=True)
df['mailing_city'] = city_state_zip[0].replace('', pd.NA).str.strip()
rest = city_state_zip[1].replace('', pd.NA)
rest_split = rest.fillna('').astype(str).str.split(' - ', n=1, expand=True)
df['mailing_state'] = rest_split[0].replace('', pd.NA).str.strip()
df['mailing_zip'] = pd.to_numeric(rest_split[1].replace('', pd.NA).str.strip(), errors='coerce').astype('Int64')
df.drop(columns=['unknown_ids', 'mailing_city_state_zip'], inplace=True)
df.rename(columns={'zip_code': 'zip'}, inplace=True)
df = df.applymap(_unknown_to_nan)

# merge dfs
df_additional = pd.read_csv('data/preprocessed_provider_data.csv')

df = merge_single_column(df, df_additional, 'location', 'Location')
df['county'] = df_additional['County']
df = merge_single_column(df, df_additional, 'address', 'Address')
df = merge_single_column(df, df_additional, 'city', 'City')
df = merge_single_column(df, df_additional, 'state', 'State')
df = merge_single_column(df, df_additional, 'zip', 'Zip')
df = merge_single_column(df, df_additional, 'mailing_address', 'MailingAddress')
df = merge_single_column(df, df_additional, 'mailing_city', 'MailingCity')
df = merge_single_column(df, df_additional, 'mailing_state', 'MailingState')
df = merge_single_column(df, df_additional, 'mailing_zip', 'MailingZip')
df['email'] = df_additional['Email']
df = merge_single_column(df, df_additional, 'phone', 'Phone')
df = merge_single_column(df, df_additional, 'capacity', 'LicenseCapacity')
# hours open (keep new scraped)
# hours close (keep new scraped)
df_additional = convert_onehot_to_bool(df_additional, 'Infant_0_To_12mos')
df = merge_single_column(df, df_additional, 'infant_0_to_12_months', 'Infant_0_To_12mos')
df_additional = convert_onehot_to_bool(df_additional, 'Toddler_13mos_To_2yrs')
df = merge_single_column(df, df_additional, 'toddler_13mos_to_2yrs', 'Toddler_13mos_To_2yrs')
df_additional = convert_onehot_to_bool(df_additional, 'Preschool_3yrs_To_4yrs')
df = merge_single_column(df, df_additional, 'preschool_3yrs_to_4yrs', 'Preschool_3yrs_To_4yrs')
df_additional = convert_onehot_to_bool(df_additional, 'Pre_K_Served')
df = merge_single_column(df, df_additional, 'pre_k_served', 'Pre_K_Served')
df_additional = convert_onehot_to_bool(df_additional, 'School_Age_5yrs_Plus')
df = merge_single_column(df, df_additional, 'school_age_5yrs_plus', 'School_Age_5yrs_Plus')
df_additional = convert_onehot_to_bool(df_additional, 'Ages_Other_Than_Pre_K_Served')
df['ages_other_than_pre_k_served'] = df_additional['Ages_Other_Than_Pre_K_Served']
df_additional = convert_onehot_to_bool(df_additional, 'CAPS_Enrolled')
df = merge_single_column(df, df_additional, 'has_caps', 'CAPS_Enrolled')
df_additional = convert_onehot_to_bool(df_additional, 'Has_Evening_Care')
df = merge_single_column(df, df_additional, 'has_evening_care', 'Has_Evening_Care')
df_additional = convert_onehot_to_bool(df_additional, 'Has_Drop_In_Care')
df = merge_single_column(df, df_additional, 'has_drop_in_care', 'Has_Drop_In_Care')
df_additional = convert_onehot_to_bool(df_additional, 'Has_School_Age_Summer_Care')
df = merge_single_column(df, df_additional, 'has_schoolagesummercare', 'Has_School_Age_Summer_Care')
df_additional = convert_onehot_to_bool(df_additional, 'Has_Transport_ToFrom_School')
df = merge_single_column(df, df_additional, 'has_transport_tofrom_school', 'Has_Transport_ToFrom_School')
df_additional = convert_onehot_to_bool(df_additional, 'Has_Transport_ToFrom_Home')
df = merge_single_column(df, df_additional, 'has_transport_tofrom_home', 'Has_Transport_ToFrom_Home')
df_additional = convert_onehot_to_bool(df_additional, 'Has_Cacfp')
df = merge_single_column(df, df_additional, 'has_cacfp', 'Has_Cacfp')
df['pre_k_slots_available'] = df_additional['Available_PreK_Slots']
df['pre_k_slots_funded'] = df_additional['Funded_PreK_Slots']
df_additional = convert_onehot_to_bool(df_additional, 'QR_Participant')
df['qr_participant'] = df_additional['QR_Participant']
df_additional = convert_onehot_to_bool(df_additional, 'QR_Rated')
df['qr_rated'] = df_additional['QR_Rated']
df['qr_rating'] = df_additional['QR_Rating']
df_additional = convert_onehot_to_bool(df_additional, 'IsTemporarilyClosed')
df['is_temporarily_closed'] = df_additional['IsTemporarilyClosed']
df['temporary_closure_start_date'] = df_additional['TemporaryClosure_StartDate']
df['temporary_closure_end_date'] = df_additional['TemporaryClosure_EndDate']
# operation_JAN (keep new scraped)
# operation_FEB (keep new scraped)
# operation_MAR (keep new scraped)
# operation_APR (keep new scraped)
# operation_MAY (keep new scraped)
# operation_JUN (keep new scraped)
# operation_JUL (keep new scraped)
# operation_AUG (keep new scraped)
# operation_SEP (keep new scraped)
# operation_OCT (keep new scraped)
# operation_NOV (keep new scraped)
# operation_DEC (keep new scraped)
# operation_OtherSchoolBreak (keep new scraped)
# operation_MO (keep new scraped)
# operation_TU (keep new scraped)
# operation_WE (keep new scraped)
# operation_TH (keep new scraped)
# operation_FR (keep new scraped)
# operation_SA (keep new scraped)
# operation_SU (keep new scraped)
# Program_Type_Child Care Learning Center (keep new scraped)
# Program_Type_Department of Defense (keep new scraped)
# Program_Type_Exempt Only (keep new scraped)
# Program_Type_Family Child Care Learning Home (keep new scraped)
# Program_Type_GA Early Head Start (keep new scraped)
# Program_Type_GA Head Start (keep new scraped)
# Program_Type_Local School System (keep new scraped)
# Program_Type_University (keep new scraped)
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_CCLC')
df['provider_type_cclc'] = df_additional['Provider_Type_CCLC']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_DOD')
df['provider_type_dod'] = df_additional['Provider_Type_DOD']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_EXMT')
df['provider_type_exmt'] = df_additional['Provider_Type_EXMT']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_FCCLH')
df['provider_type_fcclh'] = df_additional['Provider_Type_FCCLH']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_GAEHS')
df['provider_type_gaehs'] = df_additional['Provider_Type_GAEHS']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_GAHS')
df['provider_type_gahs'] = df_additional['Provider_Type_GAHS']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_LSS')
df['provider_type_lss'] = df_additional['Provider_Type_LSS']
df_additional = convert_onehot_to_bool(df_additional, 'Provider_Type_UNIV')
df['provider_type_univ'] = df_additional['Provider_Type_UNIV']
# Accreditation_Status_ACA (keep new scraped)
# Accreditation_Status_ACSI (keep new scraped)
# Accreditation_Status_ACSI|GAC (keep new scraped)
# Accreditation_Status_AMI (keep new scraped)
# Accreditation_Status_AMI|GAC (keep new scraped)
# Accreditation_Status_AMI|SACS (keep new scraped)
# Accreditation_Status_AMS (keep new scraped)
# Accreditation_Status_COA (keep new scraped)
# Accreditation_Status_COA|SACS (keep new scraped)
# Accreditation_Status_GAC (keep new scraped)
# Accreditation_Status_GACS (keep new scraped)
# Accreditation_Status_GAC|AMI (keep new scraped)
# Accreditation_Status_GAC|GAC (keep new scraped)
# Accreditation_Status_GAC|SACS (keep new scraped)
# Accreditation_Status_GAC|SAIS (keep new scraped)
# Accreditation_Status_NAC (keep new scraped)
# Accreditation_Status_NAEYC (keep new scraped)
# Accreditation_Status_NAEYC|COA (keep new scraped)
# Accreditation_Status_NAFCC (keep new scraped)
# Accreditation_Status_NECPA (keep new scraped)
# Accreditation_Status_NLSA (keep new scraped)
# Accreditation_Status_SACS (keep new scraped)
# Accreditation_Status_SACS|ACSI (keep new scraped)
# Accreditation_Status_SACS|AMI (keep new scraped)
# Accreditation_Status_SACS|AMS (keep new scraped)
# Accreditation_Status_SACS|GAC (keep new scraped)
# Accreditation_Status_SACS|SACS (keep new scraped)
# Accreditation_Status_SAIS (keep new scraped)
# Accreditation_Status_SAIS|GAC|SAIS (keep new scraped)
# Accreditation_Status_SAIS|SAIS (keep new scraped)
# Exemption_Category_Day Camp and School Breaks (keep new scraped)
# Exemption_Category_Government (keep new scraped)
df_additional = convert_onehot_to_bool(df_additional, 'Region_CE')
df['region_ce'] = df_additional['Region_CE']
df_additional = convert_onehot_to_bool(df_additional, 'Region_CW')
df['region_cw'] = df_additional['Region_CW']
df_additional = convert_onehot_to_bool(df_additional, 'Region_NE')
df['region_ne'] = df_additional['Region_NE']
df_additional = convert_onehot_to_bool(df_additional, 'Region_NW')
df['region_nw'] = df_additional['Region_NW']
df_additional = convert_onehot_to_bool(df_additional, 'Region_SE')
df['region_se'] = df_additional['Region_SE']
df_additional = convert_onehot_to_bool(df_additional, 'Region_SW')
df['region_sw'] = df_additional['Region_SW']
# CurrentProgramStatus_Open (keep new scraped)
# CurrentProgramStatus_RevocationPending (keep new scraped)
# CurrentProgramStatus_Temporary Closure (keep new scraped)

# fixing column names
df.columns = [col.lower().replace(':', '_').replace('\'', '') for col in df.columns]
# assume your DataFrame is called df

# list of columns to update
cols_to_update = [
    "weekly_full_day_under_1_year",
    "weekly_before_school_under_1_year",
    "weekly_after_school_under_1_year",
    "vacancies_under_1_year",
    "#_of_rooms_under_1_year",
    "staff_child_ratio_under_1_year",
    "daily_drop_in_care_under_1_year",
    "day_camp_min_under_1_year",
    "day_camp_max_under_1_year",
    "weekly_full_day_1_year",
    "weekly_before_school_1_year",
    "weekly_after_school_1_year",
    "vacancies_1_year",
    "#_of_rooms_1_year",
    "staff_child_ratio_1_year",
    "daily_drop_in_care_1_year",
    "day_camp_min_1_year",
    "day_camp_max_1_year",
    "weekly_full_day_2_years",
    "weekly_before_school_2_years",
    "weekly_after_school_2_years",
    "vacancies_2_years",
    "#_of_rooms_2_years",
    "staff_child_ratio_2_years",
    "daily_drop_in_care_2_years",
    "day_camp_min_2_years",
    "day_camp_max_2_years",
    "weekly_full_day_3_years",
    "weekly_before_school_3_years",
    "weekly_after_school_3_years",
    "vacancies_3_years",
    "#_of_rooms_3_years",
    "staff_child_ratio_3_years",
    "daily_drop_in_care_3_years",
    "day_camp_min_3_years",
    "day_camp_max_3_years",
    "weekly_full_day_4_years",
    "weekly_before_school_4_years",
    "weekly_after_school_4_years",
    "vacancies_4_years",
    "#_of_rooms_4_years",
    "staff_child_ratio_4_years",
    "daily_drop_in_care_4_years",
    "day_camp_min_4_years",
    "day_camp_max_4_years",
    "weekly_full_day_5_years_kindergarten",
    "weekly_before_school_5_years_kindergarten",
    "weekly_after_school_5_years_kindergarten",
    "vacancies_5_years_kindergarten",
    "#_of_rooms_5_years_kindergarten",
    "staff_child_ratio_5_years_kindergarten",
    "daily_drop_in_care_5_years_kindergarten",
    "day_camp_min_5_years_kindergarten",
    "day_camp_max_5_years_kindergarten",
    "weekly_full_day_5_years_and_older",
    "weekly_before_school_5_years_and_older",
    "weekly_after_school_5_years_and_older",
    "vacancies_5_years_and_older",
    "#_of_rooms_5_years_and_older",
    "staff_child_ratio_5_years_and_older",
    "daily_drop_in_care_5_years_and_older",
    "day_camp_min_5_years_and_older",
    "day_camp_max_5_years_and_older",
]
rename_dict = {col: f"rates_table_{col}" for col in cols_to_update}
df = df.rename(columns=rename_dict)
df = df.rename(columns={col: reorder_compliance_columns(col) for col in df.columns})
df = df.rename(columns={'languages': 'languages_offered', 'infant_0_to_12_months': 'is_accepting_infant_0_to_12_months', 'toddler_13mos_to_2yrs': 'is_accepting_toddler_13mos_to_2yrs','preschool_3yrs_to_4yrs': 'is_accepting_preschool_3yrs_to_4yrs','pre_k_served': 'is_accepting_pre_k_served','school_age_5yrs_plus': 'is_accepting_school_age_5yrs_plus', 'accepts_fulltime_children' : 'is_accepting_full_time_children', 'accepts_parttime_children': 'is_accepting_part_time_children', 'ages_other_than_pre_k_served': 'is_serving_ages_other_than_pre_k', 'location': 'location_name', 'smoke_free': 'has_smoke_free'})

df['provide_own_equipment'] = ~df['provide_own_equipment'].astype(bool)
df = df.rename(columns={'provide_own_equipment': 'has_equipment_for_children'})
df['parents_provide_meals'] = ~df['parents_provide_meals'].astype(bool)
df = df.rename(columns={'parents_provide_meals': 'has_provided_meals'})

# visit_message is all nan and service_provided is all true
df = df.drop(columns=['visit_message', 'service_provided'])

# assume your DataFrame is called df

df.columns = [col.lower().replace('#', 'num') for col in df.columns]
column_order = [
    "provider_number",
    "location_name",
    "url",
    "address",
    "city",
    "county",
    "state",
    "zip",
    "mailing_address",
    "mailing_city",
    "mailing_state",
    "mailing_zip",
    "phone",
    "email",
    "activities",
    "curriculum",
    "languages_offered",
    "family_engagement",
    "qr_participant",
    "qr_rated",
    "qr_rating",
    "program_type",
    "current_program_status",
    "other_child_care_type",
    "accreditation_status",
    "registration_fee",
    "activity_fee",
    "operation_month",
    "operation_day",
    "operation_hours",
    "special_hours",
    "is_temporarily_closed",
    "temporary_closure_start_date",
    "temporary_closure_end_date",
    "capacity",
    "slots_available",
    "vacancies",
    "pre_k_slots_available",
    "pre_k_slots_funded",
    "admin_name",
    "liability_insurance",
    "exempt_ages_served",
    "financial_information",
    "non_profit",
    "for_profit",
    "provider_type_cclc",
    "provider_type_dod",
    "provider_type_exmt",
    "provider_type_fcclh",
    "provider_type_gaehs",
    "provider_type_gahs",
    "provider_type_lss",
    "provider_type_univ",
    "region_ce",
    "region_cw",
    "region_ne",
    "region_nw",
    "region_se",
    "region_sw",
    "is_accepting_new_children",
    "is_accepting_full_time_children",
    "is_accepting_part_time_children",
    "is_accepting_infant_0_to_12_months",
    "is_accepting_toddler_13mos_to_2yrs",
    "is_accepting_preschool_3yrs_to_4yrs",
    "is_accepting_pre_k_served",
    "is_accepting_school_age_5yrs_plus",
    "is_serving_ages_other_than_pre_k",
    "has_transport_tofrom_home",
    "has_transport_tofrom_school",
    "has_transport_afterschool_only",
    "has_transport_georgiaprek_only",
    "has_transport_nearpublictransport",
    "has_transport_schoolbus",
    "has_transport_fieldtrips",
    "has_transport_beforeafterschool",
    "has_breakfast",
    "has_lunch",
    "has_dinner",
    "has_amsnacks",
    "has_pmsnacks",
    "has_specialdiets",
    "has_infantmeals",
    "has_provided_meals",
    "has_summercamp",
    "has_beforecampcare",
    "has_aftercampcare",
    "has_caps",
    "has_headstart",
    "has_afterschool_only",
    "has_religionbased",
    "has_cacfp",
    "has_schoolagesummercare",
    "has_drop_in_care",
    "has_sfsp",
    "has_evening_care",
    "has_nopets",
    "has_equipment_for_children",
    "has_sportsfields",
    "has_videosurveillance",
    "has_outdoorplayareas",
    "has_security",
    "has_tenniscourts",
    "has_webcam",
    "has_pool",
    "has_smoke_free",
    "rates_table_weekly_full_day_under_1_year",
    "rates_table_weekly_before_school_under_1_year",
    "rates_table_weekly_after_school_under_1_year",
    "rates_table_vacancies_under_1_year",
    "rates_table_num_of_rooms_under_1_year",
    "rates_table_staff_child_ratio_under_1_year",
    "rates_table_daily_drop_in_care_under_1_year",
    "rates_table_day_camp_min_under_1_year",
    "rates_table_day_camp_max_under_1_year",
    "rates_table_weekly_full_day_1_year",
    "rates_table_weekly_before_school_1_year",
    "rates_table_weekly_after_school_1_year",
    "rates_table_vacancies_1_year",
    "rates_table_num_of_rooms_1_year",
    "rates_table_staff_child_ratio_1_year",
    "rates_table_daily_drop_in_care_1_year",
    "rates_table_day_camp_min_1_year",
    "rates_table_day_camp_max_1_year",
    "rates_table_weekly_full_day_2_years",
    "rates_table_weekly_before_school_2_years",
    "rates_table_weekly_after_school_2_years",
    "rates_table_vacancies_2_years",
    "rates_table_num_of_rooms_2_years",
    "rates_table_staff_child_ratio_2_years",
    "rates_table_daily_drop_in_care_2_years",
    "rates_table_day_camp_min_2_years",
    "rates_table_day_camp_max_2_years",
    "rates_table_weekly_full_day_3_years",
    "rates_table_weekly_before_school_3_years",
    "rates_table_weekly_after_school_3_years",
    "rates_table_vacancies_3_years",
    "rates_table_num_of_rooms_3_years",
    "rates_table_staff_child_ratio_3_years",
    "rates_table_daily_drop_in_care_3_years",
    "rates_table_day_camp_min_3_years",
    "rates_table_day_camp_max_3_years",
    "rates_table_weekly_full_day_4_years",
    "rates_table_weekly_before_school_4_years",
    "rates_table_weekly_after_school_4_years",
    "rates_table_vacancies_4_years",
    "rates_table_num_of_rooms_4_years",
    "rates_table_staff_child_ratio_4_years",
    "rates_table_daily_drop_in_care_4_years",
    "rates_table_day_camp_min_4_years",
    "rates_table_day_camp_max_4_years",
    "rates_table_weekly_full_day_5_years_kindergarten",
    "rates_table_weekly_before_school_5_years_kindergarten",
    "rates_table_weekly_after_school_5_years_kindergarten",
    "rates_table_vacancies_5_years_kindergarten",
    "rates_table_num_of_rooms_5_years_kindergarten",
    "rates_table_staff_child_ratio_5_years_kindergarten",
    "rates_table_daily_drop_in_care_5_years_kindergarten",
    "rates_table_day_camp_min_5_years_kindergarten",
    "rates_table_day_camp_max_5_years_kindergarten",
    "rates_table_weekly_full_day_5_years_and_older",
    "rates_table_weekly_before_school_5_years_and_older",
    "rates_table_weekly_after_school_5_years_and_older",
    "rates_table_vacancies_5_years_and_older",
    "rates_table_num_of_rooms_5_years_and_older",
    "rates_table_staff_child_ratio_5_years_and_older",
    "rates_table_daily_drop_in_care_5_years_and_older",
    "rates_table_day_camp_min_5_years_and_older",
    "rates_table_day_camp_max_5_years_and_older",
    "compliance",
    "compliance_2025_total_rule_violations",
    "compliance_2025_total_rules_met",
    "compliance_2024_total_rule_violations",
    "compliance_2024_total_rules_met",
    "compliance_2023_total_rule_violations",
    "compliance_2023_total_rules_met",
    "compliance_2025_activities_and_equipment_rules_met",
    "compliance_2025_activities_and_equipment_rules_total",
    "compliance_2025_childrens_records_rules_met",
    "compliance_2025_childrens_records_rules_total",
    "compliance_2025_evening_care_rules_met",
    "compliance_2025_evening_care_rules_total",
    "compliance_2025_facility_rules_met",
    "compliance_2025_facility_rules_total",
    "compliance_2025_food_service_rules_met",
    "compliance_2025_food_service_rules_total",
    "compliance_2025_health_and_hygiene_rules_met",
    "compliance_2025_health_and_hygiene_rules_total",
    "compliance_2025_organization_rules_met",
    "compliance_2025_organization_rules_total",
    "compliance_2025_policies_and_procedures_rules_met",
    "compliance_2025_policies_and_procedures_rules_total",
    "compliance_2025_safety_rules_met",
    "compliance_2025_safety_rules_total",
    "compliance_2025_staff_records_rules_met",
    "compliance_2025_staff_records_rules_total",
    "compliance_2025_staffing_and_supervision_rules_met",
    "compliance_2025_staffing_and_supervision_rules_total",
    "compliance_2025_sleeping_and_resting_equipment_rules_met",
    "compliance_2025_sleeping_and_resting_equipment_rules_total",
    "compliance_2024_activities_and_equipment_rules_met",
    "compliance_2024_activities_and_equipment_rules_total",
    "compliance_2024_childrens_records_rules_met",
    "compliance_2024_childrens_records_rules_total",
    "compliance_2024_evening_care_rules_met",
    "compliance_2024_evening_care_rules_total",
    "compliance_2024_facility_rules_met",
    "compliance_2024_facility_rules_total",
    "compliance_2024_food_service_rules_met",
    "compliance_2024_food_service_rules_total",
    "compliance_2024_health_and_hygiene_rules_met",
    "compliance_2024_health_and_hygiene_rules_total",
    "compliance_2024_organization_rules_met",
    "compliance_2024_organization_rules_total",
    "compliance_2024_policies_and_procedures_rules_met",
    "compliance_2024_policies_and_procedures_rules_total",
    "compliance_2024_safety_rules_met",
    "compliance_2024_safety_rules_total",
    "compliance_2024_staff_records_rules_met",
    "compliance_2024_staff_records_rules_total",
    "compliance_2024_staffing_and_supervision_rules_met",
    "compliance_2024_staffing_and_supervision_rules_total",
    "compliance_2024_sleeping_and_resting_equipment_rules_met",
    "compliance_2024_sleeping_and_resting_equipment_rules_total",
    "compliance_2023_activities_and_equipment_rules_met",
    "compliance_2023_activities_and_equipment_rules_total",
    "compliance_2023_childrens_records_rules_met",
    "compliance_2023_childrens_records_rules_total",
    "compliance_2023_evening_care_rules_met",
    "compliance_2023_evening_care_rules_total",
    "compliance_2023_facility_rules_met",
    "compliance_2023_facility_rules_total",
    "compliance_2023_food_service_rules_met",
    "compliance_2023_food_service_rules_total",
    "compliance_2023_health_and_hygiene_rules_met",
    "compliance_2023_health_and_hygiene_rules_total",
    "compliance_2023_organization_rules_met",
    "compliance_2023_organization_rules_total",
    "compliance_2023_policies_and_procedures_rules_met",
    "compliance_2023_policies_and_procedures_rules_total",
    "compliance_2023_safety_rules_met",
    "compliance_2023_safety_rules_total",
    "compliance_2023_staff_records_rules_met",
    "compliance_2023_staff_records_rules_total",
    "compliance_2023_staffing_and_supervision_rules_met",
    "compliance_2023_staffing_and_supervision_rules_total",
    "compliance_2023_sleeping_and_resting_equipment_rules_met",
    "compliance_2023_sleeping_and_resting_equipment_rules_total",
    "compliance_2026_total_rule_violations",
    "compliance_2026_total_rules_met",
    "compliance_2026_activities_and_equipment_rules_met",
    "compliance_2026_activities_and_equipment_rules_total",
    "compliance_2026_childrens_records_rules_met",
    "compliance_2026_childrens_records_rules_total",
    "compliance_2026_evening_care_rules_met",
    "compliance_2026_evening_care_rules_total",
    "compliance_2026_facility_rules_met",
    "compliance_2026_facility_rules_total",
    "compliance_2026_food_service_rules_met",
    "compliance_2026_food_service_rules_total",
    "compliance_2026_health_and_hygiene_rules_met",
    "compliance_2026_health_and_hygiene_rules_total",
    "compliance_2026_organization_rules_met",
    "compliance_2026_organization_rules_total",
    "compliance_2026_policies_and_procedures_rules_met",
    "compliance_2026_policies_and_procedures_rules_total",
    "compliance_2026_safety_rules_met",
    "compliance_2026_safety_rules_total",
    "compliance_2026_staff_records_rules_met",
    "compliance_2026_staff_records_rules_total",
    "compliance_2026_staffing_and_supervision_rules_met",
    "compliance_2026_staffing_and_supervision_rules_total",
    "compliance_2026_sleeping_and_resting_equipment_rules_met",
    "compliance_2026_sleeping_and_resting_equipment_rules_total",
    "compliance_2025_licensure_rules_met",
    "compliance_2025_licensure_rules_total",
    "compliance_2025_safety_and_discipline_rules_met",
    "compliance_2025_safety_and_discipline_rules_total",
    "compliance_2025_staff_child_ratios_and_supervision_rules_met",
    "compliance_2025_staff_child_ratios_and_supervision_rules_total",
    "compliance_2024_licensure_rules_met",
    "compliance_2024_licensure_rules_total",
    "compliance_2024_safety_and_discipline_rules_met",
    "compliance_2024_safety_and_discipline_rules_total",
    "compliance_2024_staff_child_ratios_and_supervision_rules_met",
    "compliance_2024_staff_child_ratios_and_supervision_rules_total",
    "compliance_2023_licensure_rules_met",
    "compliance_2023_licensure_rules_total",
    "compliance_2023_safety_and_discipline_rules_met",
    "compliance_2023_safety_and_discipline_rules_total",
    "compliance_2023_staff_child_ratios_and_supervision_rules_met",
    "compliance_2023_staff_child_ratios_and_supervision_rules_total",
    "compliance_2026_licensure_rules_met",
    "compliance_2026_licensure_rules_total",
    "compliance_2026_safety_and_discipline_rules_met",
    "compliance_2026_safety_and_discipline_rules_total",
    "compliance_2026_staff_child_ratios_and_supervision_rules_met",
    "compliance_2026_staff_child_ratios_and_supervision_rules_total",
    "compliance_2022_total_rule_violations",
    "compliance_2022_total_rules_met",
    "compliance_2022_activities_and_equipment_rules_met",
    "compliance_2022_activities_and_equipment_rules_total",
    "compliance_2022_childrens_records_rules_met",
    "compliance_2022_childrens_records_rules_total",
    "compliance_2022_evening_care_rules_met",
    "compliance_2022_evening_care_rules_total",
    "compliance_2022_facility_rules_met",
    "compliance_2022_facility_rules_total",
    "compliance_2022_food_service_rules_met",
    "compliance_2022_food_service_rules_total",
    "compliance_2022_health_and_hygiene_rules_met",
    "compliance_2022_health_and_hygiene_rules_total",
    "compliance_2022_organization_rules_met",
    "compliance_2022_organization_rules_total",
    "compliance_2022_policies_and_procedures_rules_met",
    "compliance_2022_policies_and_procedures_rules_total",
    "compliance_2022_safety_rules_met",
    "compliance_2022_safety_rules_total",
    "compliance_2022_staff_records_rules_met",
    "compliance_2022_staff_records_rules_total",
    "compliance_2022_staffing_and_supervision_rules_met",
    "compliance_2022_staffing_and_supervision_rules_total",
    "compliance_2022_sleeping_and_resting_equipment_rules_met",
    "compliance_2022_sleeping_and_resting_equipment_rules_total",
    "compliance_2022_licensure_rules_met",
    "compliance_2022_licensure_rules_total",
    "compliance_2022_safety_and_discipline_rules_met",
    "compliance_2022_safety_and_discipline_rules_total",
    "compliance_2022_staff_child_ratios_and_supervision_rules_met",
    "compliance_2022_staff_child_ratios_and_supervision_rules_total",
    "compliance_2021_total_rule_violations",
    "compliance_2021_total_rules_met",
    "compliance_2021_activities_and_equipment_rules_met",
    "compliance_2021_activities_and_equipment_rules_total",
    "compliance_2021_childrens_records_rules_met",
    "compliance_2021_childrens_records_rules_total",
    "compliance_2021_evening_care_rules_met",
    "compliance_2021_evening_care_rules_total",
    "compliance_2021_facility_rules_met",
    "compliance_2021_facility_rules_total",
    "compliance_2021_food_service_rules_met",
    "compliance_2021_food_service_rules_total",
    "compliance_2021_health_and_hygiene_rules_met",
    "compliance_2021_health_and_hygiene_rules_total",
    "compliance_2021_organization_rules_met",
    "compliance_2021_organization_rules_total",
    "compliance_2021_policies_and_procedures_rules_met",
    "compliance_2021_policies_and_procedures_rules_total",
    "compliance_2021_safety_rules_met",
    "compliance_2021_safety_rules_total",
    "compliance_2021_staff_records_rules_met",
    "compliance_2021_staff_records_rules_total",
    "compliance_2021_staffing_and_supervision_rules_met",
    "compliance_2021_staffing_and_supervision_rules_total",
    "compliance_2021_sleeping_and_resting_equipment_rules_met",
    "compliance_2021_sleeping_and_resting_equipment_rules_total",
    "num_downloadable_files",
]

df = df[column_order]

# with open('df_columns.txt', 'w', encoding='utf-8') as f:
#     for col in df.columns:
#         f.write(f"{col}\n")



df.to_csv('data/crawled_data/cleaned_complete_scraped_data.csv', index=False)