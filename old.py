import os
import numpy as np
import pandas as pd
from playwright.sync_api import sync_playwright
import time

log_file = '/Users/victorli/Documents/GitHub/Mining Determinants of Child Care Quality/log.txt'
def log(message, file=log_file):
    with open(file, 'a') as f:
        f.write(message + '\n')
        print(message)


if os.path.exists(log_file):
    os.remove(log_file)
with open(log_file, 'w') as f:
    f.write('')

df = pd.read_csv('data/All_Provider_Data.csv')
log(f'df shape: {df.shape}')

# trim provider numbers to ints
df['Provider_Number'] = df['Provider_Number'].apply(lambda x: str(x.split('-')[1]))
# removing duplicate provider numbers (should only be duplicate provider)
# other duplicates are different strings e.g. 1270 vs 000001270
# however, original link + '000001270' goes to '1270' --> should we assume both are the same?
dup = df[df['Provider_Number'].duplicated(keep=False)]
log(f'\nDuplicate Provider Numbers: {len(dup)} total')
log(dup['Provider_Number'].to_string(index=False))
df = df.drop_duplicates(subset='Provider_Number', keep='first')

log(f'\ndf shape after dropping duplicates: {df.shape}')

one_hot_cols = []
# one-hot encoding for operation calendar variables
calendar_cols = ['Operation_Months', 'Operation_Days']

months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC', 'OtherSchoolBreak']
df['Operation_Months_List'] = df['Operation_Months'].fillna('').str.split()
for m in months:
    df[m] = df['Operation_Months_List'].apply(lambda x: 1 if m in x else 0)
df = df.drop(columns=['Operation_Months_List', 'Operation_Months'])
df = df.rename(columns={m: f'operation_{m}' for m in months})
one_hot_cols.extend([f'operation_{m}' for m in months])

days = ['MO','TU','WE','TH','FR','SA','SU']
df['Operation_Days_List'] = df['Operation_Days'].fillna('').str.split()
for d in days:
    df[d] = df['Operation_Days_List'].apply(lambda x: 1 if d in x else 0)
df = df.drop(columns=['Operation_Days_List', 'Operation_Days'])
df = df.rename(columns={d: f'operation_{d}' for d in days})
one_hot_cols.extend([f'operation_{d}' for d in days])

log(f'\ndf shape after one-hot encoding calendar variables: {df.shape}')

# one-hot encoding for categorical variables
categorical_cols = ['Program_Type', 'Provider_Type', 'Accreditation_Status', 'Exemption_Category', 'Region', 'CurrentProgramStatus']
for col in categorical_cols:
    dummies = pd.get_dummies(df[col], prefix=col).astype('Int64')
    dummies[df[col].isna()] = pd.NA
    df = df.drop(columns=[col])
    df = pd.concat([df, dummies], axis=1)
    one_hot_cols.extend(dummies.columns.tolist())

# set nans to false in 'Accreditation_Status', 'Exemption_Category' dummies
for col_prefix in ['Accreditation_Status', 'Exemption_Category']:
    for c in df.columns:
        if c.startswith(f'{col_prefix}_'):
            df[c] = df[c].fillna(0)

log(f'\ndf shape after one-hot encoding categorical variables: {df.shape}')

# convert open and close times to 24-hour format
df['Hours_Open'] = pd.to_datetime(df['Hours_Open'], format='%I:%M %p').dt.strftime('%H:%M')
df['Hours_Close'] = pd.to_datetime(df['Hours_Close'], format='%I:%M %p').dt.strftime('%H:%M')

# convert all True/False columns to 1/0
bool_cols = df.select_dtypes(include='bool').columns
for col in bool_cols:
    df[col] = df[col].astype('Int64')
one_hot_cols.extend(bool_cols.tolist())

# save preprocessed data
df.to_csv('data/preprocessed_provider_data.csv', index=False)

# null values summary
null_counts = df.isnull().sum()
total_counts = len(df)
null_percent = (null_counts / total_counts * 100).round(2)

summary_table = pd.DataFrame({
    'Non-Null Count': total_counts - null_counts,
    'Null Count': null_counts,
    'Null %': null_percent
})

# Add columns for one_hot columns (1/0)
one_hot_true_percent = df[one_hot_cols].sum() / total_counts * 100
one_hot_false_percent = (total_counts - df[one_hot_cols].sum()) / total_counts * 100

summary_table['% True'] = one_hot_true_percent
summary_table['% False'] = one_hot_false_percent

log(summary_table)
summary_table.to_csv('data/preprocessed_provider_data_summary.csv')