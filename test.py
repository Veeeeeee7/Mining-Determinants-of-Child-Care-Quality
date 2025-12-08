import pandas as pd
import os, re

df = pd.read_csv('data/crawled_data/cleaned_complete_scraped_data.csv')

out_dir = 'data/column_json'
os.makedirs(out_dir, exist_ok=True)

if 'provider_number' not in df.columns:
    raise KeyError("provider_number column not found in dataframe")

for col in df.columns:
    if col == 'provider_number':
        continue
    
    current_df = df[['provider_number', col]].copy()
    current_df.to_json(os.path.join(out_dir, f'{col}.json'), orient='records', lines=True)