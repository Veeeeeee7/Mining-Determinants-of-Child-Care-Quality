import pandas as pd
import numpy as np

complete = pd.read_csv('data/original_crawled_data/complete_scraped_data.csv')
compliance = pd.read_csv('data/original_crawled_data/scraped_compliance.csv')
violations = pd.read_csv('data/original_crawled_data/scraped_violations.csv')

complete.drop_duplicates(inplace=True)
compliance.drop_duplicates(inplace=True)
violations.drop_duplicates(inplace=True)

complete.reset_index(drop=True, inplace=True)
compliance.reset_index(drop=True, inplace=True)
violations.reset_index(drop=True, inplace=True)

merged = pd.merge(complete, compliance, left_on='Provider_Number', right_on='id', how='left')
merged = pd.merge(merged, violations, left_on='Provider_Number', right_on='id', how='left')

merged.drop(columns=['id'], inplace=True)
merged.to_csv('data/original_crawled_data/complete_scraped_data.csv', index=False)