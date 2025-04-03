
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# Identify Indeed US rows
indeed_mask = df['source'] == 'Indeed_US'

# Check before counts
experience_count_before = df[indeed_mask]['experience'].notna().sum()
print(f"Before: Indeed US rows with experience: {experience_count_before} / {indeed_mask.sum()}")

# For debugging, let's print the first few Indeed rows that have current_title but no experience
debug_rows = indeed_mask & df['experience'].isna() & df['current_title'].notna()
if debug_rows.any():
    print(f"Found {debug_rows.sum()} Indeed rows with current_title but no experience")
    print(df.loc[debug_rows, ['current_title', 'experience']].head())
else:
    print("No Indeed rows found with current_title but missing experience")

# Direct mapping of current_title to experience for Indeed rows
if indeed_mask.any() and 'current_title' in df.columns:
    # Copy non-null current_title values to experience for Indeed rows with missing experience
    update_mask = indeed_mask & df['experience'].isna() & df['current_title'].notna()
    
    if update_mask.any():
        print(f"Updating {update_mask.sum()} Indeed rows: copying current_title to experience")
        df.loc[update_mask, 'experience'] = df.loc[update_mask, 'current_title']
    else:
        print("No rows to update")
        
    # Save changes
    df.to_csv('merged_us_data.csv', index=False)
    print("Saved changes to merged_us_data.csv")
    
    # Check after counts
    experience_count_after = df[indeed_mask]['experience'].notna().sum()
    print(f"After: Indeed US rows with experience: {experience_count_after} / {indeed_mask.sum()}")
    
    if experience_count_after > experience_count_before:
        print(f"Successfully updated {experience_count_after - experience_count_before} rows")
    else:
        print("No new rows were updated")
else:
    print("Error: Either no Indeed US rows found or current_title column is missing")
