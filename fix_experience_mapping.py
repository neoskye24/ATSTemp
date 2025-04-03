
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# Identify rows from each source
linkedin_mask = df['source'] == 'Linkedin_US'
indeed_mask = df['source'] == 'Indeed_US'

# Check if columns exist
if 'experience' in df.columns and 'current_title' in df.columns:
    # Count before changes
    print(f"Before update: Experience column has {df['experience'].notna().sum()} non-null values")
    print(f"Before update: Current title column has {df['current_title'].notna().sum()} non-null values")
    
    # For LinkedIn rows: Copy experience to current_title if current_title is empty
    linkedin_empty_title = linkedin_mask & df['current_title'].isna() & ~df['experience'].isna()
    if linkedin_empty_title.any():
        print(f"Mapping experience to current_title for {linkedin_empty_title.sum()} LinkedIn US rows")
        df.loc[linkedin_empty_title, 'current_title'] = df.loc[linkedin_empty_title, 'experience']
    
    # For Indeed rows: Copy current_title to experience if experience is empty
    indeed_empty_exp = indeed_mask & df['experience'].isna() & ~df['current_title'].isna()
    if indeed_empty_exp.any():
        print(f"Mapping current_title to experience for {indeed_empty_exp.sum()} Indeed US rows")
        df.loc[indeed_empty_exp, 'experience'] = df.loc[indeed_empty_exp, 'current_title']
    
    # Special case: Copy Indeed position to experience for rows with missing experience
    indeed_empty_exp2 = indeed_mask & df['experience'].isna() & df['position'].notna()
    if indeed_empty_exp2.any():
        print(f"Mapping position to experience for {indeed_empty_exp2.sum()} Indeed US rows with missing experience")
        df.loc[indeed_empty_exp2, 'experience'] = df.loc[indeed_empty_exp2, 'position']
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully updated the merged_us_data.csv file")
    
    # Summary of non-null values in both columns
    print(f"After update: Experience column now has {df['experience'].notna().sum()} non-null values")
    print(f"After update: Current title column now has {df['current_title'].notna().sum()} non-null values")
    print(f"LinkedIn rows with current_title: {df.loc[linkedin_mask, 'current_title'].notna().sum()} / {linkedin_mask.sum()}")
    print(f"Indeed rows with experience: {df.loc[indeed_mask, 'experience'].notna().sum()} / {indeed_mask.sum()}")
else:
    missing_cols = []
    if 'experience' not in df.columns:
        missing_cols.append('experience')
    if 'current_title' not in df.columns:
        missing_cols.append('current_title')
    print(f"Error: Missing columns in the dataset: {', '.join(missing_cols)}")
