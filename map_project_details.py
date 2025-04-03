
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# Identify rows from each source
linkedin_mask = df['source'] == 'Linkedin_US'
indeed_mask = df['source'] == 'Indeed_US'

# Check if columns exist
if 'project_details' in df.columns and 'status' in df.columns:
    # For LinkedIn rows: Copy project_details to status if status is empty
    mask = linkedin_mask & df['status'].isna() & ~df['project_details'].isna()
    if mask.any():
        print(f"Mapping project_details to status for {mask.sum()} LinkedIn US rows")
        df.loc[mask, 'status'] = df.loc[mask, 'project_details']
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully updated the merged_us_data.csv file")
    
    # Summary of non-null values in status column
    print(f"Status column now has {df['status'].notna().sum()} non-null values")
    print(f"LinkedIn rows with status: {df.loc[linkedin_mask, 'status'].notna().sum()} / {linkedin_mask.sum()}")
    print(f"Indeed rows with status: {df.loc[indeed_mask, 'status'].notna().sum()} / {indeed_mask.sum()}")
else:
    missing_cols = []
    if 'project_details' not in df.columns:
        missing_cols.append('project_details')
    if 'status' not in df.columns:
        missing_cols.append('status')
    print(f"Error: Missing columns in the dataset: {', '.join(missing_cols)}")
