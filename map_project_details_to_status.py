
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# Identify LinkedIn US records
linkedin_mask = df['source'] == 'Linkedin_US'

# Check for necessary columns
if 'project_details' in df.columns and 'status' in df.columns:
    # Count records before update
    status_empty_count = (linkedin_mask & df['status'].isna() & df['project_details'].notna()).sum()
    print(f"Found {status_empty_count} LinkedIn US records with project_details but no status")
    
    # Map project_details to status for LinkedIn records with empty status
    if status_empty_count > 0:
        df.loc[linkedin_mask & df['status'].isna() & df['project_details'].notna(), 'status'] = \
            df.loc[linkedin_mask & df['status'].isna() & df['project_details'].notna(), 'project_details']
        print(f"Mapped project_details to status for {status_empty_count} LinkedIn US records")
    
    # Save the updated dataframe
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully updated the merged_us_data.csv file")
    
    # Print stats after update
    print("\nAfter update:")
    print(f"LinkedIn US records with status: {df.loc[linkedin_mask, 'status'].notna().sum()} / {linkedin_mask.sum()}")
    print(f"LinkedIn US records with project_details: {df.loc[linkedin_mask, 'project_details'].notna().sum()} / {linkedin_mask.sum()}")
else:
    missing_cols = []
    if 'project_details' not in df.columns:
        missing_cols.append('project_details')
    if 'status' not in df.columns:
        missing_cols.append('status')
    print(f"Error: Missing columns in the dataset: {', '.join(missing_cols)}")
