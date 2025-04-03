
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# First, check which columns exist
print("Current columns in the dataset:", df.columns.tolist())

# Count of records with data in each column of interest
print(f"Records with active_project data: {df['active_project'].notna().sum()}")
print(f"Records with current_title data: {df['current_title'].notna().sum()}")
print(f"Records with project_details data: {df['project_details'].notna().sum()}")
print(f"Records with experience data: {df['experience'].notna().sum()}")

# Since we've already synchronized experience and current_title 
# We can handle active_project and project_details

# If project_details column exists and has data, you can decide to:
# 1. Move it to a different column if needed
# 2. Or remove it if redundant

# First check if active_project contains data that's not in position column
if 'active_project' in df.columns and 'position' in df.columns:
    unique_data = df['active_project'].notna() & df['position'].isna()
    if unique_data.any():
        print(f"Moving unique active_project data to position for {unique_data.sum()} rows")
        df.loc[unique_data, 'position'] = df.loc[unique_data, 'active_project']
    
# Now we can remove the extra columns if they're redundant
columns_to_drop = []
if 'active_project' in df.columns:
    columns_to_drop.append('active_project')
    
if 'project_details' in df.columns:
    # You can choose to keep or drop project_details
    # If it contains valuable info, you might want to keep it
    if df['project_details'].notna().sum() == 0:
        columns_to_drop.append('project_details')
    else:
        print(f"Note: project_details column contains {df['project_details'].notna().sum()} records with data")
        print("Sample values:", df['project_details'].dropna().head(3).tolist())

if columns_to_drop:
    print(f"Dropping columns: {columns_to_drop}")
    df = df.drop(columns=columns_to_drop)
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully cleaned the merged_us_data.csv file")
    print("Final columns:", df.columns.tolist())
else:
    print("No columns were identified for removal")
