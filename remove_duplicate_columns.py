
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# First, check which columns exist
print("Current columns in the dataset:", df.columns.tolist())

# Check if both columns exist and count identical values
if 'experience' in df.columns and 'current_title' in df.columns:
    total_rows = len(df)
    matching_rows = (df['experience'] == df['current_title']).sum()
    null_experience = df['experience'].isna().sum()
    null_current_title = df['current_title'].isna().sum()
    
    print(f"Total rows: {total_rows}")
    print(f"Rows with matching values: {matching_rows} ({matching_rows/total_rows*100:.1f}%)")
    print(f"Rows with null experience: {null_experience}")
    print(f"Rows with null current_title: {null_current_title}")
    
    # Adding current_title to columns to drop
    columns_to_drop = ['current_title']
    print(f"Adding current_title to columns to drop")
    
    # If project_details exists, it may also be redundant with status
    if 'project_details' in df.columns:
        print(f"Adding project_details to columns to drop")
        columns_to_drop.append('project_details')
    
    print(f"Dropping extra columns: {columns_to_drop}")
    df = df.drop(columns=columns_to_drop)
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully cleaned the merged_us_data.csv file")
    print(f"Final columns: {df.columns.tolist()}")
else:
    missing_cols = []
    if 'experience' not in df.columns:
        missing_cols.append('experience')
    if 'current_title' not in df.columns:
        missing_cols.append('current_title')
    print(f"Error: Missing columns in the dataset: {', '.join(missing_cols)}")

# Print a summary of the data
print("\nData summary after cleaning:")
print(f"Total records: {len(df)}")
if 'source' in df.columns:
    source_counts = df['source'].value_counts()
    for source, count in source_counts.items():
        print(f"- {source}: {count} records")
