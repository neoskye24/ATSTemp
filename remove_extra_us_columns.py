
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# Check if the redundant columns exist
columns_to_drop = []

if 'current_title' in df.columns:
    # Ensure all values are properly transferred to experience before dropping
    if 'experience' in df.columns:
        # Check if any rows would lose data by dropping current_title
        mask = df['current_title'].notna() & df['experience'].isna()
        if mask.any():
            print(f"Copying {mask.sum()} values from current_title to experience before dropping the column")
            df.loc[mask, 'experience'] = df.loc[mask, 'current_title']
    
    columns_to_drop.append('current_title')
    print("Adding current_title to columns to drop")

if 'project_details' in df.columns:
    # Ensure all values are properly transferred to status before dropping
    if 'status' in df.columns:
        # Check if any rows would lose data by dropping project_details
        mask = df['project_details'].notna() & df['status'].isna()
        if mask.any():
            print(f"Copying {mask.sum()} values from project_details to status before dropping the column")
            df.loc[mask, 'status'] = df.loc[mask, 'project_details']
    
    columns_to_drop.append('project_details')
    print("Adding project_details to columns to drop")

# Drop the redundant columns
if columns_to_drop:
    print(f"Dropping extra columns: {columns_to_drop}")
    df = df.drop(columns=columns_to_drop)
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully cleaned the merged_us_data.csv file")
    print("Final columns:", df.columns.tolist())
else:
    print("No extra columns were found to remove")

# Print a summary of the current data
print("\nData summary after cleaning:")
print(f"Total records: {len(df)}")
source_counts = df['source'].value_counts()
for source, count in source_counts.items():
    print(f"- {source}: {count} records")
