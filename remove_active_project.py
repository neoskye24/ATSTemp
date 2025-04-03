
import pandas as pd

print("Reading merged_india_data.csv...")
df = pd.read_csv('merged_india_data.csv')

# Check if active_project column exists
if 'active_project' in df.columns:
    print(f"Found active_project column with {df['active_project'].notna().sum()} non-null values")
    
    # Before removing, check if there's any data in active_project that should be preserved
    if 'position' in df.columns:
        # Copy active_project to position where position is null but active_project is not
        mask = df['position'].isna() & df['active_project'].notna()
        if mask.any():
            print(f"Copying {mask.sum()} values from active_project to position before dropping the column")
            df.loc[mask, 'position'] = df.loc[mask, 'active_project']
    
    # Remove the active_project column
    df = df.drop(columns=['active_project'])
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_india_data.csv', index=False)
    print("Successfully removed active_project column from merged_india_data.csv")
    print(f"Final columns: {df.columns.tolist()}")
else:
    print("active_project column not found in the merged_india_data.csv file")

# Print summary of the data
print("\nData summary after removing active_project:")
print(f"Total records: {len(df)}")
if 'source' in df.columns:
    source_counts = df['source'].value_counts()
    for source, count in source_counts.items():
        print(f"- {source}: {count} records")
