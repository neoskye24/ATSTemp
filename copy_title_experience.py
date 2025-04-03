
import pandas as pd

print("Reading merged_us_data.csv...")
df = pd.read_csv('merged_us_data.csv')

# Check if columns exist
if 'experience' in df.columns and 'current_title' in df.columns:
    # Get stats before changes
    print(f"Before update: Experience column has {df['experience'].notna().sum()} non-null values")
    print(f"Before update: Current title column has {df['current_title'].notna().sum()} non-null values")
    
    # For each row where one value is null but the other isn't, copy the non-null value
    # Copy current_title to experience where experience is null
    exp_null_mask = df['experience'].isna() & df['current_title'].notna()
    if exp_null_mask.any():
        print(f"Copying current_title to experience for {exp_null_mask.sum()} rows")
        df.loc[exp_null_mask, 'experience'] = df.loc[exp_null_mask, 'current_title']
    
    # Copy experience to current_title where current_title is null
    title_null_mask = df['current_title'].isna() & df['experience'].notna()
    if title_null_mask.any():
        print(f"Copying experience to current_title for {title_null_mask.sum()} rows")
        df.loc[title_null_mask, 'current_title'] = df.loc[title_null_mask, 'experience']
    
    # If both columns have a value, but they're different, make them match
    # This prioritizes experience values over current_title
    both_values_mask = df['experience'].notna() & df['current_title'].notna() & (df['experience'] != df['current_title'])
    if both_values_mask.any():
        print(f"Synchronizing {both_values_mask.sum()} rows where both columns have different values")
        # You can choose which column to prioritize - here we're using experience
        df.loc[both_values_mask, 'current_title'] = df.loc[both_values_mask, 'experience']
    
    # Save the changes
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully updated the merged_us_data.csv file")
    
    # Get stats after changes
    print(f"After update: Experience column now has {df['experience'].notna().sum()} non-null values")
    print(f"After update: Current title column now has {df['current_title'].notna().sum()} non-null values")
    
    # Report on final state
    print(f"Rows where both columns have the same value: {(df['experience'] == df['current_title']).sum()}")
    print(f"Rows where columns have different values: {(df['experience'] != df['current_title']).sum()}")
    
    # Count null values in each column
    experience_null = df['experience'].isna().sum()
    title_null = df['current_title'].isna().sum()
    print(f"Rows with null experience: {experience_null}")
    print(f"Rows with null current_title: {title_null}")
else:
    missing_cols = []
    if 'experience' not in df.columns:
        missing_cols.append('experience')
    if 'current_title' not in df.columns:
        missing_cols.append('current_title')
    print(f"Error: Missing columns in the dataset: {', '.join(missing_cols)}")
