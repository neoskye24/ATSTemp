
import pandas as pd

def map_experience_title():
    print("Reading merged_us_data.csv...")
    try:
        df = pd.read_csv('merged_us_data.csv')
    except FileNotFoundError:
        print("Error: merged_us_data.csv file not found!")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return
    
    # Identify rows from each source
    linkedin_mask = df['source'] == 'Linkedin_US'
    indeed_mask = df['source'] == 'Indeed_US'
    
    print(f"Found {linkedin_mask.sum()} LinkedIn US rows and {indeed_mask.sum()} Indeed US rows")
    
    # Create columns if they don't exist
    for col in ['experience', 'current_title', 'position']:
        if col not in df.columns:
            df[col] = None
            print(f"Created missing column: {col}")
    
    # Copy data between columns where appropriate
    # 1. For LinkedIn: copy current_title to experience if empty
    mask = linkedin_mask & df['experience'].isna() & ~df['current_title'].isna()
    if mask.any():
        print(f"Mapping current_title to experience for {mask.sum()} LinkedIn US rows")
        df.loc[mask, 'experience'] = df.loc[mask, 'current_title']
    
    # 2. For LinkedIn: copy experience to current_title if empty
    mask = linkedin_mask & df['current_title'].isna() & ~df['experience'].isna()
    if mask.any():
        print(f"Mapping experience to current_title for {mask.sum()} LinkedIn US rows")
        df.loc[mask, 'current_title'] = df.loc[mask, 'experience']
    
    # 3. For Indeed: copy position to current_title if empty
    mask = indeed_mask & df['current_title'].isna() & ~df['position'].isna()
    if mask.any():
        print(f"Mapping position to current_title for {mask.sum()} Indeed US rows")
        df.loc[mask, 'current_title'] = df.loc[mask, 'position']
    
    # 4. For Indeed: copy current_title to position if empty
    mask = indeed_mask & df['position'].isna() & ~df['current_title'].isna()
    if mask.any():
        print(f"Mapping current_title to position for {mask.sum()} Indeed US rows")
        df.loc[mask, 'position'] = df.loc[mask, 'current_title']
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_us_data.csv', index=False)
    print("Successfully updated the merged_us_data.csv file")
    
    # Summary of non-null values in both columns
    for col in ['experience', 'current_title', 'position']:
        print(f"{col} column now has {df[col].notna().sum()} non-null values")
        print(f"LinkedIn rows with {col}: {df.loc[linkedin_mask, col].notna().sum()} / {linkedin_mask.sum()}")
        print(f"Indeed rows with {col}: {df.loc[indeed_mask, col].notna().sum()} / {indeed_mask.sum()}")

if __name__ == "__main__":
    map_experience_title()
