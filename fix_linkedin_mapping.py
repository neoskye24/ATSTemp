
import pandas as pd
import os

def fix_linkedin_mapping():
    """Fix mapping issues for LinkedIn data in merged_us_data.csv"""
    print("=== FIXING LINKEDIN MAPPING ISSUES ===")
    
    # Check if the file exists
    if not os.path.exists('merged_us_data.csv'):
        print("Error: merged_us_data.csv does not exist.")
        return
    
    # Read the data
    df = pd.read_csv('merged_us_data.csv')
    print(f"Read {len(df)} rows from merged_us_data.csv")
    
    # Identify LinkedIn rows
    linkedin_mask = df['source'] == 'Linkedin_US'
    print(f"Found {linkedin_mask.sum()} LinkedIn US rows")
    
    # Check for columns that might be problematic
    columns = df.columns.tolist()
    print(f"Current columns: {columns}")
    
    # Fix 1: Handle missing columns gracefully
    required_columns = ['name', 'email', 'phone', 'position', 'current_title', 'experience', 'status', 'project_details']
    missing_columns = [col for col in required_columns if col not in columns]
    
    if missing_columns:
        print(f"Missing columns: {missing_columns}")
        for col in missing_columns:
            df[col] = None
            print(f"Added empty column: {col}")
    
    # Fix 2: Map position to current_title if current_title is empty
    linkedin_empty_title = linkedin_mask & df['current_title'].isna() & ~df['position'].isna()
    if linkedin_empty_title.any():
        print(f"Mapping position to current_title for {linkedin_empty_title.sum()} LinkedIn rows")
        df.loc[linkedin_empty_title, 'current_title'] = df.loc[linkedin_empty_title, 'position']
    
    # Fix 3: Handle project_details properly - map to status since they contain the same information
    if 'project_details' in df.columns:
        # Map project_details to status if status is empty for LinkedIn rows
        mask = linkedin_mask & df['status'].isna() & ~df['project_details'].isna()
        if mask.any():
            print(f"Mapping project_details to status for {mask.sum()} LinkedIn rows")
            df.loc[mask, 'status'] = df.loc[mask, 'project_details']
            print("LinkedIn project_details is functionally equivalent to Indeed status - mapped values")
    
    # Fix 4: Ensure position is populated from active_project if needed
    if 'active_project' in df.columns:
        linkedin_empty_position = linkedin_mask & df['position'].isna() & ~df['active_project'].isna()
        if linkedin_empty_position.any():
            print(f"Mapping active_project to position for {linkedin_empty_position.sum()} LinkedIn rows")
            df.loc[linkedin_empty_position, 'position'] = df.loc[linkedin_empty_position, 'active_project']
        
        # Consider removing active_project column if redundant
        if input("Remove active_project column? (y/n): ").lower() == 'y':
            df = df.drop(columns=['active_project'])
            print("Removed active_project column")
    
    # Fix 5: Handle duplicated rows
    before_dedup = len(df)
    if 'email' in df.columns:
        df = df.drop_duplicates(subset=['email'], keep='first')
        num_dupes = before_dedup - len(df)
        if num_dupes > 0:
            print(f"Removed {num_dupes} duplicate rows based on email")
    
    # Save the updated data
    df.to_csv('merged_us_data.csv', index=False)
    print(f"Saved {len(df)} rows to merged_us_data.csv")
    
    # Print final column stats for LinkedIn rows
    for col in df.columns:
        non_null = df.loc[linkedin_mask, col].notna().sum()
        total = linkedin_mask.sum()
        print(f"LinkedIn US: {col} has {non_null}/{total} non-null values ({non_null/total*100:.1f}%)")

if __name__ == "__main__":
    fix_linkedin_mapping()
