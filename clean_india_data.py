
import pandas as pd
import os

def clean_india_data(df=None):
    """Clean India data columns"""
    if df is None and os.path.exists('merged_india_data.csv'):
        df = pd.read_csv('merged_india_data.csv')
    elif df is None:
        print("No data provided and merged_india_data.csv not found")
        return None
        
    # Check if active_project column exists
    if 'active_project' in df.columns:
        print(f"Found 'active_project' column with {df['active_project'].notna().sum()} non-null values")
        
        # Check if we need to map data before removing
        if 'position' in df.columns:
            # Copy active_project to position where position is null but active_project is not
            mask = df['position'].isna() & df['active_project'].notna()
            if mask.any():
                print(f"Copying {mask.sum()} values from active_project to position")
                df.loc[mask, 'position'] = df.loc[mask, 'active_project']
        
        # Remove the active_project column
        print("Dropping active_project column")
        df = df.drop(columns=['active_project'])
    
    return df

if __name__ == "__main__":
    clean_india_data()
