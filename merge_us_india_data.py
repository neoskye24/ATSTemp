
import pandas as pd
import os
from datetime import datetime
from add_stage_column import add_stage_column_to_file
from clean_india_data import clean_india_data
from clean_us_data_columns import clean_us_columns
from copy_title_experience import copy_title_experience
from fix_experience_mapping import fix_experience_mapping
from fix_linkedin_mapping import fix_linkedin_mapping
from map_current_title_to_experience import map_current_title_to_experience
from map_experience_title import map_experience_title
from map_project_details import map_project_details
from map_project_details_to_status import map_project_details_to_status
from merge_calendly_data import merge_calendly_data
from preprocess_database import preprocess_files
from process_calendly_data import process_calendly_data
from remove_active_project import remove_active_project
from remove_duplicate_columns import remove_duplicate_columns
from remove_extra_us_columns import remove_extra_columns
from run_data_processing import process_data

def merge_us_india_data():
    """Merge US and India data with comprehensive preprocessing"""
    print("Starting comprehensive US and India data merging process...")
    
    # First run general preprocessing
    print("Running initial preprocessing...")
    preprocess_files()
    
    try:
        # Process US data
        if os.path.exists('merged_us_data.csv'):
            us_df = pd.read_csv('merged_us_data.csv')
            print(f"Processing US data ({len(us_df)} records)...")
            
            # Apply US-specific cleaning
            us_df = clean_us_columns(us_df)
            us_df = fix_experience_mapping(us_df)
            us_df = fix_linkedin_mapping(us_df)
            us_df = map_current_title_to_experience(us_df)
            us_df = map_experience_title(us_df)
            us_df = map_project_details(us_df)
            us_df = map_project_details_to_status(us_df)
            
            # Remove unnecessary columns
            us_df = remove_extra_columns(us_df)
            us_df = remove_duplicate_columns(us_df)
            
            us_df.to_csv('merged_us_data.csv', index=False)
            print("US data processing complete")
        
        # Process India data
        if os.path.exists('merged_india_data.csv'):
            india_df = pd.read_csv('merged_india_data.csv')
            print(f"Processing India data ({len(india_df)} records)...")
            
            # Apply India-specific cleaning
            india_df = clean_india_data(india_df)
            india_df = remove_active_project(india_df)
            india_df = copy_title_experience(india_df)
            
            # Remove unnecessary columns
            india_df = remove_duplicate_columns(india_df)
            
            india_df.to_csv('merged_india_data.csv', index=False)
            print("India data processing complete")
        
        # Merge US and India data
        if os.path.exists('merged_us_data.csv') and os.path.exists('merged_india_data.csv'):
            us_df = pd.read_csv('merged_us_data.csv')
            india_df = pd.read_csv('merged_india_data.csv')
            
            # Ensure column alignment
            all_columns = set(us_df.columns).union(set(india_df.columns))
            for col in all_columns:
                if col not in us_df.columns:
                    us_df[col] = None
                if col not in india_df.columns:
                    india_df[col] = None
            
            # Merge the dataframes
            merged_df = pd.concat([us_df, india_df], ignore_index=True)
            print(f"Combined data: {len(merged_df)} total records")
            
            # Process Calendly data
            print("Processing Calendly data...")
            process_calendly_data()
            merge_calendly_data()
            
            # Add stage column to final merged data
            output_file = 'merged_all_data.csv'
            merged_df.to_csv(output_file, index=False)
            add_stage_column_to_file(output_file)
            
            # Create backup
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = os.path.join('database', timestamp)
            os.makedirs(backup_dir, exist_ok=True)
            backup_file = os.path.join(backup_dir, output_file)
            merged_df.to_csv(backup_file, index=False)
            
            print(f"\nMerged data saved to:")
            print(f"- {output_file}")
            print(f"- {backup_file}")
            
            # Run final data processing
            process_data()
            
    except Exception as e:
        print(f"Error during merging process: {e}")

if __name__ == "__main__":
    merge_us_india_data()
