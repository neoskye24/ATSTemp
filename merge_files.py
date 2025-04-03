
import pandas as pd
import os
from main import read_file, identify_source_type, process_columns, define_column_tags

def merge_naukri_linkedin_india():
    """Merge data from Naukri India and LinkedIn India files"""
    print("Merging Naukri India and LinkedIn India data...")
    
    # Find the appropriate files in uploads directory
    uploads_dir = 'uploads'
    naukri_file = None
    linkedin_file = None
    
    for file in os.listdir(uploads_dir):
        filepath = os.path.join(uploads_dir, file)
        if 'naukri' in file.lower():
            naukri_file = filepath
            print(f"Found Naukri file: {file}")
        elif 'linkedin' in file.lower() and ('india' in file.lower() or not 'us' in file.lower()):
            linkedin_file = filepath
            print(f"Found LinkedIn India file: {file}")
    
    if not naukri_file:
        print("Error: Naukri India file not found in uploads directory")
        return None
    
    if not linkedin_file:
        print("Error: LinkedIn India file not found in uploads directory")
        return None
    
    # Read the files
    naukri_df = read_file(naukri_file, keep_first_row=True, is_naukri=True)
    linkedin_df = read_file(linkedin_file, is_linkedin=True)
    
    if naukri_df is None or linkedin_df is None:
        print("Error: Failed to read one or both files")
        return None
    
    # Process the data with appropriate preprocessing
    source_type_naukri, source_tags_naukri = "Naukri_India", define_column_tags()["Naukri_India"]
    source_type_linkedin, source_tags_linkedin = "Linkedin_India", define_column_tags()["Linkedin_India"]
    
    processed_naukri = process_columns(naukri_df, source_type_naukri, source_tags_naukri)
    processed_linkedin = process_columns(linkedin_df, source_type_linkedin, source_tags_linkedin)
    
    # Add a source column to each dataframe
    processed_naukri['source'] = 'Naukri_India'
    processed_linkedin['source'] = 'Linkedin_India'
    
    # Make sure both dataframes have the same columns
    common_columns = set(processed_naukri.columns).intersection(set(processed_linkedin.columns))
    all_columns = set(processed_naukri.columns).union(set(processed_linkedin.columns))
    
    # Add missing columns with None values
    for col in all_columns:
        if col not in processed_naukri.columns:
            processed_naukri[col] = None
        if col not in processed_linkedin.columns:
            processed_linkedin[col] = None
    
    # Concatenate the dataframes
    merged_df = pd.concat([processed_naukri, processed_linkedin], ignore_index=True)
    
    # Generate output filename
    output_path = "merged_india_data.csv"
    
    try:
        merged_df.to_csv(output_path, index=False)
        print(f"Merged data saved as CSV to {output_path}")
        return merged_df
    except Exception as e:
        print(f"Error saving merged file: {e}")
        return merged_df

def merge_indeed_linkedin_us():
    """Merge data from Indeed US and LinkedIn US files"""
    print("Merging Indeed US and LinkedIn US data...")
    
    # Find the appropriate files in uploads directory
    uploads_dir = 'uploads'
    indeed_file = None
    linkedin_file = None
    
    for file in os.listdir(uploads_dir):
        filepath = os.path.join(uploads_dir, file)
        if 'indeed' in file.lower() and 'us' in file.lower():
            indeed_file = filepath
            print(f"Found Indeed US file: {file}")
        elif 'linkedin' in file.lower() and 'us' in file.lower():
            linkedin_file = filepath
            print(f"Found LinkedIn US file: {file}")
    
    if not indeed_file:
        print("Error: Indeed US file not found in uploads directory")
        return None
    
    if not linkedin_file:
        print("Error: LinkedIn US file not found in uploads directory")
        return None
    
    # Read the files
    indeed_df = read_file(indeed_file)
    linkedin_df = read_file(linkedin_file, is_linkedin=True)
    
    if indeed_df is None or linkedin_df is None:
        print("Error: Failed to read one or both files")
        return None
    
    # Process the data with appropriate preprocessing
    source_type_indeed, source_tags_indeed = "Indeed_US", define_column_tags()["Indeed_US"]
    source_type_linkedin, source_tags_linkedin = "Linkedin_US", define_column_tags()["Linkedin_US"]
    
    processed_indeed = process_columns(indeed_df, source_type_indeed, source_tags_indeed)
    processed_linkedin = process_columns(linkedin_df, source_type_linkedin, source_tags_linkedin)
    
    # Add a source column to each dataframe
    processed_indeed['source'] = 'Indeed_US'
    processed_linkedin['source'] = 'Linkedin_US'
    
    # Make sure both dataframes have the same columns
    common_columns = set(processed_indeed.columns).intersection(set(processed_linkedin.columns))
    all_columns = set(processed_indeed.columns).union(set(processed_linkedin.columns))
    
    # Add missing columns with None values
    for col in all_columns:
        if col not in processed_indeed.columns:
            processed_indeed[col] = None
        if col not in processed_linkedin.columns:
            processed_linkedin[col] = None
    
    # Concatenate the dataframes
    merged_df = pd.concat([processed_indeed, processed_linkedin], ignore_index=True)
    
    # Generate output filename
    output_path = "merged_us_data.csv"
    
    try:
        merged_df.to_csv(output_path, index=False)
        print(f"Merged data saved as CSV to {output_path}")
        return merged_df
    except Exception as e:
        print(f"Error saving merged file: {e}")
        return merged_df

if __name__ == "__main__":
    print("Starting to merge files...")
    
    # Create merged India data
    india_data = merge_naukri_linkedin_india()
    if india_data is not None:
        print(f"Successfully merged India data: {len(india_data)} rows")
    
    # Create merged US data
    us_data = merge_indeed_linkedin_us()
    if us_data is not None:
        print(f"Successfully merged US data: {len(us_data)} rows")
