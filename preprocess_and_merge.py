
import os
import pandas as pd
from main import read_file, identify_source_type, process_columns, define_column_tags

def ensure_uploads_directory():
    """Ensure the uploads directory exists"""
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
        print("Created uploads directory")
    else:
        print("Uploads directory already exists")

def find_files():
    """Find the appropriate files in uploads directory"""
    uploads_dir = 'uploads'
    files = {
        'indeed_us': None,
        'linkedin_us': None,
        'naukri_india': None,
        'linkedin_india': None
    }
    
    for file in os.listdir(uploads_dir):
        filepath = os.path.join(uploads_dir, file)
        
        # Identify file types based on names
        if 'indeed' in file.lower() and 'us' in file.lower():
            files['indeed_us'] = filepath
        elif 'linkedin' in file.lower() and 'us' in file.lower():
            files['linkedin_us'] = filepath
        elif 'naukri' in file.lower():
            files['naukri_india'] = filepath
        elif 'linkedin' in file.lower() and ('india' in file.lower() or not 'us' in file.lower()):
            files['linkedin_india'] = filepath
    
    # Report findings
    for file_type, path in files.items():
        if path:
            print(f"Found {file_type.replace('_', ' ')} file: {os.path.basename(path)}")
        else:
            print(f"Warning: {file_type.replace('_', ' ')} file not found")
    
    return files

def preprocess_file(file_path, source_type, is_naukri=False, is_linkedin=False):
    """Read and preprocess a file based on its source type"""
    if not file_path:
        return None
    
    print(f"\nPreprocessing {source_type} file: {os.path.basename(file_path)}")
    
    # Read the file with appropriate settings
    keep_first_row = is_naukri
    df = read_file(file_path, keep_first_row=keep_first_row, is_naukri=is_naukri, is_linkedin=is_linkedin)
    
    if df is None:
        print(f"Error: Failed to read {source_type} file")
        return None
    
    # Process the data with appropriate preprocessing
    source_tags = define_column_tags()[source_type]
    processed_df = process_columns(df, source_type, source_tags)
    
    # Add source column
    processed_df['source'] = source_type
    
    print(f"Successfully preprocessed {source_type} data: {len(processed_df)} rows")
    return processed_df

def merge_dataframes(df1, df2, source1, source2, output_file):
    """Merge two dataframes and save to CSV"""
    if df1 is None or df2 is None:
        print(f"Error: Cannot merge {source1} and {source2} - one or both datasets are missing")
        return None
    
    print(f"\nMerging {source1} and {source2} data...")
    
    # Make sure both dataframes have the same columns
    all_columns = set(df1.columns).union(set(df2.columns))
    
    # Add missing columns with None values to each dataframe
    for col in all_columns:
        if col not in df1.columns:
            df1[col] = None
        if col not in df2.columns:
            df2[col] = None
    
    # Concatenate the dataframes
    merged_df = pd.concat([df1, df2], ignore_index=True)
    
    try:
        merged_df.to_csv(output_file, index=False)
        print(f"Successfully merged data saved to {output_file} ({len(merged_df)} total rows)")
        return merged_df
    except Exception as e:
        print(f"Error saving merged file: {e}")
        return merged_df

def main():
    """Main function to preprocess and merge all data"""
    print("Starting preprocessing and merging workflow...")
    
    # Ensure uploads directory exists
    ensure_uploads_directory()
    
    # Find files
    files = find_files()
    
    # Process US data files
    indeed_us_df = preprocess_file(
        files['indeed_us'], 
        source_type='Indeed_US'
    )
    
    linkedin_us_df = preprocess_file(
        files['linkedin_us'],
        source_type='Linkedin_US',
        is_linkedin=True
    )
    
    # Process India data files
    naukri_india_df = preprocess_file(
        files['naukri_india'],
        source_type='Naukri_India',
        is_naukri=True
    )
    
    linkedin_india_df = preprocess_file(
        files['linkedin_india'],
        source_type='Linkedin_India',
        is_linkedin=True
    )
    
    # Merge US data
    us_merged = merge_dataframes(
        indeed_us_df, 
        linkedin_us_df,
        'Indeed_US',
        'Linkedin_US',
        'merged_us_data.csv'
    )
    
    # Merge India data
    india_merged = merge_dataframes(
        naukri_india_df,
        linkedin_india_df,
        'Naukri_India',
        'Linkedin_India',
        'merged_india_data.csv'
    )
    
    # Print summary
    print("\nPreprocessing and merging complete!")
    if us_merged is not None:
        print(f"US data: {len(us_merged)} total records merged")
    if india_merged is not None:
        print(f"India data: {len(india_merged)} total records merged")

if __name__ == "__main__":
    main()
