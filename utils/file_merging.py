
import os
import pandas as pd
from main import identify_source_type, process_columns, define_column_tags, read_file

def find_files_by_type(directory='uploads'):
    """Find all data files in the uploads directory by type"""
    files = {
        'indeed_us': [],
        'linkedin_us': [],
        'linkedin_india': [],
        'naukri': []
    }
    
    if not os.path.exists(directory):
        print(f"Creating {directory} directory")
        os.makedirs(directory)
        return files
    
    for file in os.listdir(directory):
        filepath = os.path.join(directory, file)
        if not os.path.isfile(filepath):
            continue
            
        # Skip non-Excel/CSV files
        if not file.lower().endswith(('.xlsx', '.xls', '.csv')):
            continue
            
        # Identify file types based on names
        file_lower = file.lower()
        if 'indeed' in file_lower:
            if 'us' in file_lower:
                files['indeed_us'].append(filepath)
        elif 'linkedin' in file_lower:
            if 'us' in file_lower:
                files['linkedin_us'].append(filepath)
            else:
                files['linkedin_india'].append(filepath)
        elif 'naukri' in file_lower:
            files['naukri'].append(filepath)
    
    return files

def preprocess_file(file_path, source_type):
    """Read and preprocess a file based on its source type"""
    if not file_path:
        return None
    
    print(f"Preprocessing {source_type} file: {os.path.basename(file_path)}")
    
    # Set flags based on source type
    is_naukri = 'naukri' in source_type.lower()
    is_linkedin = 'linkedin' in source_type.lower()
    keep_first_row = is_naukri
    
    # Read the file with appropriate settings
    df = read_file(file_path, keep_first_row=keep_first_row, is_naukri=is_naukri, is_linkedin=is_linkedin)
    
    if df is None:
        print(f"Error: Failed to read {os.path.basename(file_path)}")
        return None
    
    # Process the data with appropriate preprocessing
    source_tags = define_column_tags()[source_type]
    processed_df = process_columns(df, source_type, source_tags)
    
    # Add source column
    processed_df['source'] = source_type
    
    print(f"Successfully preprocessed {os.path.basename(file_path)}: {len(processed_df)} rows")
    return processed_df

def merge_dataframes(df_list, output_file):
    """Merge multiple dataframes and save to CSV"""
    # Filter out None values
    valid_dfs = [df for df in df_list if df is not None]
    
    if not valid_dfs:
        print(f"Error: No valid dataframes to merge")
        return None
    
    print(f"Merging {len(valid_dfs)} dataframes...")
    
    # Get all unique columns across all dataframes
    all_columns = set()
    for df in valid_dfs:
        all_columns.update(df.columns)
    
    # Add missing columns with None values to each dataframe
    for df in valid_dfs:
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
    
    # Concatenate the dataframes
    merged_df = pd.concat(valid_dfs, ignore_index=True)
    
    try:
        merged_df.to_csv(output_file, index=False)
        print(f"Successfully merged data saved to {output_file} ({len(merged_df)} total rows)")
        return merged_df
    except Exception as e:
        print(f"Error saving merged file: {e}")
        return None
