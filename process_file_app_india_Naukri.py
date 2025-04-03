import pandas as pd
import os

# naukri_files = ["INDIA DATA/naukri.xlsx"]


def read_file(file_path):
    if file_path.lower().endswith('.xlsx'):
        df = pd.read_excel(file_path, engine='openpyxl')
    elif file_path.lower().endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format for file: {file_path}")
    return df


def preprocess_naukri_data(df):
    """Preprocess Naukri data with standard column mappings"""
    print('preprocess_naukri_data Started')
    # Standard column mappings for Naukri
    df['source'] = 'Naukri_India'

    # Map standard column names
    column_mappings = {
        'Name': 'name',
        'Email ID': 'email',
        'Phone Number': 'phone',
        'Total Experience': 'total_experience',
        'Current Location': 'location',
        'Current Title': 'position',
        'Annual Salary': 'annual_salary',
        'Notice Period': 'notice_period'
    }

    # Apply mappings where columns exist
    for old_col, new_col in column_mappings.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
            if old_col != new_col:  # Only drop if different name
                df = df.drop(columns=[old_col])

    print('preprocess_naukri_data Completed')
    return df


# Process LinkedIn files
def process_Naukri_india(naukri_files):
    print('process_Naukri_india Started')
    naukri_dfs = []
    for file in naukri_files:
        df = read_file(file)
        df = preprocess_naukri_data(df)
        # Optionally add a source column for later identification
        df['source'] = 'Naukri_India'
        naukri_dfs.append(df)

    # Merge all processed DataFrames into one
    merged_df_naukri = pd.concat(naukri_dfs, ignore_index=True)
    print('process_Naukri_india Completed')
    return merged_df_naukri

    # Optional: Save the merged result to a new Excel file
    # merged_df.to_excel("merged_naukri_data.xlsx", index=False)
