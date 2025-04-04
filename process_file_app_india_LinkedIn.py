import pandas as pd
import os
import re


def read_file(file_path):
    print('read file for linkedin started')
    if file_path.lower().endswith('.xlsx'):
        print('file is xlsx')
        df = pd.read_excel(file_path, engine='openpyxl', header=1)
    elif file_path.lower().endswith('.csv'):
        print('file is csv')
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format for file: {file_path}")
    print('read file for linkedin completed')
    return df


def extract_parentheses(text):
    print('extract_parentheses Started')
    if pd.isna(text):
        return ('', '')

    match = re.search(r'\((.*?)\)', str(text))
    if match:
        main_text = str(text).split('(')[0].strip()
        parentheses_text = match.group(1).strip()
        print('extract_parentheses Completed')
        return (main_text, parentheses_text)
    else:
        print('extract_parentheses Completed')
        return (str(text), '')


def preprocess_linkedin_india(df):
    """Special preprocessing for LinkedIn India format"""
    print("preprocess_linkedin_india Started")

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # --- Implement name_cols mapping ---
    name_cols = {
        'first_name': ['First Name', 'first name', 'firstname'],
        'last_name': ['Last Name', 'last name', 'lastname']
    }
    for canonical, synonyms in name_cols.items():
        for col in processed_df.columns:
            if col.strip().lower() in [s.lower() for s in synonyms]:
                processed_df.rename(columns={col: canonical}, inplace=True)
                print(f"  Renamed column '{col}' to '{canonical}'")
                break

    # First pass: remove notes, feedback, and headline columns (case-insensitive)
    columns_to_remove = ['feedback', 'Feedback', 'headline', 'Headline']
    existing_cols_to_remove = [col for col in processed_df.columns if col in columns_to_remove]
    if existing_cols_to_remove:
        processed_df = processed_df.drop(columns=existing_cols_to_remove)
        print(f"  Removed columns: {existing_cols_to_remove}")

    # Combine first and last names if both exist
    if 'first_name' in processed_df.columns and 'last_name' in processed_df.columns:
        processed_df['name'] = processed_df['first_name'].fillna('') + ' ' + processed_df['last_name'].fillna('')
        processed_df['name'] = processed_df['name'].str.strip()
        print("  Combined first_name and last_name into name column")

    # Define the expected column structure for LinkedIn India
    expected_cols = ['first_name', 'last_name', 'location', 'current_title','current_company', 'email', 'phone', 'profile_url', 'active_project','notes','annual_salary','notice_period']

    # Check if columns are unnamed (like "Unnamed: 0", "Unnamed: 1", etc.)
    unnamed_pattern = any(col.startswith('Unnamed:') for col in processed_df.columns)

    if unnamed_pattern:
        print("Detected unnamed columns - applying standard LinkedIn column names")
        # Directly assign new column names based on position
        new_columns = []
        for i in range(len(processed_df.columns)):
            if i < len(expected_cols):
                new_columns.append(expected_cols[i])
            else:
                new_columns.append(f'extra_column_{i+1}')
        processed_df.columns = new_columns
        print(f"  Applied standard column names: {new_columns}")
    else:
        # If columns are already named, create a mapping from original columns to expected columns
        rename_map = {}
        for i, expected_col in enumerate(expected_cols):
            if i < len(processed_df.columns):
                rename_map[processed_df.columns[i]] = expected_col
        if rename_map:
            processed_df = processed_df.rename(columns=rename_map)
            print(f"  Renamed columns according to expected LinkedIn structure")
            print(f"  Column mapping: {rename_map}")

    # If there are more columns than expected, keep them with original names
    if len(processed_df.columns) > len(expected_cols):
        extra_cols = processed_df.columns[len(expected_cols):]
        print(f"  Keeping additional columns: {list(extra_cols)}")

    # Split active_project column to extract content in parentheses
    if 'active_project' in processed_df.columns:
        split_results = processed_df['active_project'].apply(extract_parentheses)
        processed_df['active_project'] = split_results.str[0]  # Text before parentheses
        processed_df['status'] = split_results.str[1]  # Text inside parentheses as status
        processed_df['position'] = processed_df['active_project']
        print("Split active_project column - extracted content in parentheses to 'status' column")
        print("Mapped 'active_project' to 'position'")
    # Check for the Notes column
    if 'Notes' in processed_df.columns or 'notes' in processed_df.columns:
        notes_col = 'Notes' if 'Notes' in processed_df.columns else 'notes'

        def extract_after_colon(note):
            if pd.isna(note):
                return ''
            try:
                # Convert to string and split on pipe character
                note_str = str(note)
                parts = note_str.split('|')
                # For each part, if a colon exists, take the text after it
                extracted = [p.split(':', 1)[1].strip() for p in parts if ':' in p]
                # Join the extracted parts with a pipe
                return '|'.join(extracted)
            except Exception as e:
                print(f"Error processing note: {e}")
                return str(note)

        processed_df[notes_col] = processed_df[notes_col].apply(
            extract_after_colon)

    # Merge first and last name into a single 'name' column (again, in case previous combination was overwritten)
    if 'first_name' in processed_df.columns and 'last_name' in processed_df.columns:
        processed_df['name'] = processed_df['first_name'].fillna('') + ' ' + processed_df['last_name'].fillna('')
        processed_df['name'] = processed_df['name'].str.strip()
        print("  Merged 'first_name' and 'last_name' into 'name' column")

        # Rearrange columns to make 'name' the first column
        cols = list(processed_df.columns)
        if 'name' in cols:
            cols.remove('name')
        if 'first_name' in cols:
            cols.remove('first_name')
        if 'last_name' in cols:
            cols.remove('last_name')
        processed_df = processed_df[['name'] + cols]

        # Drop the original first_name and last_name columns if they exist
        columns_to_drop = []
        if 'first_name' in processed_df.columns:
            columns_to_drop.append('first_name')
        if 'last_name' in processed_df.columns:
            columns_to_drop.append('last_name')
        if columns_to_drop:
            processed_df = processed_df.drop(columns=columns_to_drop, axis=1)
            print(f"  Removed {columns_to_drop} columns")
        print("  Moved 'name' to be the first column")

    # Final pass: Ensure Notes and Feedback columns are removed
    cols_to_remove = [col for col in processed_df.columns if col in ['Feedback', 'feedback']]
    if cols_to_remove:
        processed_df = processed_df.drop(columns=cols_to_remove)
        print(f"  Final cleanup: Removed {cols_to_remove} columns")
        print("preprocess_linkedin_india Completed")

    return processed_df

# Process LinkedIn files
def process_Linkedin_india(linkedin_files):
    print('process_Linkedin_india Started')
    linkedin_dfs = []
    for file in linkedin_files:
        df = read_file(file)
        print('read file is completed')
        # df.head()
        df = preprocess_linkedin_india(df)
        # Optionally add a source column for later identification
        df['source'] = 'linkedin_India'
        linkedin_dfs.append(df)

    # Merge all processed DataFrames into one
    merged_df_linkedin = pd.concat(linkedin_dfs, ignore_index=True)
    print('Linkedin merged Completed. ',merged_df_linkedin.dtypes)
    print('process_Linkedin_india Completed')
    return merged_df_linkedin

    # Optional: Save the merged result to a new Excel file
    # merged_df.to_excel("merged_linkedin_india_data.xlsx", index=False)
