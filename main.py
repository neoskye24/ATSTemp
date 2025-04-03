import pandas as pd
import os
import sys
import shutil
from datetime import datetime

def read_file(file_path, keep_first_row=False, is_naukri=False, is_linkedin=False):
    """Read Excel or CSV file and return a pandas DataFrame."""
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        return None

    # Determine file extension
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()

    try:
        if file_extension == '.csv':
            # Read CSV file
            df = pd.read_csv(file_path)
            print(f"Successfully read CSV file: {file_path}")
        elif file_extension in ['.xlsx', '.xls']:
            # Read Excel file - for LinkedIn files, always skip the first row
            if is_linkedin:
                df = pd.read_excel(file_path, skiprows=1)
                print(f"Successfully read Excel file: {file_path} (skipped first row for LinkedIn)")
            else:
                df = pd.read_excel(file_path)
                print(f"Successfully read Excel file: {file_path}")
        else:
            print(f"Error: Unsupported file format. Please provide a CSV, XLS, or XLSX file.")
            return None

        # Pre-processing: Delete first row unless keep_first_row is True
        if not is_naukri and not is_linkedin and len(df) > 0:
            df = df.iloc[1:].reset_index(drop=True)
            print("Pre-processing: Deleted first row")
        elif is_naukri:
            print("Naukri file detected - keeping first row")
        elif is_linkedin:
            print("LinkedIn file detected - first row was automatically skipped when reading the file")

        return df
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def display_data_info(df):
    """Display information about the DataFrame."""
    if df is None:
        return

    print("\nData Summary:")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Number of columns: {df.shape[1]}")
    print("\nColumn names:")
    for col in df.columns:
        print(f"- {col}")

    print("\nFirst 5 rows:")
    print(df.head())

    print("\nData types:")
    print(df.dtypes)

# Define column tagging dictionaries for different sources
def define_column_tags():
    # Define dictionaries for different sources with focus on Naukri_India
    Naukri_India = {
        'position': ['position', 'job position', 'role', 'job title', 'designation'],
        'name': ['name', 'full name', 'candidate name'],
        'email': ['email', 'email address', 'email id'],
        'phone': ['phone', 'mobile', 'contact', 'phone number'],
        'location': ['current location', 'location', 'city'],
        'total_experience': ['total experience', 'experience', 'exp'],
        'annual_salary': ['annual salary', 'salary', 'ctc', 'current ctc'],
        'notice_period': ['notice period', 'availability to join', 'joining time']
    }

    Linkedin_India = {
        'name': ['name', 'full name', 'candidate name'],
        'first_name': ['first name', 'firstname', 'given name'],
        'last_name': ['last name', 'lastname', 'surname', 'family name'],
        'location': ['location', 'city', 'current location'],
        'current_title': ['current title', 'job title', 'position title'],
        'current_company': ['current company', 'company', 'employer'],
        'email': ['email', 'email address', 'email id'],
        'phone': ['phone', 'mobile', 'contact', 'phone number'],
        'profile_url': ['profile url', 'linkedin url', 'profile link'],
        'position': ['active project', 'current project', 'project']
    }

    Calendly_India = {
        'position': [],
        'name': [],
        'email': [],
        'phone': [],
        'other1': [],
        'other2': [],
        'other3': [],
        'other4': []
    }

    Indeed_US = {
        'name': ['name', 'candidate name', 'full name'],
        'email': ['email', 'email address', 'email id'],
        'phone': ['phone', 'phone number', 'contact', 'mobile'],
        'status': ['status', 'application status', 'job status'],
        'location': ['location', 'city', 'address', 'candidate location'],
        'experience': ['experience', 'years of experience', 'work experience'],
        'position': ['position', 'job title', 'role', 'job position']
    }

    Linkedin_US = {
        'name': ['name', 'full name', 'candidate name'],
        'first_name': ['first name', 'firstname', 'given name'],
        'last_name': ['last name', 'lastname', 'surname', 'family name'],
        'headline': ['headline', 'title', 'position'],
        'location': ['location', 'city', 'current location'],
        'current_title': ['current title', 'job title', 'position title'],
        'current_company': ['current company', 'company', 'employer'],
        'email': ['email', 'email address', 'email id'],
        'phone': ['phone', 'mobile', 'contact', 'phone number'],
        'profile_url': ['profile url', 'linkedin url', 'profile link'],
        'position': ['active project', 'current project', 'project']
    }

    Calendly_US = {
        'name': ['name', 'invitee name', 'full name'],
        'email': ['email', 'invitee email', 'email address'],
        'phone': ['phone', 'phone number', 'contact'],
        'date': ['date', 'scheduled date', 'meeting date'],
        'profile': ['profile', 'user profile', 'candidate profile'],
        'salary': ['salary', 'expected salary', 'current salary'],
        'declaration': ['declaration', 'self declaration', 'legal declaration'],
        'position': ['position', 'job position', 'role'],
        'no-show': ['no-show', 'no show', 'attendance'],
        'status': ['status', 'event status', 'meeting status']
    }

    return {
        'Naukri_India': Naukri_India,
        'Linkedin_India': Linkedin_India,
        'Calendly_India': Calendly_India,
        'Indeed_US': Indeed_US,
        'Linkedin_US': Linkedin_US,
        'Calendly_US': Calendly_US
    }

def identify_source_type(df):
    """Identify which source dictionary best matches the dataframe columns"""
    tags = define_column_tags()

    # Convert all column names to lowercase for matching
    df_columns = [col.lower() for col in df.columns]

    # Score each source type based on column matches
    scores = {}
    for source_name, source_dict in tags.items():
        score = 0
        for category, keywords in source_dict.items():
            for keyword in keywords:
                for col in df_columns:
                    if keyword.lower() in col:
                        score += 1
                        break
        scores[source_name] = score

    # Return the source type with the highest score
    best_match = max(scores, key=scores.get)
    print(f"Detected source type: {best_match} (match score: {scores[best_match]})")
    return best_match, tags[best_match]

def preprocess_linkedin_india(df):
    """Special preprocessing for LinkedIn India format"""
    print("Preprocessing LinkedIn India data...")

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # First pass: remove notes, feedback, and headline columns (case-insensitive)
    columns_to_remove = ['notes', 'feedback', 'Notes', 'Feedback', 'headline', 'Headline']
    existing_cols_to_remove = [col for col in processed_df.columns if col in columns_to_remove]
    if existing_cols_to_remove:
        processed_df = processed_df.drop(columns=existing_cols_to_remove)
        print(f"  Removed columns: {existing_cols_to_remove}")
        
    # Combine first and last names
    if 'first_name' in processed_df.columns and 'last_name' in processed_df.columns:
        processed_df['name'] = processed_df['first_name'].fillna('') + ' ' + processed_df['last_name'].fillna('')
        processed_df['name'] = processed_df['name'].str.strip()
        print("  Combined first_name and last_name into name column")

    # Define the expected column structure for LinkedIn India
    expected_cols = [
        'first_name',
        'last_name',
        'location',
        'current_title',
        'current_company',
        'email',
        'phone',
        'profile_url',
        'active_project'
    ]

    # Check if columns are unnamed (like "Unnamed: 0", "Unnamed: 1", etc.)
    unnamed_pattern = any(col.startswith('Unnamed:') for col in processed_df.columns)

    if unnamed_pattern:
        print("  Detected unnamed columns - applying standard LinkedIn column names")
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

        # Rename the columns
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
        # Create a function to extract text inside parentheses
        def extract_parentheses(text):
            if pd.isna(text):
                return ('', '')

            import re
            # Find content inside parentheses
            match = re.search(r'\((.*?)\)', str(text))
            if match:
                # Get text before parentheses
                main_text = str(text).split('(')[0].strip()
                # Get text inside parentheses
                parentheses_text = match.group(1).strip()
                return (main_text, parentheses_text)
            else:
                return (str(text), '')

        # Apply the function to split the active_project column
        split_results = processed_df['active_project'].apply(extract_parentheses)

        # Create new columns with the split results
        processed_df['active_project'] = split_results.str[0]  # Text before parentheses
        processed_df['status'] = split_results.str[1]  # Text inside parentheses is renamed to status directly

        # Map active_project to position
        processed_df['position'] = processed_df['active_project']
        print("  Split active_project column - extracted content in parentheses to 'status' column")
        print("  Mapped 'active_project' to 'position'")

    # Merge first and last name into a single 'name' column
    if 'first_name' in processed_df.columns and 'last_name' in processed_df.columns:
        processed_df['name'] = processed_df['first_name'].fillna('') + ' ' + processed_df['last_name'].fillna('')
        processed_df['name'] = processed_df['name'].str.strip()
        print("  Merged 'first_name' and 'last_name' into 'name' column")

        # Rearrange columns to make 'name' the first column
        cols = list(processed_df.columns)
        cols.remove('name')

        # Check if columns exist before removing them
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
    cols_to_remove = []
    for col in processed_df.columns:
        if col in ['Notes', 'Feedback', 'notes', 'feedback']:
            cols_to_remove.append(col)

    if cols_to_remove:
        processed_df = processed_df.drop(columns=cols_to_remove)
        print(f"  Final cleanup: Removed {cols_to_remove} columns")

    return processed_df

def preprocess_calendly_india(df):
    """Special preprocessing for Calendly India to keep only specific columns"""
    print("Preprocessing Calendly India data...")

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # Get all column indices (zero-based index)
    all_columns = list(range(len(processed_df.columns)))

    # Columns to keep (3, 6, 11, 19, 21, 23, 27, 29, 31, 42, 43)
    # Convert to zero-based indexing
    columns_to_keep = [2, 5, 10, 18, 20, 22, 26, 28, 30, 41, 42]  # -1 from each

    # Get columns to drop (all except those to keep)
    columns_to_drop = [i for i in all_columns if i not in columns_to_keep]

    # Drop the columns by position
    if columns_to_drop:
        processed_df = processed_df.drop(processed_df.columns[columns_to_drop], axis=1)
        print(f"  Dropped {len(columns_to_drop)} columns, keeping only columns 3, 6, 11, 19, 21, 23, 27, 29, 31, 42, 43")

    # Rename remaining columns to specified names
    new_column_names = [
        'name',
        'email',
        'date',
        'profile',
        'salary',
        'consent',
        'position',
        'source',
        'phone',
        'no-show',
        'status'
    ]

    # Apply new column names
    if len(processed_df.columns) == len(new_column_names):
        processed_df.columns = new_column_names
        print(f"  Renamed columns to: {', '.join(new_column_names)}")
    else:
        print(f"  Warning: Number of kept columns ({len(processed_df.columns)}) doesn't match expected column names ({len(new_column_names)})")
        # Assign basic names if counts don't match
        processed_df.columns = [f'column_{i+1}' for i in range(len(processed_df.columns))]

    return processed_df

def preprocess_indeed_us(df):
    """Special preprocessing for Indeed US to keep only specific columns"""
    print("Preprocessing Indeed US data...")

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # Get all column indices (zero-based index)
    all_columns = list(range(len(processed_df.columns)))

    # Columns to keep (1, 2, 3, 4, 5, 6, 8)
    # Convert to zero-based indexing
    columns_to_keep = [0, 1, 2, 3, 4, 5, 7]  # -1 from each

    # Get columns to drop (all except those to keep)
    columns_to_drop = [i for i in all_columns if i not in columns_to_keep]

    # Drop the columns by position
    if columns_to_drop:
        processed_df = processed_df.drop(processed_df.columns[columns_to_drop], axis=1)
        print(f"  Dropped {len(columns_to_drop)} columns, keeping only columns 1, 2, 3, 4, 5, 6, 8")

    # Rename remaining columns to specified names
    new_column_names = [
        'name',
        'email',
        'phone',
        'status',
        'location',
        'experience',
        'position'
    ]

    # Apply new column names
    if len(processed_df.columns) == len(new_column_names):
        processed_df.columns = new_column_names
        print(f"  Renamed columns to: {', '.join(new_column_names)}")
    else:
        print(f"  Warning: Number of kept columns ({len(processed_df.columns)}) doesn't match expected column names ({len(new_column_names)})")
        # Assign basic names if counts don't match
        processed_df.columns = [f'column_{i+1}' for i in range(len(processed_df.columns))]

    return processed_df

def preprocess_calendly_us(df):
    """Special preprocessing for Calendly US to keep only specific columns"""
    print("Preprocessing Calendly US data...")

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # Get all column indices (zero-based index)
    all_columns = list(range(len(processed_df.columns)))

    # Columns to keep (3, 6, 9, 14, 19, 21, 23, 27, 38, 39)
    # Convert to zero-based indexing
    columns_to_keep = [2, 5, 8, 13, 18, 20, 22, 26, 37, 38]  # -1 from each

    # Get columns to drop (all except those to keep)
    columns_to_drop = [i for i in all_columns if i not in columns_to_keep]

    # Drop the columns by position
    if columns_to_drop:
        processed_df = processed_df.drop(processed_df.columns[columns_to_drop], axis=1)
        print(f"  Dropped {len(columns_to_drop)} columns, keeping only columns 3, 6, 9, 14, 19, 21, 23, 27, 38, 39")

    # Rename remaining columns to specified names
    new_column_names = [
        'name',
        'email',
        'phone',
        'date',
        'profile',
        'salary',
        'declaration',
        'position',
        'no-show',
        'status'
    ]

    # Apply new column names
    if len(processed_df.columns) == len(new_column_names):
        processed_df.columns = new_column_names
        print(f"  Renamed columns to: {', '.join(new_column_names)}")
    else:
        print(f"  Warning: Number of kept columns ({len(processed_df.columns)}) doesn't match expected column names ({len(new_column_names)})")
        # Assign basic names if counts don't match
        processed_df.columns = [f'column_{i+1}' for i in range(len(processed_df.columns))]

    return processed_df

def process_columns(df, source_type, source_tags):
    """Process dataframe columns based on the identified source type"""
    print(f"Processing columns for {source_type}...")

    # Special preprocessing for various source types
    if source_type == 'Linkedin_India':
        return preprocess_linkedin_india(df)
    elif source_type == 'Linkedin_US':
        return preprocess_linkedin_us(df)
    elif source_type == 'Calendly_India':
        return preprocess_calendly_india(df)
    elif source_type == 'Calendly_US':
        return preprocess_calendly_us(df)
    elif source_type == 'Indeed_US':
        return preprocess_indeed_us(df)

    # Create a new dataframe with standardized columns
    processed_df = pd.DataFrame()

    # Convert all dataframe columns to lowercase for matching
    df.columns = [col.lower() for col in df.columns]

    # Map columns based on source tags
    for category, keywords in source_tags.items():
        matched = False
        for keyword in keywords:
            matching_cols = [col for col in df.columns if keyword in col]
            if matching_cols:
                # Use the first matching column
                processed_df[category] = df[matching_cols[0]]
                print(f"  Mapped '{matching_cols[0]}' to '{category}'")
                matched = True
                break

        if not matched:
            # Add empty column if no match found
            processed_df[category] = None
            print(f"  No match found for '{category}'")

    return processed_df

def preprocess_linkedin_us(df):
    """Special preprocessing for LinkedIn US format, similar to LinkedIn India"""
    print("Preprocessing LinkedIn US data...")

    # Create a copy to avoid modifying the original
    processed_df = df.copy()

    # First pass: remove notes and feedback columns (case-insensitive)
    columns_to_remove = ['notes', 'feedback', 'Notes', 'Feedback']
    existing_cols_to_remove = [col for col in processed_df.columns if col in columns_to_remove]
    if existing_cols_to_remove:
        processed_df = processed_df.drop(columns=existing_cols_to_remove)
        print(f"  Removed columns: {existing_cols_to_remove}")

    # Drop headline and current company columns as requested
    headline_cols = [col for col in processed_df.columns if 'headline' in str(col).lower()]
    company_cols = [col for col in processed_df.columns if 'current company' in str(col).lower() or 'company' in str(col).lower()]

    all_cols_to_drop = headline_cols + company_cols
    if all_cols_to_drop:
        processed_df = processed_df.drop(columns=all_cols_to_drop)
        print(f"  Dropped headline and company columns: {all_cols_to_drop}")

    # Define the expected column structure for LinkedIn US
    expected_cols = [
        'first_name',
        'last_name',
        'location',
        'current_title',
        'email',
        'phone',
        'profile_url',
        'active_project'
    ]

    # Check if columns are unnamed (like "Unnamed: 0", "Unnamed: 1", etc.)
    unnamed_pattern = any(col.startswith('Unnamed:') for col in processed_df.columns)

    if unnamed_pattern:
        print("  Detected unnamed columns - applying standard LinkedIn US column names")
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

        # Rename the columns
        if rename_map:
            processed_df = processed_df.rename(columns=rename_map)
            print(f"  Renamed columns according to expected LinkedIn US structure")
            print(f"  Column mapping: {rename_map}")

    # If there are more columns than expected, keep them with original names
    if len(processed_df.columns) > len(expected_cols):
        extra_cols = processed_df.columns[len(expected_cols):]
        print(f"  Keeping additional columns: {list(extra_cols)}")

    # Split active_project column to extract content in parentheses
    if 'active_project' in processed_df.columns:
        # Create a function to extract text inside parentheses
        def extract_parentheses(text):
            if pd.isna(text):
                return ('', '')

            import re
            # Find content inside parentheses
            match = re.search(r'\((.*?)\)', str(text))
            if match:
                # Get text before parentheses
                main_text = str(text).split('(')[0].strip()
                # Get text inside parentheses
                parentheses_text = match.group(1).strip()
                return (main_text, parentheses_text)
            else:
                return (str(text), '')

        # Apply the function to split the active_project column
        split_results = processed_df['active_project'].apply(extract_parentheses)

        # Create new columns with the split results
        processed_df['active_project'] = split_results.str[0]  # Text before parentheses
        processed_df['status'] = split_results.str[1]  # Text inside parentheses is renamed to status directly
        print("  Extracted content in parentheses to 'status' column (instead of project_details)")

    # Merge first and last name into a single 'name' column
    if 'first_name' in processed_df.columns and 'last_name' in processed_df.columns:
        processed_df['name'] = processed_df['first_name'].fillna('') + ' ' + processed_df['last_name'].fillna('')
        processed_df['name'] = processed_df['name'].str.strip()
        print("  Merged 'first_name' and 'last_name' into 'name' column")

        # Rearrange columns to make 'name' the first column
        cols = list(processed_df.columns)
        cols.remove('name')

        # Check if columns exist before removing them
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
    cols_to_remove = []
    for col in processed_df.columns:
        if col in ['Notes', 'Feedback', 'notes', 'feedback']:
            cols_to_remove.append(col)

    if cols_to_remove:
        processed_df = processed_df.drop(columns=cols_to_remove)
        print(f"  Final cleanup: Removed {cols_to_remove} columns")

    return processed_df

def list_excel_files(directory='.'):
    """List all Excel files in the given directory"""
    excel_files = []
    for file in os.listdir(directory):
        if file.lower().endswith(('.xlsx', '.xls', '.csv')):
            excel_files.append(file)
    return excel_files

def run_merge_all_files():
    """Import and run the merge_all_files.py script"""
    print("\n===== RUNNING MERGE ALL FILES PROCESS =====")

    # Import functions from merge_all_files.py
    try:
        from merge_all_files import list_files_by_type, preprocess_file, merge_files_by_type, merge_dataframes, main as merge_main

        # Execute the main function
        merge_main()
        print("Merge all files process completed successfully.")
    except Exception as e:
        print(f"Error running merge_all_files.py: {e}")

def run_copy_title_experience():
    """Run the copy_title_experience.py script"""
    print("\n===== RUNNING COPY TITLE TO EXPERIENCE PROCESS =====")

    try:
        # Check if merged_us_data.csv exists
        if not os.path.exists('merged_us_data.csv'):
            print("Error: merged_us_data.csv does not exist. Skipping this step.")
            return

        print("Reading merged_us_data.csv...")
        df = pd.read_csv('merged_us_data.csv')

        # Check if both columns exist
        if 'experience' in df.columns and 'current_title' in df.columns:
            # Get stats before changes
            print(f"Before update: Experience column has {df['experience'].notna().sum()} non-null values")
            print(f"Before update: Current title column has {df['current_title'].notna().sum()} non-null values")

            # Map current_title to experience where experience is null
            experience_null_mask = df['experience'].isna() & df['current_title'].notna()
            if experience_null_mask.any():
                df.loc[experience_null_mask, 'experience'] = df.loc[experience_null_mask, 'current_title']
                print(f"Filled {experience_null_mask.sum()} null experience values with current_title values")

            # Map experience to current_title where current_title is null
            title_null_mask = df['current_title'].isna() & df['experience'].notna()
            if title_null_mask.any():
                df.loc[title_null_mask, 'current_title'] = df.loc[title_null_mask, 'experience']
                print(f"Filled {title_null_mask.sum()} null current_title values with experience values")

            # Handle cases where both columns have values but they are different
            both_values_mask = df['experience'].notna() & df['current_title'].notna() & (df['experience'] != df['current_title'])
            if both_values_mask.any():
                print(f"Found {both_values_mask.sum()} rows where both columns have different values")
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

    except Exception as e:
        print(f"Error running copy_title_experience.py: {e}")

def run_clean_us_data_columns():
    """Run the clean_us_data_columns.py script"""
    print("\n===== RUNNING CLEAN US DATA COLUMNS PROCESS =====")

    try:
        # Check if merged_us_data.csv exists
        if not os.path.exists('merged_us_data.csv'):
            print("Error: merged_us_data.csv does not exist. Skipping this step.")
            return

        print("Reading merged_us_data.csv...")
        df = pd.readcsv('merged_us_data.csv')

        # First, check which columns exist
        print("Current columns in the dataset:", df.columns.tolist())

        # Count of records with data in each column of interest
        if 'active_project' in df.columns:
            print(f"Records with active_project data: {df['active_project'].notna().sum()}")
        if 'current_title' in df.columns:
            print(f"Records with current_title data: {df['current_title'].notna().sum()}")
        if 'project_details' in df.columns:
            print(f"Records with project_details data: {df['project_details'].notna().sum()}")
        if 'experience' in df.columns:
            print(f"Records with experience data: {df['experience'].notna().sum()}")

        # Since we've already synchronized experience and current_title 
        # We can handle active_project and project_details

        # First check if active_project contains data that's not in position column
        if 'active_project' in df.columns and 'position' in df.columns:
            unique_data = df['active_project'].notna() & df['position'].isna()
            if unique_data.any():
                print(f"Moving unique active_project data to position for {unique_data.sum()} rows")
                df.loc[unique_data, 'position'] = df.loc[unique_data, 'active_project']

        # Now we can remove the extra columns if they're redundant
        columns_to_drop = []
        if 'active_project' in df.columns:
            columns_to_drop.append('active_project')

        if 'project_details' in df.columns:
            # You can choose to keep or drop project_details
            # If it contains valuable info, you might want to keep it
            if df['project_details'].notna().sum() == 0:
                columns_to_drop.append('project_details')
            else:
                print(f"Note: project_details column contains {df['project_details'].notna().sum()} records with data")
                print("Sample values:", df['project_details'].dropna().head(3).tolist())

        if columns_to_drop:
            print(f"Dropping columns: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)

            # Save the modified dataframe back to CSV
            df.to_csv('merged_us_data.csv', index=False)
            print("Successfully cleaned the merged_us_data.csv file")
            print("Final columns:", df.columns.tolist())
        else:
            print("No columns were identified for removal")

    except Exception as e:
        print(f"Error running clean_us_data_columns.py: {e}")

def run_clean_india_data():
    """Run the clean_india_data.py script"""
    print("\n===== RUNNING CLEAN INDIA DATA PROCESS =====")

    try:
        # Check if merged_india_data.csv exists
        if not os.path.exists('merged_india_data.csv'):
            print("Error: merged_india_data.csv does not exist. Skipping this step.")
            return

        print("Reading merged_india_data.csv...")
        df = pd.read_csv('merged_india_data.csv')

        # Check if active_project column exists
        if 'active_project' in df.columns:
            print(f"Found 'active_project' column with {df['active_project'].notna().sum()} non-null values")

            # Check if we need to map data before removing
            if 'position' in df.columns:
                # Copy active_project to position where position is null but active_project is not
                mask = df['position'].isna() & df['active_project'].notna()
                if mask.any():
                    print(f"Copying {mask.sum()} values from active_project to position before dropping the column")
                    df.loc[mask, 'position'] = df.loc[mask, 'active_project']

            # Remove the active_project column
            columns_to_drop = ['active_project']
            print(f"Dropping extra column: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)

            # Save the modified dataframe back to CSV
            df.to_csv('merged_india_data.csv', index=False)
            print("Successfully removed active_project column from merged_india_data.csv")
            print(f"Final columns: {df.columns.tolist()}")
        else:
            print("active_project column not found in the merged_india_data.csv file")

        # Print a summary of the current data
        print("\nData summary after cleaning:")
        print(f"Total records: {len(df)}")
        if 'source' in df.columns:
            source_counts = df['source'].value_counts()
            for source, count in source_counts.items():
                print(f"- {source}: {count} records")

    except Exception as e:
        print(f"Error running clean_india_data.py: {e}")

def run_remove_duplicates():
    """Run the remove_duplicates.py script to remove duplicate records"""
    print("\n===== RUNNING REMOVE DUPLICATES PROCESS =====")

    try:
        # Process US data
        if os.path.exists('merged_us_data.csv'):
            print("Removing duplicates from US data...")
            df_us = pd.read_csv('merged_us_data.csv')
            original_us_count = len(df_us)

            # Check if required columns exist
            has_name = 'name' in df_us.columns
            has_email = 'email' in df_us.columns
            has_phone = 'phone' in df_us.columns

            if not has_name:
                print("Error: 'name' column not found in US dataset")
            else:
                # Create combination identifiers for deduplication
                identifiers = []

                if has_name and has_email:
                    # Create identifier based on name+email
                    df_us['name_email_id'] = df_us['name'].str.lower().fillna('') + '_' + df_us['email'].str.lower().fillna('')
                    identifiers.append('name_email_id')
                    print("Created name+email identifier for deduplication")

                if has_name and has_phone:
                    # Create identifier based on name+phone
                    # Normalize phone numbers by removing non-numeric characters
                    df_us['phone_norm'] = df_us['phone'].astype(str).str.replace(r'\D', '', regex=True)
                    df_us['name_phone_id'] = df_us['name'].str.lower().fillna('') + '_' + df_us['phone_norm'].fillna('')
                    identifiers.append('name_phone_id')
                    print("Created name+phone identifier for deduplication")

                if identifiers:
                    # Remove duplicates based on all identifiers
                    df_us = df_us.drop_duplicates(subset=identifiers, keep='first')

                    # Clean up temporary columns
                    if 'name_email_id' in df_us.columns:
                        df_us = df_us.drop(columns=['name_email_id'])
                    if 'name_phone_id' in df_us.columns:
                        df_us = df_us.drop(columns=['name_phone_id'])
                    if 'phone_norm' in df_us.columns:
                        df_us = df_us.drop(columns=['phone_norm'])

                    # Save the deduplicated dataset
                    df_us.to_csv('merged_us_data.csv', index=False)

                    # Print summary
                    removed = original_us_count - len(df_us)
                    print(f"Removed {removed} duplicate records from US data")
                    print(f"Final US record count: {len(df_us)}")
                else:
                    print("Not enough columns for deduplication in US data")
        else:
            print("US data file not found, skipping deduplication")

        # Process India data
        if os.path.exists('merged_india_data.csv'):
            print("\nRemoving duplicates from India data...")
            df_india = pd.read_csv('merged_india_data.csv')
            original_india_count = len(df_india)

            # Check if required columns exist
            has_name = 'name' in df_india.columns
            has_email = 'email' in df_india.columns
            has_phone = 'phone' in df_india.columns

            if not has_name:
                print("Error: 'name' column not found in India dataset")
            else:
                # Create combination identifiers for deduplication
                identifiers = []

                if has_name and has_email:
                    # Create identifier based on name+email
                    df_india['name_email_id'] = df_india['name'].str.lower().fillna('') + '_' + df_india['email'].str.lower().fillna('')
                    identifiers.append('name_email_id')
                    print("Created name+email identifier for deduplication")

                if has_name and has_phone:
                    # Create identifier based on name+phone
                    # Normalize phone numbers by removing non-numeric characters
                    df_india['phone_norm'] = df_india['phone'].astype(str).str.replace(r'\D', '', regex=True)
                    df_india['name_phone_id'] = df_india['name'].str.lower().fillna('') + '_' + df_india['phone_norm'].fillna('')
                    identifiers.append('name_phone_id')
                    print("Created name+phone identifier for deduplication")

                if identifiers:
                    # Remove duplicates based on all identifiers
                    df_india = df_india.drop_duplicates(subset=identifiers, keep='first')

                    # Clean up temporary columns
                    if 'name_email_id' in df_india.columns:
                        df_india = df_india.drop(columns=['name_email_id'])
                    if 'name_phone_id' in df_india.columns:
                        df_india = df_india.drop(columns=['name_phone_id'])
                    if 'phone_norm' in df_india.columns:
                        df_india = df_india.drop(columns=['phone_norm'])

                    # Save the deduplicated dataset
                    df_india.to_csv('merged_india_data.csv', index=False)

                    # Print summary
                    removed = original_india_count - len(df_india)
                    print(f"Removed {removed} duplicate records from India data")
                    print(f"Final India record count: {len(df_india)}")
                else:
                    print("Not enough columns for deduplication in India data")
        else:
            print("India data file not found, skipping deduplication")

    except Exception as e:
        print(f"Error removing duplicates: {e}")

def run_remove_duplicate_columns():
    """Run the remove_duplicate_columns.py script"""
    print("\n===== RUNNING REMOVE DUPLICATE COLUMNS PROCESS =====")

    try:
        # Check if merged_us_data.csv exists
        if not os.path.exists('merged_us_data.csv'):
            print("Error: merged_us_data.csv does not exist. Skipping this step.")
            return

        print("Reading merged_us_data.csv...")
        df = pd.read_csv('merged_us_data.csv')

        # First, check which columns exist
        print("Current columns in the dataset:", df.columns.tolist())

        # Check if both columns exist and count identical values
        if 'experience' in df.columns and 'current_title' in df.columns:
            total_rows = len(df)
            matching_rows = (df['experience'] == df['current_title']).sum()
            null_experience = df['experience'].isna().sum()
            null_current_title = df['current_title'].isna().sum()

            print(f"Total rows: {total_rows}")
            print(f"Rows with matching values: {matching_rows} ({matching_rows/total_rows*100:.1f}%)")
            print(f"Rows with null experience: {null_experience}")
            print(f"Rows with null current_title: {null_current_title}")

            # Adding current_title to columns to drop
            columns_to_drop = ['current_title']
            print(f"Adding current_title to columns to drop")

            # If project_details exists, it may also be redundant with status
            if 'project_details' in df.columns:
                print(f"Adding project_details to columns to drop")
                columns_to_drop.append('project_details')

            print(f"Dropping extra columns: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)

            # Save the modified dataframe back to CSV
            df.to_csv('merged_us_data.csv', index=False)
            print("Successfully cleaned the merged_us_data.csv file")
            print(f"Final columns: {df.columns.tolist()}")
        else:
            missing_cols = []
            if 'experience' not in df.columns:
                missing_cols.append('experience')
            if 'current_title' not in df.columns:
                missing_cols.append('current_title')
            print(f"Error: Missing columns in the dataset: {', '.join(missing_cols)}")

        # Print a summary of the data
        print("\nData summary after cleaning:")
        print(f"Total records: {len(df)}")
        if 'source' in df.columns:
            source_counts = df['source'].value_counts()
            for source, count in source_counts.items():
                print(f"- {source}: {count} records")

    except Exception as e:
        print(f"Error running remove_duplicate_columns.py: {e}")

def run_process_calendly_data():
    """Run the process_calendly_data.py script to merge Calendly data"""
    print("\n===== RUNNING CALENDLY DATA PROCESSING =====")

    try:
        # Import the process_calendly_data module
        from process_calendly_data import process_all_calendly_data

        # Execute the main function
        process_all_calendly_data()

        print("Calendly data processing completed.")
    except Exception as e:
        print(f"Error processing Calendly data: {e}")

def run_remove_excel_duplicates():
    """Run the remove_excel_duplicates.py script to deduplicate original Excel files"""
    print("\n===== RUNNING EXCEL FILE DEDUPLICATION =====")

    try:
        # Import the remove_excel_duplicates module
        from remove_excel_duplicates import main as dedupe_main

        # Execute the main function
        dedupe_main()

        print("Excel file deduplication completed.")
    except Exception as e:
        print(f"Error deduplicating Excel files: {e}")

def run_add_stage_column():
    """Run the add_stage_column.py script to add stage column to merged data"""
    print("\n===== ADDING STAGE COLUMN TO MERGED DATA =====")

    try:
        # Import the add_stage_column module
        from add_stage_column import main as stage_main

        # Execute the main function
        stage_main()

        print("Stage column addition completed.")
    except Exception as e:
        print(f"Error adding stage column: {e}")



def store_database(timestamp=None):
    """Store a copy of the database in a database directory with timestamp"""
    if timestamp is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Create database directory
    db_dir = os.path.join('database', timestamp)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    # Files to store
    db_files = [
        'merged_us_data.csv',
        'merged_india_data.csv',
        'merged_all_data.csv'
    ]

    stored_files = []

    for file in db_files:
        if os.path.exists(file):
            # Copy the file to the database directory
            dest_path = os.path.join(db_dir, file)
            shutil.copy2(file, dest_path)
            stored_files.append(dest_path)

    return db_dir, stored_files

def main():
    """Main function to run all data processing scripts in sequence"""
    # Step 1: Deduplicate the original Excel files
    run_remove_excel_duplicates()

    # Step 2: Run merge_all_files.py - this will create the merged CSV files
    run_merge_all_files()

    # Step 3: Run the copy_title_experience.py script
    run_copy_title_experience()

    # Step 4: Run the clean_us_data_columns.py script
    run_clean_us_data_columns()

    # Step 5: Run the clean_india_data.py script
    run_clean_india_data()

    # Step 6: Run the remove_duplicate_columns.py script
    run_remove_duplicate_columns()

    # Step 7: Run the remove_duplicates function to handle duplicate records
    run_remove_duplicates()

    # Step 8: Process Calendly data and merge with appropriate datasets
    run_process_calendly_data()

    # Step 9: Add stage column to all merged datasets
    run_add_stage_column()

    # Step 10: Store the database
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    store_database(timestamp)

    # Step 12: Archive the upload files
    from archive_uploads import archive_uploads
    archive_uploads()

if __name__ == "__main__":
    main()