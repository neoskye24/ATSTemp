import pandas as pd
import os
from datetime import datetime

# Define the column mapping:
# Keys are the original column names in the Calendly files;
# Values are the new names to keep.


def preprocess_calendly(filepath, country_name="India"):
    print('preprocess_calendly Started')
    """
    Loads a Calendly CSV file, retains and renames only the desired columns,
    adds a source column, converts and sorts the date column, prints a sample,
    and returns the processed DataFrame.
    """
    column_mapping = {
        "Invitee Name": "name",
        "Invitee Email": "email",
        "Start Date & Time": "Start Date & Time",  # kept as is
        "Response 1": "Profile",
        "Response 2": "salary",
        "Response 3": "notice_period",
        "Response 4": 'location',
        "Response 5": "position",
        "Response 6": "Source",
        "Response 7": "phone",
        "Marked as No-Show": "Marked as No-Show",
        "Meeting Notes": "meeting_notes"
    }
    try:
        # Load the file based on extension
        if filepath.endswith('.xlsx'):
            df = pd.read_excel(filepath)
        else:
            df = pd.read_csv(filepath)

        # Convert relevant columns to string type
        string_columns = ['name', 'email', 'phone', 'location', 'position', 'source']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # Rename columns according to the mapping
        df.rename(columns=column_mapping, inplace=True)

        # Remove existing source column if present
        if 'source' in df.columns:
            df = df.drop(columns=['source'])
        # Add source column
        df['source'] = f'Calendly_{country_name}'

        # Convert date strings in 'Start Date & Time' to datetime and sort by date
        if 'Start Date & Time' in df.columns:
            try:
                # Define potential date formats; added a format for "YYYY-MM-DD hh:mm am/pm"
                date_formats = ['%Y-%m-%d %I:%M %p', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%B %d, %Y']
                for date_format in date_formats:
                    try:
                        df['Start Date & Time'] = pd.to_datetime(df['Start Date & Time'], format=date_format)
                        print(f"  Successfully parsed dates using format: {date_format}")
                        break
                    except Exception as e:
                        continue
                df = df.sort_values('Start Date & Time', ascending=False)
                print("  Sorted records by date (most recent first)")
            except Exception as e:
                print(f"  Warning: Could not parse date column - {e}")

        # Print sample data (first 3 rows) for verification
        if all(col in df.columns for col in ['Nname', 'email', 'phone']):
            print("\nCalendly data sample (first 3 rows):")
            print(df[['Nname', 'email', 'phone']].head(3))

        print('preprocess_calendly for India Completed')

        return df

    except Exception as e:
        print(f"Error processing Calendly {country_name} file ({filepath}): {e}")
        return None

def process_calendly_india(calendly_files):
    print('process_calendly_india Started')
    calendly_dfs = []
    for file in calendly_files:
            df = preprocess_calendly(file)
            # df.head()
            # Optionally add a source column for later identification
            df['source'] = 'Calendly_India'
            calendly_dfs.append(df)

    # Merge all processed DataFrames into one
    merged_df_calendly = pd.concat(calendly_dfs, ignore_index=True)
    print('process_calendly_india Completed')
    return merged_df_calendly

# Optional: Save the merged result to a new Excel file
# merged_df.to_excel("merged_calendly_india_data.xlsx", index=False)

# --- Define mapping dictionaries ---

#-----------------------------------------------------------------------------------------------------------------#

def standardize_columns(df, mapping):
    """
    Standardizes the DataFrame's columns based on the provided mapping.
    """
    print('standardize_columns for Calendly Started')
    rename_dict = {}
    for col in df.columns:
        col_norm = col.strip().lower()
        for std_col, synonyms in mapping.items():
            synonyms_norm = [s.strip().lower() for s in synonyms]
            if col_norm in synonyms_norm:
                rename_dict[col] = std_col
                break
    print('standardize_columns for Calendly Completed')
    return df.rename(columns=rename_dict)



calendly_mapping = {
    'name': ['name', 'full name'],
    'email': ['email', 'email address'],
    'phone': ['phone', 'mobile', 'contact'],
    'date': ['date', 'appointment date'],
    'profile_url': ['profile', 'profile url'],
    'salary': ['salary'],  # Calendly data already has 'salary'
    'notice_period': ['notice period/ availability to join', 'notice_period','Notice period/ Availability to join'],
    'position': ['position', 'job title'],
    'no_show': ['no-show', 'no_show'],
    'source': ['source','Source'],
    'location': ['location', 'city', 'current location'],
    'meeting_notes':['Meeting Notes','Notes','meeting_notes'],
    'status':['status','Status'],
    'Date':['Start Date & Time','date']}

csv_mapping = {
    'name': ['name', 'full name'],
    'email': ['email', 'email address'],
    'phone': ['phone', 'mobile', 'contact'],
    'position': ['position', 'job title', 'role'],
    'notice_period': ['notice_period','Notice period/ Availability to join'],
    # For CSV, the column might be named "annual_salary". We'll standardize it to "salary"
    'salary': ['annual_salary', 'salary', 'ctc'],
    'profile_url': ['profile_url', 'profile link'],
    'status': ['status','Status'],
    'total_experience': ['total_experience', 'experience', 'exp'],
    'location': ['location', 'city', 'current location'],
    'current_title': ['current_title', 'job title', 'position title'],
    'current_company': ['current_company', 'company', 'employer'],
    'active_project': ['active_project', 'current project', 'project'],
    'source': ['source'],
    'Date':['Start Date & Time','date'],
    'meeting_notes':['Meeting Notes','Notes','meeting_notes']
}

def process_L_N_C(india_dfs_calendly, india_dfs_L_N):
    print('process_L_N_C Started')
    # --- Load the DataFrames and add file_source ---
    # Load Calendly (Excel) data and add file_source column
    # df_calendly = pd.read_excel("merged_calendly_india_data.xlsx")
    # df_calendly['file_source'] = "Calendly_India"

    # Load CSV (Naukri/LinkedIn) data and add file_source column
    # df_csv = pd.read_csv("merged_naukri_linkedin_data_new.csv")
    # df_csv['file_source'] = "Naukri_LinkedIn"

    # --- Standardize the column names ---
    df_calendly = standardize_columns(india_dfs_calendly, calendly_mapping)
    df_csv = standardize_columns(india_dfs_L_N, csv_mapping)

    # --- Remove duplicate columns if any ---
    df_calendly = df_calendly.loc[:, ~df_calendly.columns.duplicated()]
    df_csv = df_csv.loc[:, ~df_csv.columns.duplicated()]

    # # --- For CSV: Replace active_project with position (if position is missing) ---
    # # We'll fill missing position values with active_project values and then drop active_project.
    # df_csv['position'] = df_csv['position'].combine_first(df_csv.get('active_project'))
    # if 'active_project' in df_csv.columns:
    #     df_csv.drop(columns=['active_project'], inplace=True)

    # --- Align the DataFrames ---
    # Create the union of all columns from both DataFrames
    all_cols = set(df_calendly.columns).union(set(df_csv.columns))

    # Add missing columns to each DataFrame with NaN values
    for col in all_cols:
        if col not in df_calendly.columns:
            df_calendly[col] = pd.NA
        if col not in df_csv.columns:
            df_csv[col] = pd.NA

    # --- Combine the DataFrames ---
    merged_df = pd.concat([df_csv,df_calendly], ignore_index=True)

    # --- Create additional columns based on equivalences ---
    # annual_salary is same as salary: create a duplicate column
    merged_df["annual_salary"] = merged_df["salary"]

    # notice_period is same as declaration: duplicate that column
    merged_df["notice_period"] = merged_df["notice_period"]


    # no-show: duplicate from no_show if exists; if not, leave as is
    if "no_show" in merged_df.columns:
        merged_df["no-show"] = merged_df["no_show"]
    else:
        merged_df["no-show"] = pd.NA

    # consent column: add as empty (or you can set a default value)
    merged_df["consent"] = pd.NA
        # Only combine meeting notes when we have matching records from both sources
    def merge_meeting_notes(row):
        if pd.notna(row['meeting_notes']) and 'LinkedIn' in str(row['source']):
            # Keep original LinkedIn notes without modification
            return row['meeting_notes']
        return row['meeting_notes']

    merged_df['meeting_notes'] = merged_df.apply(merge_meeting_notes, axis=1)

    # --- Define the final column order ---
    final_order = [
         "name", "email", "phone", "position", "status","location", "total_experience",
        "annual_salary", "notice_period", "source",
        "current_company", "no-show","meeting_notes", "salary", "Date",
    ]

    # Ensure all columns exist in the merged dataframe
    for col in final_order:
        if col not in merged_df.columns:
            merged_df[col] = pd.NA

    merged_df_L_N_C = merged_df[final_order]
    print('process_L_N_C Completed')
    return merged_df_L_N_C


