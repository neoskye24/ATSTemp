import pandas as pd
import os
from process_file_app_india_LinkedIn import process_Linkedin_india
from process_file_app_india_Naukri import process_Naukri_india

# Mapping dictionaries for standardizing columns
Naukri_India = {
    'position':
    ['position', 'job position', 'role', 'job title', 'designation'],
    'name': ['name', 'full name', 'candidate name'],
    'email': ['email', 'email address', 'email id'],
    'phone': ['phone', 'mobile', 'contact', 'phone number'],
    'location': ['current location', 'location', 'city'],
    'total_experience': ['total experience', 'experience', 'exp'],
    'annual_salary': ['annual salary', 'salary', 'ctc', 'current ctc'],
    'status': ['status', 'Status'],
    'meeting_notes': ['Meeting Notes', 'Notes', 'meeting_notes'],
    'notice_period': [
        'notice period/ availability to join', 'notice_period',
        'Notice period/ Availability to join'
    ]
}

Linkedin_India = {
    'name': ['name', 'full name', 'candidate name'],
    'first_name': ['first name', 'firstname', 'given name'],
    'last_name': ['last name', 'lastname', 'surname', 'family name'],
    'location': ['location', 'city', 'current location'],
    'current_title': ['current title', 'job title', 'position title'],
    'current_company': ['current company', 'company', 'employer'],
    'annual_salary': ['annual_salary', 'salary', 'ctc', 'current ctc'],
    'email': ['email', 'email address', 'email id'],
    'phone': ['phone', 'mobile', 'contact', 'phone number'],
    'profile_url': ['profile url', 'linkedin url', 'profile link'],
    'status': ['status', 'Status'],
    'position': ['active project', 'current project', 'project'],
    'meeting_notes': ['Meeting Notes', 'Notes', 'meeting_notes'],
    'notice_period': [
        'notice period/availability to join', 'notice_period',
        'Notice period/ Availability to join'
    ]
}


def standardize_columns(df, mapping_dict):
    print('standardize_columns for merge_L_N Started')
    """
    Renames the columns in the DataFrame based on the mapping dictionary.

    Parameters:
      df: pandas.DataFrame - The dataframe whose columns need to be standardized.
      mapping_dict: dict - A dictionary where keys are standardized (canonical)
                           column names and values are lists of possible synonym strings.

    Returns:
      The DataFrame with standardized column names.
    """
    # Create a mapping for renaming
    rename_map = {}
    for std_col, synonyms in mapping_dict.items():
        for col in df.columns:
            # Normalize both column names and synonyms to lowercase and strip spaces
            col_norm = col.strip().lower()
            synonyms_norm = [s.strip().lower() for s in synonyms]
            if col_norm in synonyms_norm:
                rename_map[col] = std_col
                break  # Once found, no need to check more synonyms for this std_col

    # Apply the renaming
    df.rename(columns=rename_map, inplace=True)
    print('standardize_columns for merge_L_N Completed')
    return df


def process_linkedin_naukri(merged_df_naukri, merged_df_linkedin):
    print('Merging the Naukri and Linkedin Data started')
    # Load your Naukri data file (adjust file path and method as needed)
    # naukri_file = "merged_naukri_data.xlsx"
    # df_naukri = pd.read_excel(naukri_file, engine='openpyxl')
    df_naukri = standardize_columns(merged_df_naukri, Naukri_India)
    print('Standardizing the Naukri completed')

    # Load your LinkedIn India data file (adjust file path as needed)
    # linkedin_file = "merged_linkedin_india_data.xlsx"  # update with your actual file path
    # df_linkedin = pd.read_excel(linkedin_file, engine='openpyxl')
    df_linkedin = standardize_columns(merged_df_linkedin, Linkedin_India)
    print('Standardizing the Linkedin completed')

    # For LinkedIn data, you might need to combine first_name and last_name if they exist:
    if 'first_name' in df_linkedin.columns and 'last_name' in df_linkedin.columns:
        df_linkedin['name'] = df_linkedin['first_name'].fillna(
            '') + ' ' + df_linkedin['last_name'].fillna('')
        df_linkedin['name'] = df_linkedin['name'].str.strip()
        # Optionally drop the individual name columns
        df_linkedin.drop(columns=['first_name', 'last_name'], inplace=True)

    # Merge the two DataFrames
    merged_df_L_N = pd.concat([df_naukri, df_linkedin],
                              ignore_index=True,
                              sort=False)
    print('Merging the Naukri and Linkedin Data Completed')
    return merged_df_L_N
    # # Optionally, save the merged DataFrame to a file
    # output_file = "merged_naukri_linkedin_data_new.csv"
    # merged_df.to_csv(output_file, index=False)
    # print(f"Merged data saved to {output_file}")

    # # For debugging: print out the standardized column names
    # print("Standardized columns in Naukri data:", df_naukri.columns.tolist())
    # print("Standardized columns in LinkedIn data:", df_linkedin.columns.tolist())
