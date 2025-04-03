import numpy as np
import networkx as nx
import pandas as pd

# --- Mapping dictionaries for standardizing columns ---
indeed_us_mapping = {
    'name': ['name', 'candidate name', 'full name'],
    'email': ['email', 'email address', 'email id'],
    'phone': ['phone', 'mobile', 'contact', 'phone number'],
    'status': ['status', 'application status'],
    'location': ['location', 'candidate location', 'current location', 'city'],
    'profile': ['profile_url', 'linkedin url', 'Profile'],
    'job title': ['job title', 'position', 'role', 'designation'],
    'Date':['Start Date & Time','Date']
}

linkedin_us_mapping = {
    'name': ['name', 'candidate name', 'full name'],
    'location': ['location', 'current location', 'city'],
    'current_company': ['current company', 'company', 'employer'],
    'email': ['email', 'email address', 'email id'],
    'status': ['status', 'application status'],
    'phone': ['phone', 'mobile', 'contact', 'phone number'],
    'profile': ['profile_url', 'linkedin url', 'Profile'],
    'job title': ['current title', 'job title', 'position title', 'position', 'role', 'designation'],
    'Date':['Start Date & Time','Date']
}

calendly_us_mapping = {
    'name': ['name', 'candidate name', 'full name'],
    'email': ['email', 'email address', 'email id'],
    'phone': ['phone', 'mobile', 'contact', 'phone number'],
    'status': ['status', 'meeting status', 'appointment status'],
    'location': ['location', 'candidate location', 'current location', 'city'],
    'profile': ['profile_url', 'linkedin url', 'Profile'],
    'job title': ['job title', 'position', 'role', 'designation'],
    'Date':['Start Date & Time','Date']
}

def standardize_columns(df, mapping_dict):
    """
    Renames DataFrame columns based on the provided mapping dictionary.

    Parameters:
        df (pd.DataFrame): The DataFrame to process.
        mapping_dict (dict): Dictionary where keys are the standardized column names
                             and values are lists of possible synonyms.

    Returns:
        pd.DataFrame: DataFrame with standardized column names.
    """
    rename_map = {}
    for std_col, synonyms in mapping_dict.items():
        for col in df.columns:
            col_norm = col.strip().lower()
            synonyms_norm = [s.strip().lower() for s in synonyms]
            if col_norm in synonyms_norm:
                rename_map[col] = std_col
                break
    return df.rename(columns=rename_map)

def final_merge_US(US_dfs_indeed, US_dfs_linkedin, US_dfs_calendly):
    US_dfs_indeed = standardize_columns(US_dfs_indeed, indeed_us_mapping)
    US_dfs_linkedin = standardize_columns(US_dfs_linkedin, linkedin_us_mapping)
    US_dfs_calendly = standardize_columns(US_dfs_calendly, calendly_us_mapping)
    merged_df = pd.concat([US_dfs_indeed, US_dfs_linkedin, US_dfs_calendly], ignore_index=True, sort=False)
    print('merged_df = pd.concat([US_dfs_indeed, US_dfs_linkedin, US_dfs_calendly], ignore_index=True, sort=False) completed')
    return merged_df