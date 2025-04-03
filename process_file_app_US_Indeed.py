import pandas as pd
import numpy as np

def read_file(file_path):
  """
  Reads a file from the given file path.
  Supports both Excel (.xlsx) and CSV (.csv) formats.
  The skiprows parameter is used for files that require skipping header rows (e.g., LinkedIn data).
  """
  if file_path.lower().endswith('.xlsx'):
      df = pd.read_excel(file_path, engine='openpyxl',header=1)
  elif file_path.lower().endswith('.csv'):
      df = pd.read_csv(file_path)
  else:
      raise ValueError(f"Unsupported file format for file: {file_path}")
  return df

def preprocess_indeed_US(df):
  """
  Processes Indeed US DataFrames provided in a dictionary mapping file labels to DataFrames.
  Keeps only the following columns:
    - name
    - email
    - phone
    - status
    - candidate location (renamed to 'location')
    - job title
  It then merges all DataFrames into one, saves it as 'merged_indeed_data.csv',
  and returns the merged DataFrame.

  Parameters:
      dataframes (dict): Dictionary with keys as labels and values as pandas DataFrames.

  Returns:
      pd.DataFrame: The merged DataFrame.
  """
  desired_columns = ['name', 'email', 'phone', 'status', 'location', 'job title']

  # Standardize column names: trim spaces and convert to lowercase.
  df.columns = [col.strip().lower() for col in df.columns]

  # Rename 'candidate location' to 'location' if present.
  if 'candidate location' in df.columns:
      df = df.rename(columns={'candidate location': 'location'})

  # Build a new DataFrame with only the desired columns.
  filtered_data = {}
  for col in desired_columns:
      filtered_data[col] = df[col] if col in df.columns else pd.NA
  df_filtered = pd.DataFrame(filtered_data)

  # Add a 'source' column to indicate the origin of the data.
  df_filtered['source'] = "Indeed_US"
  return df_filtered


# Process LinkedIn files
def process_Indeed_US(indeed_files_US):
    print('process_Indeed_US Started')
    indeed_dfs = []
    for file in indeed_files_US:
        df = read_file(file)
    print('read file_US is completed')
      
    df = preprocess_indeed_US(df)
    print('df = preprocess_indeed_US(df) is completed')
    # Optionally add a source column for later identification
    df['source'] = 'Indeed_US'
    indeed_dfs.append(df)
    # Merge all processed DataFrames into one
    merged_df_indeed_US = pd.concat(indeed_dfs, ignore_index=True)
    print('Indeed merged Completed. ', merged_df_indeed_US.dtypes)
    print('process_Indeed_india Completed')
    return merged_df_indeed_US