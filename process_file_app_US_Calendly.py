import pandas as pd
import os
from datetime import datetime

# Define the column mapping:
# Keys are the original column names in the Calendly files;
# Values are the new names to keep.


def preprocess_calendly_US(filepath, country_name="US"):
    print('preprocess_calendly_US Started')
    """
    Loads a Calendly CSV file, retains and renames only the desired columns,
    adds a source column, converts and sorts the date column, prints a sample,
    and returns the processed DataFrame.
    """
    column_mapping = {
        "Invitee Name": "name",
        "Invitee Email": "email",
        "Text Reminder Number": "phone",
        "Start Date & Time": "Start Date & Time",  # kept as is
        "Response 1": "Profile",
        "Response 2": "salary",
        "Response 3": "US Person",
        "Response 4": "location",
        "Response 5": "position",
        "Marked as No-Show": "Marked as No-Show",
        "Meeting Notes": "Meeting Notes"
    }
    try:
        # Load the file
        df = pd.read_csv(filepath)

        # Keep only the columns present in our mapping
        cols_to_keep = [col for col in column_mapping.keys() if col in df.columns]
        df = df[cols_to_keep].copy()

        # Rename columns according to the mapping
        df.rename(columns=column_mapping, inplace=True)

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
            print("\nCalendly_US data sample (first 3 rows):")
            print(df[['Nname', 'email', 'phone']].head(3))

        print('preprocess_calendly_US Completed')

        return df

    except Exception as e:
        print(f"Error processing Calendly {country_name} file ({filepath}): {e}")
        return None

def process_calendly_US(calendly_files_US):
    print('process_calendly_US Started')
    calendly_dfs = []
    for file in calendly_files_US:
            df = preprocess_calendly_US(file)
            # df.head()
            # Optionally add a source column for later identification
            df['source'] = 'Calendly_US'
            calendly_dfs.append(df)

    # Merge all processed DataFrames into one
    merged_df_calendly_US = pd.concat(calendly_dfs, ignore_index=True)
    print('process_calendly_US Completed')
    return merged_df_calendly_US
