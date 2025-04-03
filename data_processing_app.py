import streamlit as st
import pandas as pd
import os
import io
import sys
from datetime import datetime

from process_file_app_india_LinkedIn import *
from process_file_app_india_Naukri import *
from process_file_app_india_Calendy import *
from process_file_app_india_merge_naukri_linkedin import *
from merged_duplicates_processing import *
from process_file_app_US_Calendly import *
from process_file_app_US_LinkedIn import *
from process_file_app_US_Indeed import *
from process_file_app_US_merge_calendly_linkedin_indeed import *
from update_candidate_records import *

final_dataframe_india = pd.DataFrame()
final_dataframe_us = pd.DataFrame()


def save_uploaded_file(uploaded_file, source_type):
    """Save uploaded file to the uploads directory"""
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    file_path = os.path.join('uploads', f"{source_type}_{uploaded_file.name}")
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path


def load_or_create_data():
    """Load existing data or create new DataFrame"""
    current_file = 'merged_all_data.csv'

    if os.path.exists(current_file):
        return pd.read_csv(current_file)
    return pd.DataFrame(columns=[
        'Stage', 'name', 'email', 'phone', 'location', 'experience',
        'position', 'status', 'profile', 'salary', 'declaration',
        'source', 'file_source',  'date'
    ])


def save_data(df):
    """Save DataFrame to CSV"""
    df.to_csv('merged_all_data.csv', index=False)


def main():
    st.set_page_config(page_title="Data Processing Pipeline", layout="wide")

    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        operation = st.radio("Select Operation", [
            "Data Processing", "Mass/Individual Records Update",
            "Candidate Data Update Tool", "Database File Management"
        ])
        region = st.radio("Select Region", ["US", "India"])

    if operation == "Database File Management":
        st.header("Database File Management")

        # Get all directories in database
        database_dir = 'database'
        if os.path.exists(database_dir):
            directories = [d for d in os.listdir(database_dir) if os.path.isdir(os.path.join(database_dir, d))]

            # Create tabs for each directory
            tabs = st.tabs(directories)

            for tab, directory in zip(tabs, directories):
                with tab:
                    dir_path = os.path.join(database_dir, directory)
                    files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

                    if files:
                        st.write(f"Files in {directory}:")
                        # Create checkboxes for each file
                        selected_files = []
                        for file in files:
                            if st.checkbox(file, key=f"{directory}_{file}"):
                                selected_files.append(file)

                        if selected_files:
                            col1, col2 = st.columns(2)
                            with col1:
                                if st.button(f"Delete Selected Files from {directory}", key=f"delete_{directory}"):
                                    try:
                                        for file in selected_files:
                                            file_path = os.path.join(dir_path, file)
                                            os.remove(file_path)
                                        st.success(f"Successfully deleted {len(selected_files)} files from {directory}")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error deleting files: {str(e)}")
                            with col2:
                                if st.button(f"Download Selected Files from {directory}", key=f"download_{directory}"):
                                    try:
                                        for file in selected_files:
                                            file_path = os.path.join(dir_path, file)
                                            with open(file_path, 'rb') as f:
                                                st.download_button(
                                                    label=f"Download {file}",
                                                    data=f,
                                                    file_name=file,
                                                    mime='text/csv',
                                                    key=f"download_button_{directory}_{file}"
                                                )
                                    except Exception as e:
                                        st.error(f"Error downloading files: {str(e)}")

                    else:
                        st.info(f"No files found in {directory}")
        else:
            st.warning("Database directory not found")

    elif operation == "Data Processing":
        st.header(f"{region} Data Processing Pipeline")
        st.text("Please include the Naukri, Linkedin, Calendly and US/India in file name e.g. Linkedin_India.")

        if region == "US":
            files = st.file_uploader(
                "Upload US Data Files (Indeed/LinkedIn/Calendly)",
                type=['csv', 'xlsx'],
                accept_multiple_files=True,
                key='us_data')
        else:  # India
            files = st.file_uploader(
                "Upload India Data Files (Naukri/LinkedIn/Calendly)",
                type=['csv', 'xlsx'],
                accept_multiple_files=True,
                key='india_data')

        if st.button("Process Files", key="process_files"):
            with st.spinner("Processing files..."):
                try:
                    database_dir = 'database'
                    if not os.path.exists(database_dir):
                        os.makedirs(database_dir)

                    # Save and process files
                    us_dfs = []
                    india_dfs = []
                    india_dfs_L_N = []
                    naukri_files = []
                    linkedin_files = []
                    calendly_files = []
                    calendly_files_US = []
                    linkedin_files_US = []
                    indeed_files_US = []
                    india_dfs_naukri = pd.DataFrame()
                    india_dfs_linkedin = pd.DataFrame()
                    india_dfs_calendly = pd.DataFrame()
                    US_dfs_indeed = pd.DataFrame()
                    US_dfs_linkedin = pd.DataFrame()
                    US_dfs_calendly = pd.DataFrame()

                    for file in files:
                        file_lower = file.name.lower()
                        if region == "US":
                            # Only process if file name contains "US" or "us"
                            if 'us' in file_lower:
                                file_path = None
                                if 'indeed' in file_lower:
                                    file_path = save_uploaded_file(
                                        file, "Indeed_US")
                                    indeed_files_US.append(file_path)
                                    print('indeed data US is saved')

                                elif 'linkedin' in file_lower:
                                    file_path = save_uploaded_file(
                                        file, "LinkedIn_US")
                                    linkedin_files_US.append(file_path)
                                    print('linkedin data US is saved')

                                elif 'calendly' in file_lower:
                                    file_path = save_uploaded_file(
                                        file, "Calendly_US")
                                    calendly_files_US.append(file_path)
                                    print('calendly data US is saved')
                                print('region wise files distributed for US')
                            else:
                                print(f'Skipping {file.name} - no US identifier in filename')
                        else:  # India
                            # Only process if file name contains "India" or "india"
                            if 'india' in file_lower:
                                file_path = None
                                if 'naukri' in file_lower:
                                    file_path = save_uploaded_file(
                                        file, "Naukri_India")
                                    naukri_files.append(file_path)

                                elif 'linkedin' in file_lower:
                                    file_path = save_uploaded_file(
                                        file, "LinkedIn_India")
                                    linkedin_files.append(file_path)

                                elif 'calendly' in file_lower:
                                    file_path = save_uploaded_file(
                                        file, "Calendly_India")
                                    calendly_files.append(file_path)
                                print('region wise files distributed for India')
                            else:
                                print(f'Skipping {file.name} - no India identifier in filename')

                    if len(naukri_files) > 0:
                        india_dfs_naukri = process_Naukri_india(naukri_files)
                    if len(linkedin_files) > 0:
                        india_dfs_linkedin = process_Linkedin_india(
                            linkedin_files)
                    if len(calendly_files) > 0:
                        india_dfs_calendly = process_calendly_india(
                            calendly_files)
                    if len(indeed_files_US) > 0:
                        US_dfs_indeed = process_Indeed_US(indeed_files_US)
                    if len(linkedin_files_US) > 0:
                        US_dfs_linkedin = process_Linkedin_US(
                            linkedin_files_US)
                    if len(calendly_files_US) > 0:
                        US_dfs_calendly = process_calendly_US(
                            calendly_files_US)
                    print('data processing completed')
                    print(
                        'len(india_dfs_naukri) > 0 or len(india_dfs_linkedin) > 0 or len(india_dfs_calendly): ',
                        len(india_dfs_naukri), len(india_dfs_linkedin),
                        len(india_dfs_calendly))
                    print(
                        'len(US_dfs_indeed) > 0 or len(US_dfs_linkedin) > 0 or len(US_dfs_calendly) > 0',
                        len(US_dfs_indeed), len(US_dfs_linkedin),
                        len(US_dfs_calendly))
                    # if len(linkedin_files) > 0 and len(naukri_files) > 0:
                    if len(india_dfs_naukri) > 0 or len(
                            india_dfs_linkedin) > 0 or len(
                                india_dfs_calendly) > 0:
                        region = "India"
                        print(region)
                        india_dfs_L_N = process_linkedin_naukri(
                            india_dfs_naukri, india_dfs_linkedin)
                        india_dfs = process_L_N_C(india_dfs_calendly,
                                                  india_dfs_L_N)
                        merge_duplicates_dfs_india = merge_duplicates(
                            india_dfs)
                        final_dataframe_india = pd.concat(
                            [merge_duplicates_dfs_india], ignore_index=True)
                        print(
                            'final_dataframe_india = pd.concat(merge_duplicates_dfs, ignore_index=True) Completed'
                        )
                        final_dataframe_india = merge_duplicates(
                            final_dataframe_india)
                        # Drop unnecessary columns
                        columns_to_drop = [
                            'profile_url', 'current_company', 'no-show',
                            'date', 'salary', 'job title', 'US Person',
                            'salary', 'active_project', 'project_details'
                        ]
                        final_dataframe_india = final_dataframe_india.drop(
                            [
                                col for col in columns_to_drop
                                if col in final_dataframe_india.columns
                            ],
                            axis=1)
                        final_dataframe_india.index += 1
                        # Insert a new column named "Stage" as the first column, with a default empty value
                        final_dataframe_india.insert(0, "Stage", pd.NA)
                        final_dataframe_india['name'] = final_dataframe_india[
                            "name"].str.title()
                        final_dataframe_india["phone"] = final_dataframe_india[
                            "phone"].astype(str)
                        final_dataframe_india[
                            "location"] = final_dataframe_india[
                                "location"].apply(lambda x: str(x)
                                                  if pd.notnull(x) else "")

                        # Save final India data with timestamp
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        final_india_dir = os.path.join(database_dir,
                                                       'Final India Data')
                        if not os.path.exists(final_india_dir):
                            os.makedirs(final_india_dir)

                        # Save the new file
                        final_file_path = os.path.join(
                            final_india_dir,
                            f'final_india_data_{timestamp}.csv')
                        final_dataframe_india.to_csv(final_file_path,
                                                     index=False)
                        print(f"Saved final India data to {final_file_path}")

                        # Keep only the 5 most recent files
                        india_files = sorted([
                            f for f in os.listdir(final_india_dir)
                            if f.endswith('.csv')
                        ],
                                             reverse=True)
                        if len(india_files) > 5:
                            for old_file in india_files[5:]:
                                os.remove(
                                    os.path.join(final_india_dir, old_file))
                            print(
                                "Cleaned up older files for India, keeping only 5 most recent files"
                            )

                        # Get the two most recent files from Final India Data directory
                        india_files = sorted([
                            f for f in os.listdir(final_india_dir)
                            if f.endswith('.csv')
                        ],
                                             reverse=True)

                        if len(india_files) >= 2:
                            # Create Merge Final India directory if it doesn't exist
                            merge_dir = os.path.join(database_dir,
                                                     'Merge Final India')
                            if not os.path.exists(merge_dir):
                                os.makedirs(merge_dir)

                            # Read and concatenate the two most recent files
                            file1 = pd.read_csv(
                                os.path.join(final_india_dir, india_files[0]))
                            file2 = pd.read_csv(
                                os.path.join(final_india_dir, india_files[1]))
                            merged_df = pd.concat([file1, file2],
                                                  ignore_index=True)
                            merged_df = merge_duplicates(merged_df)
                            # Save merged result
                            merged_file_path = os.path.join(
                                merge_dir,
                                f'merged_india_data_{timestamp}.csv')
                            merged_df.to_csv(merged_file_path, index=False)
                        else:
                            print(
                                'merged_df.to_csv(merged_file_path, index=False) - only one file is present'
                            )

                        # Keep only 3 most recent files in Merge Final India directory
                        merge_files = sorted([
                            f for f in os.listdir(merge_dir)
                            if f.endswith('.csv')
                        ],
                                             reverse=True)
                        if len(merge_files) > 3:
                            for old_file in merge_files[3:]:
                                os.remove(os.path.join(merge_dir, old_file))
                            st.info("Keeping only 3 most recent merged files")

                        # print(f"Merged latest two files and saved to {merged_file_path}")
                        status_text = st.empty()
                        st.success("Files processed successfully")
                        st.header("Processed India Data")
                        st.dataframe(final_dataframe_india,
                                     use_container_width=True)
                        st.text(f"Total records: {len(final_dataframe_india)}")

                    elif len(US_dfs_indeed) > 0 or len(
                            US_dfs_linkedin) > 0 or len(US_dfs_calendly) > 0:
                        region = "US"
                        print(region)
                        US_dfs = final_merge_US(US_dfs_indeed, US_dfs_linkedin,
                                                US_dfs_calendly)
                        merge_duplicates_dfs_US = merge_duplicates(US_dfs)
                        final_dataframe_US = pd.concat(
                            [merge_duplicates_dfs_US], ignore_index=True)
                        print(
                            'final_dataframe_US = pd.concat([merge_duplicates_dfs_US], ignore_index=True) Completed'
                        )
                        final_dataframe_US = merge_duplicates(
                            final_dataframe_US)
                        # Drop unnecessary columns
                        columns_to_drop = [
                            'current_title', 'current_company', 'profile_url',
                            'active_project', 'profile',
                            'Event Created Date & Time', 'Marked as No-Show'
                        ]
                        final_dataframe_US = final_dataframe_US.drop([
                            col for col in columns_to_drop
                            if col in final_dataframe_US.columns
                        ],
                                                                     axis=1)
                        final_dataframe_US.index += 1
                        # Insert a new column named "Stage" as the first column, with a default empty value
                        final_dataframe_US.insert(0, "Stage", pd.NA)
                        final_dataframe_US['name'] = final_dataframe_US[
                            "name"].str.title()
                        final_dataframe_US["phone"] = final_dataframe_US[
                            "phone"].astype(str)
                        final_dataframe_US["location"] = final_dataframe_US[
                            "location"].apply(lambda x: str(x)
                                              if pd.notnull(x) else "")

                        # Save final India data with timestamp
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        final_US_dir = os.path.join(database_dir,
                                                    'Final US Data')
                        if not os.path.exists(final_US_dir):
                            os.makedirs(final_US_dir)

                        # Save the new file
                        final_file_path = os.path.join(
                            final_US_dir, f'final_US_data_{timestamp}.csv')
                        final_dataframe_US.to_csv(final_file_path, index=False)
                        print(f"Saved final US data to {final_file_path}")

                        # Keep only the 5 most recent files
                        US_files = sorted([
                            f for f in os.listdir(final_US_dir)
                            if f.endswith('.csv')
                        ],
                                          reverse=True)
                        print('US_files: ', US_files, '\n len(US_files): ',
                              len(US_files))
                        if len(US_files) > 5:
                            for old_file in US_files[5:]:
                                os.remove(os.path.join(final_US_dir, old_file))
                            print(
                                "Cleaned up older files for US, keeping only 5 most recent files"
                            )

                        # Get the two most recent files from Final India Data directory
                        US_files = sorted([
                            f for f in os.listdir(final_US_dir)
                            if f.endswith('.csv')
                        ],
                                          reverse=True)
                        print('Updated US_files: ', US_files)
                        merge_dir = os.path.join(database_dir,
                                                 'Merge Final US')
                        if not os.path.exists(merge_dir):
                            os.makedirs(merge_dir)
                        if len(US_files) >= 2:
                            file1 = pd.read_csv(
                                os.path.join(final_US_dir, US_files[0]))
                            file2 = pd.read_csv(
                                os.path.join(final_US_dir, US_files[1]))
                            merged_df = pd.concat([file1, file2],
                                                  ignore_index=True)
                            merged_df = merge_duplicates(merged_df)
                            # Save merged result
                            merged_file_path = os.path.join(
                                merge_dir, f'merged_US_data_{timestamp}.csv')
                            merged_df.to_csv(merged_file_path, index=False)
                            print(
                                'merged_df.to_csv(merged_file_path, index=False) Completed'
                            )
                        else:
                            print(
                                'merged_df.to_csv(merged_file_path, index=False) - only one file is present'
                            )

                        # Keep only 3 most recent files in Merge Final India directory
                        merge_files = sorted([
                            f for f in os.listdir(merge_dir)
                            if f.endswith('.csv')
                        ],
                                             reverse=True)
                        if len(merge_files) > 3:
                            for old_file in merge_files[3:]:
                                os.remove(os.path.join(merge_dir, old_file))
                            print("Keeping only 3 most recent merged files")

                        # print(f"Merged latest two files and saved to {merged_file_path}")
                        status_text = st.empty()
                        st.success("Files processed successfully")
                        st.header("Processed US Data")
                        st.dataframe(final_dataframe_US,
                                     use_container_width=True)
                        st.text(f"Total records: {len(final_dataframe_US)}")
                    else:
                        print(
                            'region selection after the file distribution regionwise is not working'
                        )

                    #---------------------------------------------------------------------------------#
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")

    elif operation == "Mass/Individual Records Update":
        st.header(f"{region} Template Management")

        if region == "US":
            # Get the latest merged US data
            # First check Modified Data directory in database folder
            # Get all relevant directories
            modified_dir = os.path.join('database', 'Modified Data US')
            final_dir = os.path.join('database', 'Final US Data')
            merge_dir = os.path.join('database', 'Merge Final US')

            dfs_to_concat = []
            latest_timestamps = {}

            # Check Modified Data US directory
            if os.path.exists(modified_dir):
                modified_files = sorted([f for f in os.listdir(modified_dir) if f.endswith('.csv')], reverse=True)
                if modified_files:
                    latest_modified = os.path.join(modified_dir, modified_files[0])
                    try:
                        df_modified = pd.read_csv(latest_modified)
                        dfs_to_concat.append(df_modified)
                        timestamp = modified_files[0].split('_')[-1].replace('.csv', '')
                        latest_timestamps['Modified'] = timestamp
                        st.info(f"Using modified data from: {modified_files[0]}")
                    except Exception as e:
                        st.error(f"Error reading modified file: {str(e)}")

            # Check Final US Data directory
            if os.path.exists(final_dir):
                final_files = sorted([f for f in os.listdir(final_dir) if f.endswith('.csv')], reverse=True)
                if final_files:
                    latest_final = os.path.join(final_dir, final_files[0])
                    try:
                        df_final = pd.read_csv(latest_final)
                        timestamp = final_files[0].split('_')[-1].replace('.csv', '')
                        if 'Modified' not in latest_timestamps or timestamp > latest_timestamps['Modified']:
                            dfs_to_concat.append(df_final)
                            latest_timestamps['Final'] = timestamp
                            st.info(f"Using final data from: {final_files[0]}")
                    except Exception as e:
                        st.error(f"Error reading final file: {str(e)}")

            # Check Merge Final US directory
            if os.path.exists(merge_dir):
                merge_files = sorted([f for f in os.listdir(merge_dir) if f.endswith('.csv')], reverse=True)
                if merge_files:
                    latest_merge = os.path.join(merge_dir, merge_files[0])
                    try:
                        df_merge = pd.read_csv(latest_merge)
                        timestamp = merge_files[0].split('_')[-1].replace('.csv', '')
                        if all(timestamp > latest_timestamps.get(key, '') for key in latest_timestamps):
                            dfs_to_concat.append(df_merge)
                            latest_timestamps['Merge'] = timestamp
                            st.info(f"Using merged data from: {merge_files[0]}")
                    except Exception as e:
                        st.error(f"Error reading merge file: {str(e)}")

            if dfs_to_concat:
                us_df = pd.concat(dfs_to_concat, ignore_index=True)
                us_df = merge_duplicates(us_df)
                us_df = us_df.reset_index(drop=True)

                # Save the combined data to Modified Data US with new timestamp
                if not os.path.exists(modified_dir):
                    os.makedirs(modified_dir)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                new_modified_file = os.path.join(modified_dir, f'modified_us_data_{timestamp}.csv')
                us_df.to_csv(new_modified_file, index=False)
                st.success(f"Combined data saved to: {os.path.basename(new_modified_file)}")
            else:
                us_df = pd.DataFrame()
                st.warning("No data files found in any directory")


            # Display data overview

            st.subheader("Current Data Overview")
            st.dataframe(us_df, use_container_width=True)
            st.text(f"Total records: {len(us_df)}")

            # Add download button for current data
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                us_df.to_excel(writer, index=False)

            st.download_button(
                label="Download Current Data as Excel",
                data=buffer.getvalue(),
                file_name="current_us_data.xlsx",
                mime=
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Add upload functionality for the downloaded file
            st.subheader("Upload Modified Data")
            uploaded_file = st.file_uploader("Upload the modified Excel file",
                                             type=['xlsx'],
                                             key="modified_us")

            if uploaded_file is not None:
                try:
                    new_df = pd.read_excel(uploaded_file)
                    if set(new_df.columns) == set(us_df.columns):
                        us_df = new_df.copy()

                        # Create Modified Data directory if it doesn't exist
                        modified_dir = os.path.join('database',
                                                    'Modified Data US')
                        if not os.path.exists(modified_dir):
                            os.makedirs(modified_dir)

                        # Save with timestamp
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        modified_file = os.path.join(
                            modified_dir, f'modified_us_data_{timestamp}.csv')
                        us_df.to_csv(modified_file, index=False)

                        # Keep only the latest 5 modified files
                        modified_files = sorted([
                            f for f in os.listdir(modified_dir)
                            if f.endswith('.csv')
                        ],
                                                reverse=True)
                        if len(modified_files) > 5:
                            for old_file in modified_files[5:]:
                                os.remove(os.path.join(modified_dir, old_file))

                        st.success(
                            f"Successfully saved modified data to {modified_file}"
                        )

                        # Refresh the data overview
                        st.subheader("Updated Data Overview")
                        st.dataframe(us_df, use_container_width=True)
                        st.text(f"Total records: {len(us_df)}")
                    else:
                        st.error(
                            "Column mismatch. Please ensure the file structure matches the original."
                        )
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")

            # Create empty template
            template_df = pd.DataFrame(columns=us_df.columns)

            # Add download button for empty template
            st.subheader("1. Download Empty Template")
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                template_df.to_excel(writer, index=False)
            st.download_button(
                label="Download Empty Template Excel",
                data=buffer.getvalue(),
                file_name="us_candidate_template.xlsx",
                mime=
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # File upload section for template
            st.subheader("2. Upload Filled Template")
            uploaded_template = st.file_uploader("Choose a file",
                                                 type=['xlsx', 'xls'],
                                                 key="template_us")

            if uploaded_template is not None:
                try:
                    # Read uploaded file
                    new_df = pd.read_excel(uploaded_template)

                    # Validate columns
                    if not all(col in new_df.columns for col in us_df.columns):
                        st.error(
                            "Invalid template format. Please use the provided template."
                        )
                        return

                    # Process each row
                    updates = 0
                    additions = 0
                    for _, row in new_df.iterrows():
                        # Check if record exists (by email or phone)
                        existing_record = us_df[
                            (us_df['email'] == row['email']) |
                            (us_df['phone'] == row['phone'])]

                        if len(existing_record) > 0:
                            # Update existing record
                            idx = existing_record.index[0]
                            for col in us_df.columns:
                                if pd.notna(row[col]
                                            ):  # Only update non-null values
                                    us_df.loc[idx, col] = row[col]
                            updates += 1
                        else:
                            # Add new record
                            us_df.loc[len(us_df)] = row
                            additions += 1

                    # Create base directories
                    database_dir = 'database'
                    if not os.path.exists(database_dir):
                        os.makedirs(database_dir)

                    final_us_dir = os.path.join(database_dir, 'Final US Data')
                    if not os.path.exists(final_us_dir):
                        os.makedirs(final_us_dir)

                    modified_dir = os.path.join(database_dir,
                                                'Modified Data US')
                    if not os.path.exists(modified_dir):
                        os.makedirs(modified_dir)

                    # Save the new file with timestamp
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    final_file_path = os.path.join(
                        final_us_dir, f'final_us_data_{timestamp}.csv')
                    us_df = merge_duplicates(us_df)
                    us_df.to_csv(final_file_path, index=False)

                    # Also save to Modified Data directory
                    modified_file = os.path.join(
                        modified_dir, f'modified_us_data_{timestamp}.csv')
                    us_df.to_csv(modified_file, index=False)

                    # Keep only the 5 most recent files in each directory
                    for directory in [final_us_dir, modified_dir]:
                        files = sorted([
                            f for f in os.listdir(directory)
                            if f.endswith('.csv')
                        ],
                                       reverse=True)
                        if len(files) > 5:
                            for old_file in files[5:]:
                                os.remove(os.path.join(directory, old_file))

                    st.success(
                        f"Successfully saved data to {final_file_path} and {modified_file}"
                    )

                    # Keep only the latest 5 modified files
                    modified_files = sorted([
                        f
                        for f in os.listdir(modified_dir) if f.endswith('.csv')
                    ],
                                            reverse=True)
                    if len(modified_files) > 5:
                        for old_file in modified_files[5:]:
                            os.remove(os.path.join(modified_dir, old_file))

                    st.success(
                        f"Successfully saved merged data to {modified_file}")

                    # Create backup
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_dir = os.path.join('database', timestamp)
                    os.makedirs(backup_dir, exist_ok=True)
                    backup_path = os.path.join(backup_dir,
                                               'merged_us_data.csv')
                    us_df.to_csv(backup_path, index=False)

                    st.success(f"Successfully processed the file:\n"
                               f"- Updated {updates} existing records\n"
                               f"- Added {additions} new records\n"
                               f"Backup created in {backup_dir}")

                    # Display updated data
                    st.subheader("All Records")
                    st.dataframe(us_df, use_container_width=True)

                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")

                    # Process each row
                    updates = 0
                    additions = 0
                    for _, row in new_df.iterrows():
                        # Check if record exists (by email or phone)
                        existing_record = us_df[
                            (us_df['email'] == row['email']) |
                            (us_df['phone'] == row['phone'])]

                        if len(existing_record) > 0:
                            # Update existing record
                            idx = existing_record.index[0]
                            for col in us_df.columns:
                                if pd.notna(row[col]
                                            ):  # Only update non-null values
                                    us_df.loc[idx, col] = row[col]
                            updates += 1
                        else:
                            # Add new record
                            us_df.loc[len(us_df)] = row
                            additions += 1

                    # Save updated data
                    us_df.to_csv('merged_us_data.csv', index=False)

                    # Create backup
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_dir = os.path.join('database', timestamp)
                    os.makedirs(backup_dir, exist_ok=True)
                    backup_path = os.path.join(backup_dir,
                                               'merged_us_data.csv')
                    us_df.to_csv(backup_path, index=False)

                    st.success(f"Successfully processed the file:\n"
                               f"- Updated {updates} existing records\n"
                               f"- Added {additions} new records\n"
                               f"Backup created in {backup_dir}")
        else:  # India region
            # Get the latest merged India data
            # First check Modified Data directory in database folder
            # Get all relevant directories
            modified_dir = os.path.join('database', 'Modified Data India')
            final_dir = os.path.join('database', 'Final India Data')
            merge_dir = os.path.join('database', 'Merge Final India')

            dfs_to_concat = []
            latest_timestamps = {}

            # Check Modified Data directory
            if os.path.exists(modified_dir):
                modified_files = sorted([f for f in os.listdir(modified_dir) if f.endswith('.csv')], reverse=True)
                if modified_files:
                    latest_modified = os.path.join(modified_dir, modified_files[0])
                    try:
                        df_modified = pd.read_csv(latest_modified)
                        dfs_to_concat.append(df_modified)
                        timestamp = modified_files[0].split('_')[-1].replace('.csv', '')
                        latest_timestamps['Modified'] = timestamp
                        st.info(f"Using modified data from: {modified_files[0]}")
                    except Exception as e:
                        st.error(f"Error reading modified file: {str(e)}")

            # Check Final India Data directory
            if os.path.exists(final_dir):
                final_files = sorted([f for f in os.listdir(final_dir) if f.endswith('.csv')], reverse=True)
                if final_files:
                    latest_final = os.path.join(final_dir, final_files[0])
                    try:
                        df_final = pd.read_csv(latest_final)
                        timestamp = final_files[0].split('_')[-1].replace('.csv', '')
                        if 'Modified' not in latest_timestamps or timestamp > latest_timestamps['Modified']:
                            dfs_to_concat.append(df_final)
                            latest_timestamps['Final'] = timestamp
                            st.info(f"Using final data from: {final_files[0]}")
                    except Exception as e:
                        st.error(f"Error reading final file: {str(e)}")

            # Check Merge Final India directory
            if os.path.exists(merge_dir):
                merge_files = sorted([f for f in os.listdir(merge_dir) if f.endswith('.csv')], reverse=True)
                if merge_files:
                    latest_merge = os.path.join(merge_dir, merge_files[0])
                    try:
                        df_merge = pd.read_csv(latest_merge)
                        timestamp = merge_files[0].split('_')[-1].replace('.csv', '')
                        if all(timestamp > latest_timestamps.get(key, '') for key in latest_timestamps):
                            dfs_to_concat.append(df_merge)
                            latest_timestamps['Merge'] = timestamp
                            st.info(f"Using merged data from: {merge_files[0]}")
                    except Exception as e:
                        st.error(f"Error reading merge file: {str(e)}")

            if dfs_to_concat:
                india_df = pd.concat(dfs_to_concat, ignore_index=True)
                india_df = merge_duplicates(india_df)
                india_df = india_df.reset_index(drop=True)

                # Save the combined data to Modified Data with new timestamp
                if not os.path.exists(modified_dir):
                    os.makedirs(modified_dir)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                new_modified_file = os.path.join(modified_dir, f'modified_india_data_{timestamp}.csv')
                india_df.to_csv(new_modified_file, index=False)
                st.success(f"Combined data saved to: {os.path.basename(new_modified_file)}")
            else:
                india_df = pd.DataFrame()
                st.warning("No data files found in any directory")

                st.info(f"Using modified data from: {modified_files[0]}")
            # Display data overview
            st.subheader("Current Data Overview")
            st.dataframe(india_df, use_container_width=True)
            st.text(f"Total records: {len(india_df)}")

            # Add download button for current data
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                india_df.to_excel(writer, index=False)

            st.download_button(
                label="Download Current Data as Excel",
                data=buffer.getvalue(),
                file_name="current_india_data.xlsx",
                mime=
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Add upload functionality for the downloaded file
            st.subheader("Upload Modified Data")
            uploaded_file = st.file_uploader("Upload the modified Excel file",
                                             type=['xlsx'],
                                             key="modified_india")

            if uploaded_file is not None:
                try:
                    new_df = pd.read_excel(uploaded_file)
                    if set(new_df.columns) == set(india_df.columns):
                        india_df = new_df

                        # Create Modified Data directory if it doesn't exist
                        modified_dir = os.path.join('database',
                                                    'Modified Data')
                        if not os.path.exists(modified_dir):
                            os.makedirs(modified_dir)

                        # Save with timestamp
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        modified_file = os.path.join(
                            modified_dir,
                            f'modified_india_data_{timestamp}.csv')
                        india_df.to_csv(modified_file, index=False)

                        # Keep only the latest 5 modified files
                        modified_files = sorted([
                            f for f in os.listdir(modified_dir)
                            if f.endswith('.csv')
                        ],
                                                reverse=True)
                        if len(modified_files) > 5:
                            for old_file in modified_files[5:]:
                                os.remove(os.path.join(modified_dir, old_file))

                        st.success(
                            f"Successfully saved modified data to {modified_file}"
                        )

                        # Refresh the data overview
                        st.subheader("Updated Data Overview")
                        st.dataframe(india_df, use_container_width=True)
                        st.text(f"Total records: {len(india_df)}")
                    else:
                        st.error(
                            "Column mismatch. Please ensure the file structure matches the original."
                        )
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")

            # Create empty template
            template_df = pd.DataFrame(columns=india_df.columns)

            # Add download button for empty template
            st.subheader("1. Download Empty Template")
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                template_df.to_excel(writer, index=False)
            st.download_button(
                label="Download Empty Template Excel",
                data=buffer.getvalue(),
                file_name="india_candidate_template.xlsx",
                mime=
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # File upload section for template
            st.subheader("2. Upload Filled Template")
            uploaded_template = st.file_uploader("Choose a file",
                                                 type=['xlsx', 'xls'],
                                                 key="template_india")

            if uploaded_template is not None:
                try:
                    # Read uploaded file
                    new_df = pd.read_excel(uploaded_template)

                    # Validate columns
                    if not all(col in new_df.columns
                               for col in india_df.columns):
                        st.error(
                            "Invalid template format. Please use the provided template."
                        )
                        return

                    # Process each row
                    updates = 0
                    additions = 0
                    for _, row in new_df.iterrows():
                        # Check if record exists (by email or phone)
                        existing_record = india_df[
                            (india_df['email'] == row['email']) |
                            (india_df['phone'] == row['phone'])]

                        if len(existing_record) > 0:
                            # Update existing record
                            idx = existing_record.index[0]
                            for col in india_df.columns:
                                if pd.notna(row[col]):  # Only update non-null values
                                    india_df.loc[idx, col] = row[col]
                            updates += 1
                        else:
                            # Add new record
                            india_df.loc[len(india_df)] = row
                            additions += 1

                    # Create base directories
                    database_dir = 'database'
                    if not os.path.exists(database_dir):
                        os.makedirs(database_dir)

                    final_india_dir = os.path.join(database_dir,
                                                   'Final India Data')
                    if not os.path.exists(final_india_dir):
                        os.makedirs(final_india_dir)

                    modified_dir = os.path.join(database_dir, 'Modified Data')
                    if not os.path.exists(modified_dir):
                        os.makedirs(modified_dir)

                    # Save the new file with timestamp
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    final_file_path = os.path.join(
                        final_india_dir, f'final_india_data_{timestamp}.csv')
                    india_df = merge_duplicates(india_df)
                    india_df.to_csv(final_file_path, index=False)

                    # Also save to Modified Data directory
                    modified_file = os.path.join(
                        modified_dir, f'modified_india_data_{timestamp}.csv')
                    india_df.to_csv(modified_file, index=False)

                    # Keep only the 5 most recent files in each directory
                    for directory in [final_india_dir, modified_dir]:
                        files = sorted([
                            f for f in os.listdir(directory)
                            if f.endswith('.csv')
                        ],
                                       reverse=True)
                        if len(files) > 5:
                            for old_file in files[5:]:
                                os.remove(os.path.join(directory, old_file))

                    st.success(
                        f"Successfully saved data to {final_file_path} and {modified_file}"
                    )

                    # Keep only the latest 5 modified files
                    modified_files = sorted([
                        f
                        for f in os.listdir(modified_dir) if f.endswith('.csv')
                    ],
                                            reverse=True)
                    if len(modified_files) > 5:
                        for old_file in modified_files[5:]:
                            os.remove(os.path.join(modified_dir, old_file))

                    st.success(
                        f"Successfully saved merged data to {modified_file}")

                    # Create backup
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_dir = os.path.join('database', timestamp)
                    os.makedirs(backup_dir, exist_ok=True)
                    backup_path = os.path.join(backup_dir,
                                               'merged_india_data.csv')
                    india_df.to_csv(backup_path, index=False)

                    st.success(f"Successfully processed the file:\n"
                               f"- Updated {updates} existing records\n"
                               f"- Added {additions} new records\n"
                               f"Backup created in {backup_dir}")

                    # Display statistics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Records", len(india_df))
                    with col2:
                        st.metric("Unique Positions",
                                  len(india_df['position'].unique()))
                    with col3:
                        st.metric("Sources", len(india_df['source'].unique()))

                    # Display updated data
                    st.subheader("All Records")
                    st.dataframe(india_df, use_container_width=True)

                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
    else:
        st.title("Candidate Data Update Tool")
        st.subheader("Update Individual Candidate Records")

        # Show data overview based on region
        st.write(f"### {region} Data Overview")

        # Load data based on region
        df = None
        if region == "India":
            database_dir = 'database'
            modified_dir = os.path.join(database_dir, 'Modified Data')
            merge_dir = os.path.join(database_dir, 'Merge Final India')

            # Try Modified Data first
            if os.path.exists(modified_dir):
                modified_files = sorted([
                    f for f in os.listdir(modified_dir) if f.endswith('.csv')
                ],
                                        reverse=True)
                if modified_files:
                    latest_file = os.path.join(modified_dir, modified_files[0])
                    df = pd.read_csv(latest_file)
                    st.info(
                        f"Loaded latest data from Modified Data: {modified_files[0]}"
                    )
                    st.subheader("Current Data Overview")
                    st.dataframe(df, use_container_width=True)
                    st.text(f"Total records: {len(df)}")

                else:
                    # If Modified Data not available, try Merge Final India
                    if os.path.exists(merge_dir):
                        merged_files = sorted([
                            f for f in os.listdir(merge_dir)
                            if f.endswith('.csv')
                        ],
                                              reverse=True)
                        if merged_files:
                            latest_file = os.path.join(merge_dir,
                                                       merged_files[0])
                            df = pd.read_csv(latest_file)
                            st.info(
                                f"Loaded latest data from Merge Final India: {merged_files[0]}"
                            )
                            st.subheader("Current Data Overview")
                            st.dataframe(df, use_container_width=True)
                            st.text(f"Total records: {len(df)}")

                        else:
                            st.error(
                                "No data found in Modified Data or Merge Final India directories"
                            )
                            st.dataframe(pd.DataFrame(),
                                         use_container_width=True)
                            st.text("Total records: 0")

                    else:
                        st.error(
                            "No data found in Modified Data or Merge Final India directories"
                        )
                        st.dataframe(pd.DataFrame(), use_container_width=True)
                        st.text("Total records: 0")

            else:
                # If Modified Data not available, try Merge Final India
                if os.path.exists(merge_dir):
                    merged_files = sorted([
                        f for f in os.listdir(merge_dir) if f.endswith('.csv')
                    ],
                                          reverse=True)
                    if merged_files:
                        latest_file = os.path.join(merge_dir, merged_files[0])
                        df = pd.read_csv(latest_file)
                        st.info(
                            f"Loaded latest data from Merge Final India: {merged_files[0]}"
                        )
                        st.subheader("Current Data Overview")
                        st.dataframe(df, use_container_width=True)
                        st.text(f"Total records: {len(df)}")

                    else:
                        st.error(
                            "No data found in Modified Data or Merge Final India directories"
                        )
                        st.dataframe(pd.DataFrame(), use_container_width=True)
                        st.text("Total records: 0")

                else:
                    st.error(
                        "No data found in Modified Data or Merge Final India directories"
                    )
                    st.dataframe(pd.DataFrame(), use_container_width=True)
                    st.text("Total records: 0")

        else:  # US region
            database_dir = 'database'
            modified_dir = os.path.join(database_dir, 'Modified Data US')
            merge_dir = os.path.join(database_dir, 'Merge Final US')

            # Try Modified Data US first
            if os.path.exists(modified_dir):
                modified_files = sorted([
                    f for f in os.listdir(modified_dir) if f.endswith('.csv')
                ],
                                        reverse=True)
                if modified_files:
                    latest_file = os.path.join(modified_dir, modified_files[0])
                    df = pd.read_csv(latest_file)
                    st.info(
                        f"Loaded latest data from Modified Data US: {modified_files[0]}"
                    )
                    st.subheader("Current Data Overview")
                    st.dataframe(df, use_container_width=True)
                    st.text(f"Total records: {len(df)}")

                else:
                    # If Modified Data not available, try Merge Final US
                    if os.path.exists(merge_dir):
                        merged_files = sorted([
                            f for f in os.listdir(merge_dir)
                            if f.endswith('.csv')
                        ],
                                              reverse=True)
                        if merged_files:
                            latest_file = os.path.join(merge_dir,
                                                       merged_files[0])
                            df = pd.read_csv(latest_file)
                            st.info(
                                f"Loaded latest data from Merge Final US: {merged_files[0]}"
                            )
                            st.subheader("Current Data Overview")
                            st.dataframe(df, use_container_width=True)
                            st.text(f"Total records: {len(df)}")

                        else:
                            st.error(
                                "No data found in Modified Data US or Merge Final US directories"
                            )
                            st.dataframe(pd.DataFrame(),
                                         use_container_width=True)
                            st.text("Total records: 0")

                    else:
                        st.error(
                            "No data found in Modified Data US or Merge Final US directories"
                        )
                        st.dataframe(pd.DataFrame(), use_container_width=True)
                        st.text("Total records: 0")

            else:
                # If Modified Data not available, try Merge Final US
                if os.path.exists(merge_dir):
                    merged_files = sorted([
                        f for f in os.listdir(merge_dir) if f.endswith('.csv')
                    ],
                                          reverse=True)
                    if merged_files:
                        latest_file = os.path.join(merge_dir, merged_files[0])
                        df = pd.read_csv(latest_file)
                        st.info(
                            f"Loaded latest data from Merge Final US: {merged_files[0]}"
                        )
                        st.subheader("Current Data Overview")
                        st.dataframe(df, use_container_width=True)
                        st.text(f"Total records: {len(df)}")

                    else:
                        st.error(
                            "No data found in Modified Data US or Merge Final US directories"
                        )
                        st.dataframe(pd.DataFrame(), use_container_width=True)
                        st.text("Total records: 0")

                else:
                    st.error(
                        "No data found in Modified Data US or Merge Final US directories"
                    )
                    st.dataframe(pd.DataFrame(), use_container_width=True)
                    st.text("Total records: 0")

        if df is None:
            st.error(f"No data found for {region}")
            return

        # Add search functionality
        st.sidebar.subheader("Search for Candidate")
        search_method = st.sidebar.radio("Search by",
                                         ["Name", "Email", "Phone"])
        search_term = st.sidebar.text_input(
            f"Enter {search_method.lower()} to search")

        if search_term:
            if search_method == "Name":
                filtered_df = df[df['name'].str.contains(search_term,
                                                         case=False,
                                                         na=False)]
            elif search_method == "Email":
                filtered_df = df[df['email'].str.contains(search_term,
                                                          case=False,
                                                          na=False)]
            else:  # Phone
                filtered_df = df[df['phone'].astype(str).str.contains(
                    search_term, na=False)]

            if len(filtered_df) == 0:
                st.warning(
                    f"No candidates found with {search_method.lower()} containing '{search_term}'"
                )
            else:
                st.success(f"Found {len(filtered_df)} candidate(s)")

                # Display candidates as selectable options
                candidate_options = []
                for i, row in filtered_df.iterrows():
                    candidate = f"{row['name']} - {row.get('email', 'No email')} - {row.get('phone', 'No phone')}"
                    candidate_options.append((candidate, i))

                if candidate_options:
                    selected_option = st.selectbox(
                        "Select a candidate to update",
                        options=[c[0] for c in candidate_options],
                        key="candidate_selector")

                    # Get the index of the selected candidate
                    selected_index = None
                    for option, idx in candidate_options:
                        if option == selected_option:
                            selected_index = idx
                            break

                    if selected_index is not None:
                        selected_candidate = df.loc[selected_index]
                        st.subheader(f"Update {selected_candidate['name']}")

                        # Create column structure based on region
                        if region == "US":
                            col1, col2 = st.columns(2)
                            col3, col4 = st.columns(2)

                            # Initialize all variables with default values
                            name = ''
                            email = ''
                            phone = ''
                            location = ''
                            salary = ''
                            us_person = ''
                            status = ''
                            source = ''
                            job_title = ''
                            position = ''
                            stage = ''
                            notes = ''
                            total_experience = ''
                            notice_period = ''
                            annual_salary = ''
                            date=''

                            if region == "US":
                                with col1:
                                    name = st.text_input(
                                        "Name",
                                        selected_candidate.get('name', ''))
                                    email = st.text_input(
                                        "Email",
                                        selected_candidate.get('email', ''))
                                    phone = st.text_input(
                                        "Phone",
                                        selected_candidate.get('phone', ''))
                                    location = st.text_input(
                                        "Location",
                                        selected_candidate.get('location', ''))

                                with col2:
                                    salary = st.text_input(
                                        "Salary",
                                        selected_candidate.get('salary', ''))
                                    us_person = st.text_input(
                                        "US Person",
                                        selected_candidate.get(
                                            'US Person', ''))
                                    status = st.text_input(
                                        "Status",
                                        selected_candidate.get('status', ''))
                                    source = st.text_input(
                                        "Source",
                                        selected_candidate.get('source', ''))

                                with col3:
                                    job_title = st.text_input(
                                        "Job Title",
                                        selected_candidate.get(
                                            'job title', ''))
                                    stage_options = [
                                        "Call Stage", "Scheduled", "Rejected", "Interview Stage", "Backed Out", "No Response", "Technical Round", "Technical Test", "Review Pending", "Pending Hire", "Sent Offer", "Hired","Joined"
                                    ]
                                    current_stage = selected_candidate.get(
                                        'Stage', 'Call Stage')
                                    stage = st.selectbox(
                                        "Stage",
                                        options=stage_options,
                                        index=stage_options.index(
                                            current_stage) if current_stage
                                        in stage_options else 0)

                                with col4:
                                    date=st.text_input("Date", selected_candidate.get('Date', ''))
                                    notes = st.text_area(
                                        "Meeting Notes",
                                        selected_candidate.get(
                                            'Meeting Notes', ''))

                        else:  # India
                            col1, col2 = st.columns(2)
                            col3, col4 = st.columns(2)

                            # Initialize all variables with default values
                            name = ''
                            email = ''
                            phone = ''
                            location = ''
                            total_experience = ''
                            annual_salary = ''
                            notice_period = ''
                            status = ''
                            position = ''
                            stage = ''
                            source = ''
                            notes = ''
                            job_title = ''
                            salary = ''
                            us_person = ''
                            date=''
                            with col1:
                                name = st.text_input(
                                    "Name", selected_candidate.get('name', ''))
                                email = st.text_input(
                                    "Email",
                                    selected_candidate.get('email', ''))
                                phone = st.text_input(
                                    "Phone",
                                    selected_candidate.get('phone', ''))
                                location = st.text_input(
                                    "Location",
                                    selected_candidate.get('location', ''))

                            with col2:
                                total_experience = st.text_input(
                                    "Total Experience",
                                    selected_candidate.get(
                                        'total_experience', ''))
                                annual_salary = st.text_input(
                                    "Annual Salary",
                                    selected_candidate.get(
                                        'annual_salary', ''))
                                notice_period = st.text_input(
                                    "Notice Period",
                                    selected_candidate.get(
                                        'notice_period', ''))
                                status = st.text_input(
                                    "Status",
                                    selected_candidate.get('status', ''))

                            with col3:
                                position = st.text_input(
                                    "Position",
                                    selected_candidate.get('position', ''))
                                stage_options = [
                                    "Call Stage", "Scheduled", "Rejected", "Interview Stage", "Backed Out", "No Response", "Technical Round", "Technical Test", "Review Pending", "Pending Hire", "Sent Offer", "Hired","Joined"
                                ]
                                current_stage = selected_candidate.get(
                                    'Stage', 'Call Stage')
                                stage = st.selectbox(
                                    "Stage",
                                    options=stage_options,
                                    index=stage_options.index(current_stage)
                                    if current_stage in stage_options else 0)

                            with col4:
                                source = st.text_input(
                                    "Source",
                                    selected_candidate.get('source', ''))
                                date=st.text_input("Date", selected_candidate.get('Date', ''))
                                notes = st.text_area(
                                    " Meeting Notes",
                                    selected_candidate.get(
                                        'meeting_notes', ''))

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            if st.button("Save Changes"):
                                try:
                                    # Create backup before changes
                                    timestamp = datetime.now().strftime(
                                        '%Y%m%d_%H%M%S')
                                    backup_dir = os.path.join(
                                        'database', 'backups')
                                    if not os.path.exists(backup_dir):
                                        os.makedirs(backup_dir)
                                    backup_file = os.path.join(
                                        backup_dir,
                                        f'backup_before_changes_{timestamp}.csv'
                                    )
                                    df.to_csv(backup_file, index=False)

                                    # Update the dataframe with new values
                                    df.loc[selected_index, 'name'] = name
                                    df.loc[selected_index, 'email'] = email
                                    df.loc[selected_index, 'phone'] = phone
                                    df.loc[selected_index,
                                           'location'] = location
                                    df.loc[selected_index, 'Date'] = date
                                    df.loc[selected_index,
                                           'position'] = position
                                    df.loc[selected_index,
                                           'job title'] = job_title
                                    df.loc[selected_index, 'salary'] = salary
                                    df.loc[selected_index,
                                           'US Person'] = us_person
                                    df.loc[selected_index, 'status'] = status
                                    df.loc[selected_index, 'Stage'] = stage
                                    df.loc[selected_index, 'source'] = source
                                    df.loc[selected_index,
                                           'Meeting Notes'] = notes
                                    df.loc[
                                        selected_index,
                                        'total_experience'] = total_experience
                                    df.loc[selected_index,
                                           'notice_period'] = notice_period
                                    df.loc[selected_index,
                                           'annual_salary'] = annual_salary

                                    if region == "US":
                                        df.loc[selected_index, 'name'] = name
                                        df.loc[selected_index, 'email'] = email
                                        df.loc[selected_index, 'phone'] = phone
                                        df.loc[selected_index,'Date']=date
                                        df.loc[selected_index,
                                               'location'] = location
                                        #df.loc[selected_index,'job title'] = job_title
                                        df.loc[selected_index,'salary'] = salary
                                        df.loc[selected_index,
                                               'US Person'] = us_person
                                        df.loc[selected_index,
                                               'status'] = status
                                        df.loc[selected_index, 'Stage'] = stage
                                        df.loc[selected_index,
                                               'source'] = source
                                        df.loc[selected_index,
                                               'Meeting Notes'] = notes

                                    else:  # India
                                        df.loc[selected_index, 'name'] = name
                                        df.loc[selected_index, 'email'] = email
                                        df.loc[selected_index, 'phone'] = phone
                                        df.loc[selected_index, 'Date'] = date
                                        df.loc[selected_index,
                                               'location'] = location
                                        df.loc[
                                            selected_index,
                                            'total_experience'] = total_experience
                                        df.loc[selected_index,
                                               'annual_salary'] = annual_salary
                                        df.loc[selected_index,
                                               'notice_period'] = notice_period
                                        df.loc[selected_index,
                                               'position'] = position
                                        df.loc[selected_index, 'Stage'] = stage
                                        df.loc[selected_index,
                                               'source'] = source
                                        df.loc[selected_index,
                                               'Meeting Notes'] = notes
                                        df.loc[selected_index,
                                               'status'] = status

                                    # Save to Modified Data directory
                                    modified_dir = os.path.join(
                                        'database',
                                        f'Modified Data{" US" if region == "US" else ""}'
                                    )
                                    os.makedirs(modified_dir, exist_ok=True)
                                    modified_file = os.path.join(
                                        modified_dir,
                                        f'modified_{region.lower()}_data_{timestamp}.csv'
                                    )
                                    df.to_csv(modified_file, index=False)

                                    st.success(
                                        f"Successfully saved changes for {name}"
                                    )
                                    st.rerun()
                                except Exception as e:
                                    st.error(
                                        f"Error updating record: {str(e)}")

                        with col2:
                            if st.button("Delete Record"):
                                try:
                                    # Create backup before deletion
                                    timestamp = datetime.now().strftime(
                                        '%Y%m%d_%H%M%S')
                                    backup_dir = os.path.join(
                                        'database', 'backups')
                                    if not os.path.exists(backup_dir):
                                        os.makedirs(backup_dir)
                                    backup_file = os.path.join(
                                        backup_dir,
                                        f'backup_before_deletion_{timestamp}.csv'
                                    )
                                    df.to_csv(backup_file, index=False)

                                    # Remove the record
                                    df = df.drop(selected_index).reset_index(
                                        drop=True)

                                    # Save to Modified Data directory
                                    modified_dir = os.path.join(
                                        'database',
                                        f'Modified Data{" US" if region == "US" else ""}'
                                    )
                                    os.makedirs(modified_dir, exist_ok=True)
                                    modified_file = os.path.join(
                                        modified_dir,
                                        f'modified_{region.lower()}_data_{timestamp}.csv'
                                    )
                                    df.to_csv(modified_file, index=False)

                                    st.success(
                                        f"Successfully deleted record for {name}"
                                    )
                                    st.rerun()
                                except Exception as e:
                                    st.error(
                                        f"Error deleting record: {str(e)}")


if __name__ == "__main__":
    main()