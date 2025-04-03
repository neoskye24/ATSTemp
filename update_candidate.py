import streamlit as st
import pandas as pd
import os
from datetime import datetime
import shutil

from utils.deduplication import find_duplicates_by_criteria, merge_records


def find_duplicates(df, name, email=None, phone=None, position=None):
    """Find potential duplicate records based on name, email, phone or position"""
    return find_duplicates_by_criteria(df, name, email, phone, position)


def load_data(region):
    """Load candidate data for the specified region"""
    if region == "India":
        modified_dir = os.path.join('database', 'Modified Data')
        if os.path.exists(modified_dir):
            modified_files = sorted([f for f in os.listdir(modified_dir) if f.endswith('.csv')], reverse=True)
            if modified_files:
                latest_file = os.path.join(modified_dir, modified_files[0])
                df = pd.read_csv(latest_file)
                st.info(f"Using latest modified data from: {modified_files[0]}")
                return df
            else:
                st.warning("No data files found in Modified Data directory")
                return None
        else:
            st.error("Modified Data directory not found in database folder")
            return None
    else:
        # For US region, keep existing logic
        file_path = f'merged_{region.lower()}_data.csv'
        if os.path.exists(file_path):
            return pd.read_csv(file_path)

        # If not found, look in the database folder (newest first)
        database_dir = 'database'
        if os.path.exists(database_dir):
            timestamp_dirs = [
                d for d in os.listdir(database_dir)
                if os.path.isdir(os.path.join(database_dir, d))
            ]
            timestamp_dirs.sort(reverse=True)  # Sort newest first

            for timestamp in timestamp_dirs:
                db_file_path = os.path.join(database_dir, timestamp,
                                            f'merged_{region.lower()}_data.csv')
                if os.path.exists(db_file_path):
                    st.info(f"Using database file from {timestamp}")
                    return pd.read_csv(db_file_path)

    return None


def save_data(df, region):
    """Save updated data and create a backup"""
    # Create backup directory if it doesn't exist
    backup_dir = os.path.join('database', 'backups')
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    # Create timestamp for this update
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save to root directory
    file_path = f'merged_{region.lower()}_data.csv'
    df.to_csv(file_path, index=False)

    # Create a backup
    backup_file = os.path.join(
        backup_dir, f'merged_{region.lower()}_data_{timestamp}.csv')
    shutil.copy2(file_path, backup_file)

    # Also save to the database directory with latest timestamp
    database_dir = 'database'
    timestamp_dirs = [
        d for d in os.listdir(database_dir)
        if os.path.isdir(os.path.join(database_dir, d))
    ]
    timestamp_dirs.sort(reverse=True)  # Sort newest first

    if timestamp_dirs:
        latest_dir = os.path.join(database_dir, timestamp_dirs[0])
        db_file_path = os.path.join(latest_dir,
                                    f'merged_{region.lower()}_data.csv')
        df.to_csv(db_file_path, index=False)
        st.success(f"Updated database file in {latest_dir}")

    # Save to Modified Data directory in database
    database_dir = 'database'
    modified_dir = os.path.join(database_dir, 'Modified Data')
    if not os.path.exists(modified_dir):
        os.makedirs(modified_dir)

    # Create timestamp for this update
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save to Modified Data directory in database
    file_path = os.path.join(modified_dir, f'modified_india_data_{timestamp}.csv')
    df.to_csv(file_path, index=False)

    return True


def on_checkbox_change(duplicate_key, checkbox_key):
    """Helper function to update duplicate selection when checkbox changes"""
    # Update duplicate_selection based on checkbox value
    if 'duplicate_selection' not in st.session_state:
        st.session_state.duplicate_selection = {}

    st.session_state.duplicate_selection[duplicate_key] = st.session_state[
        checkbox_key]


def main():
    st.set_page_config(page_title="Candidate Data Update Tool",
                       page_icon="📋",
                       layout="wide")

    # Initialize session state for duplicate selection
    if 'duplicate_selection' not in st.session_state:
        st.session_state.duplicate_selection = {}
    if 'selected_indices' not in st.session_state:
        st.session_state.selected_indices = set()

    st.title("Candidate Data Update Tool")
    st.subheader("Scenario 4: Update Individual Candidate Records")

    # Region selection
    region = st.sidebar.radio("Select Region", ["US", "India"])

    # Load data
    df = load_data(region)

    if df is None:
        st.error(
            f"No data found for {region}. Please make sure merged_{region.lower()}_data.csv exists in either the root directory or the database folder."
        )

        # Show available database folders
        database_dir = 'database'
        if os.path.exists(database_dir):
            timestamp_dirs = [
                d for d in os.listdir(database_dir)
                if os.path.isdir(os.path.join(database_dir, d))
            ]
            timestamp_dirs.sort(reverse=True)

            if timestamp_dirs:
                st.info("Available database timestamps:")
                for ts in timestamp_dirs[:5]:  # Show latest 5
                    st.code(ts)
        return

    # Add search functionality
    st.sidebar.subheader("Search for Candidate")
    search_method = st.sidebar.radio("Search by", ["Name", "Email", "Phone"])

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
            filtered_df = df[df['phone'].astype(str).str.contains(search_term,
                                                                  na=False)]

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
                # Store the actual row index from the original dataframe
                candidate_options.append((candidate, i))

            # If no candidates found, don't show the selectbox
            if not candidate_options:
                st.warning(
                    "No candidates found matching your search criteria.")
                return

            selected_option = st.selectbox(
                "Select a candidate to update",
                options=[c[0] for c in candidate_options],
                key="candidate_selector")

            # Get the index of the selected candidate in the original dataframe
            selected_index = None
            for option, idx in candidate_options:
                if option == selected_option:
                    selected_index = idx
                    break

            if selected_index is not None:
                try:
                    # Get the candidate from the full dataframe using the stored index
                    selected_candidate = df.loc[selected_index]
                    st.subheader(f"Update {selected_candidate['name']}")

                    # Create column structure based on region
                    if region == "US":
                        col1, col2 = st.columns(2)
                        col3, col4 = st.columns(2)

                        with col1:
                            name = st.text_input(
                                "Name", selected_candidate.get('name', ''))
                            email = st.text_input(
                                "Email", selected_candidate.get('email', ''))
                            phone = st.text_input(
                                "Phone", selected_candidate.get('phone', ''))
                            location = st.text_input(
                                "Location",
                                selected_candidate.get('location', ''))

                        with col2:
                            experience = st.text_input(
                                "Experience",
                                selected_candidate.get('experience', ''))
                            position = st.text_input(
                                "Position",
                                selected_candidate.get('position', ''))
                            status = st.text_input(
                                "Status", selected_candidate.get('status', ''))

                        with col3:
                            stage_options = [
                                "Call Stage", "Scheduled", "Rejected"
                            ]
                            current_stage = selected_candidate.get(
                                'stage', 'Call Stage')
                            stage = st.selectbox(
                                "Stage",
                                options=stage_options,
                                index=stage_options.index(current_stage)
                                if current_stage in stage_options else 0)

                            date = st.text_input(
                                "Date (Calendly)",
                                selected_candidate.get('date', ''))

                        with col4:
                            source = st.text_input(
                                "Source", selected_candidate.get('source', ''))
                            notes = st.text_area(
                                "Notes", selected_candidate.get('notes', ''))

                    else:  # India
                        col1, col2 = st.columns(2)
                        col3, col4 = st.columns(2)

                        with col1:
                            name = st.text_input(
                                "Name", selected_candidate.get('name', ''))
                            email = st.text_input(
                                "Email", selected_candidate.get('email', ''))
                            phone = st.text_input(
                                "Phone", selected_candidate.get('phone', ''))
                            location = st.text_input(
                                "Location",
                                selected_candidate.get('location', ''))

                        with col2:
                            total_experience = st.text_input(
                                "Total Experience",
                                selected_candidate.get('total_experience', ''))
                            annual_salary = st.text_input(
                                "Annual Salary",
                                selected_candidate.get('annual_salary', ''))
                            notice_period = st.text_input(
                                "Notice Period",
                                selected_candidate.get('notice_period', ''))

                        with col3:
                            position = st.text_input(
                                "Position",
                                selected_candidate.get('position', ''))
                            current_company = st.text_input(
                                "Current Company",
                                selected_candidate.get('current_company', ''))

                            stage_options = [
                                "Call Stage", "Scheduled", "Rejected"
                            ]
                            current_stage = selected_candidate.get(
                                'stage', 'Call Stage')
                            stage = st.selectbox(
                                "Stage",
                                options=stage_options,
                                index=stage_options.index(current_stage)
                                if current_stage in stage_options else 0)

                        with col4:
                            source = st.text_input(
                                "Source", selected_candidate.get('source', ''))
                            date = st.text_input(
                                "Date (Calendly)",
                                selected_candidate.get('date', ''))
                            notes = st.text_area(
                                "Notes", selected_candidate.get('notes', ''))

                    # Save changes button
                    if st.button("Save Changes"):
                        # Update the dataframe with new values
                        df.loc[selected_index, 'name'] = name
                        df.loc[selected_index, 'email'] = email
                        df.loc[selected_index, 'phone'] = phone
                        df.loc[selected_index, 'location'] = location
                        df.loc[selected_index, 'position'] = position
                        df.loc[selected_index, 'stage'] = stage
                        df.loc[selected_index, 'source'] = source
                        df.loc[selected_index, 'date'] = date

                        if 'notes' not in df.columns:
                            df['notes'] = ''
                        df.loc[selected_index, 'notes'] = notes

                        if region == "US":
                            df.loc[selected_index, 'experience'] = experience
                            df.loc[selected_index, 'status'] = status
                        else:  # India
                            df.loc[selected_index,
                                   'total_experience'] = total_experience
                            df.loc[selected_index,
                                   'annual_salary'] = annual_salary
                            df.loc[selected_index,
                                   'notice_period'] = notice_period
                            df.loc[selected_index,
                                   'current_company'] = current_company

                        # Save updated data
                        if save_data(df, region):
                            st.success(f"Successfully updated data for {name}")
                            # Create a "View All Changes" expander to show the updated record
                            with st.expander("View Updated Record"):
                                st.dataframe(df.loc[[selected_index]])
                        else:
                            st.error("Failed to save data")
                except Exception as e:
                    st.error(f"An error occurred: {e}")

    # Instructions for users
    with st.sidebar.expander("Instructions"):
        st.markdown("""
        ### How to use this tool:
        1. Select the region (US or India)
        2. Search for a candidate by name, email, or phone
        3. Select the candidate from the results
        4. Update the candidate information
        5. Click 'Save Changes' to save your updates

        A backup of the data will be created each time you save changes.
        """)

    # Show data overview
    with st.expander("Data Overview"):
        if region == "India":
            database_dir = 'database'
            modified_dir = os.path.join(database_dir, 'Modified Data')
            if os.path.exists(modified_dir):
                modified_files = sorted([f for f in os.listdir(modified_dir) if f.endswith('.csv')], reverse=True)
                if modified_files:
                    latest_file = os.path.join(modified_dir, modified_files[0])
                    india_df = pd.read_csv(latest_file)
                    st.info(f"Loaded latest data from: {modified_files[0]}")
                else:
                    india_df = pd.DataFrame()
                    st.warning("No modified data files found in Modified Data directory")
            else:
                india_df = pd.DataFrame()
                st.error("Modified Data directory not found")
            st.subheader("Current Data Overview")
            st.dataframe(india_df, use_container_width=True)
            st.text(f"Total records: {len(india_df)}")

        else:
            display_df = st.session_state.get('df', df)
            st.dataframe(display_df)
            st.text(f"Total records: {len(display_df)}")



    # Add Duplicate Management section
    st.markdown("---")
    st.subheader("Manage Duplicate Records")
    st.write("Find and manage potential duplicate records in the database.")

    with st.form("search_form"):
        st.subheader("Search for Duplicates")
        duplicate_name = st.text_input("Name to check for duplicates")
        duplicate_email = st.text_input("Email (optional)")
        duplicate_phone = st.text_input("Phone (optional)")
        duplicate_position = st.text_input("Position (optional)")
        submitted = st.form_submit_button("Find Duplicates")

    if submitted and duplicate_name:
        # Find potential duplicates
        duplicates = find_duplicates(df, duplicate_name, duplicate_email,
                                     duplicate_phone, duplicate_position)

        if len(duplicates) > 0:
            st.success(f"Found {len(duplicates)} potential duplicate records")

            # Display the duplicates with selection checkboxes
            st.write("Select records to manage:")

            # Initialize selection in session state if not already present
            # Display duplicates in a form
            selection_count = 0
            with st.form("duplicate_actions_form"):
                selected_records = []

                for i, row in duplicates.iterrows():
                    col1, col2 = st.columns([0.1, 0.9])

                    with col1:
                        checkbox_key = f"select_{i}"
                        if checkbox_key not in st.session_state:
                            st.session_state[checkbox_key] = False
                        if st.checkbox("Select", key=checkbox_key):
                            selected_records.append(i)
                            selection_count += 1
                            print(selected_records)

                    with col2:
                        st.write(f"**{row['name']}**")
                        st.write(f"Email: {row.get('email', 'N/A')}")
                        st.write(f"Phone: {row.get('phone', 'N/A')}")
                        st.write(f"Position: {row.get('position', 'N/A')}")
                        with st.expander("More details"):
                            for col in row.index:
                                if col not in [
                                        'name', 'email', 'phone', 'position'
                                ] and not pd.isna(row[col]) and row[col] != '':
                                    st.write(f"{col}: {row[col]}")

                    st.markdown("---")

                col1, col2 = st.columns(2)
                with col1:
                    delete_btn = st.form_submit_button("Delete Selected")
                with col2:
                    merge_btn = st.form_submit_button("Merge Selected")

                if delete_btn:
                    if not selected_records or not isinstance(selected_records, (list, tuple)):
                        st.error("No records selected for deletion")
                        return

                    try:
                        # Verify records exist
                        valid_indices = [idx for idx in selected_records if idx in df.index]
                        if len(valid_indices) != len(selected_records):
                            st.error("Some selected records no longer exist")
                            return

                        # Store the original length
                        original_len = len(df)

                        # Create backup before modification
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        backup_dir = os.path.join('database', 'backups')
                        if not os.path.exists(backup_dir):
                            os.makedirs(backup_dir)
                        backup_file = os.path.join(backup_dir, f'backup_before_deletion_{timestamp}.csv')
                        df.to_csv(backup_file, index=False)

                        # Remove records and reset index
                        df = df.drop(valid_indices).reset_index(drop=True)

                        # Save to Modified Data directory
                        modified_dir = os.path.join('database', 'Modified Data')
                        os.makedirs(modified_dir, exist_ok=True)
                        
                        modified_file = os.path.join(modified_dir, f'modified_india_data_{timestamp}.csv')
                        df.to_csv(modified_file, index=False)

                        # Update session state and load from modified data
                        st.session_state.df = df
                        st.success(f"Data saved to: {modified_file}")
                        
                        if len(df) < original_len:
                            st.success(f"Successfully deleted {len(selected_records)} records and saved to Modified Data directory")
                            # Clear form state but keep df
                            for key in list(st.session_state.keys()):
                                if key != 'df':
                                    st.session_state.pop(key)
                            st.rerun()
                        else:
                            st.error("Failed to delete records")

                        # Verify deletion worked
                        if len(df) < original_len:
                            st.success(
                                f"Permanently deleted {len(selected_records)} records"
                            )
                            # Clear form state but keep df
                            for key in list(st.session_state.keys()):
                                if key != 'df':
                                    st.session_state.pop(key)
                            st.rerun()
                        else:
                            st.error("Failed to delete records")

                    except Exception as e:
                        st.error(f"Error during deletion: {str(e)}")

                if merge_btn:
                    if len(selected_records) == 2:
                        idx1, idx2 = selected_records
                        merged_record = merge_records(df.loc[idx1],
                                                      df.loc[idx2])
                        # Update the first record with merged values
                        for col in merged_record.index:
                            df.loc[idx1, col] = merged_record[col]

                        # Remove the second record
                        df = df.drop(idx2)

                        # Save the updated data using save_data function
                        if save_data(df, region):
                            st.success("Successfully merged and saved records")
                            with st.expander("View Merged Record"):
                                st.dataframe(df.loc[[idx1]])

                            # Clear form state but keep df
                            for key in list(st.session_state.keys()):
                                if key != 'df':
                                    st.session_state.pop(key)
                            st.rerun()
                        else:
                            st.error("Failed to save merged data")
                    else:
                        st.error("Please select exactly 2 records to merge")

            # Get selected indices
            selected_indices = [
                i for i, row in duplicates.iterrows()
                if st.session_state.get(f"checkbox_{i}", False)
            ]
            selection_count = len(selected_indices)

            if selection_count > 0:
                col1, col2 = st.columns(2)

                with col1:
                    if st.button("Delete Selected", type="primary"):
                        selected_indices = [
                            i for i, row in duplicates.iterrows()
                            if st.session_state[f"checkbox_{i}"]
                        ]

                        if selected_indices:
                            # Delete the selected records
                            df = df.drop(selected_indices)

                            # Save the updated dataframe
                            if save_data(df, region):
                                st.success(
                                    f"Successfully deleted {len(selected_indices)} records"
                                )
                                # Clear checkboxes
                                for i in duplicates.index:
                                    st.session_state[f"checkbox_{i}"] = False
                                st.rerun()
                            else:
                                st.error("Failed to save data")
                        else:
                            st.error("Please select records to delete")

                with col2:
                    if st.button("Merge Selected", type="secondary"):
                        selected_indices = [
                            i for i, row in duplicates.iterrows()
                            if st.session_state[f"checkbox_{i}"]
                        ]

                        if len(selected_indices) == 2:
                            # Get the two selected indices
                            idx1 = selected_indices[0]
                            idx2 = selected_indices[1]

                            # Merge the records
                            merged_record = merge_records(
                                df.loc[idx1], df.loc[idx2])

                            # Update the first record with merged values
                            for col in merged_record.index:
                                df.loc[idx1, col] = merged_record[col]

                            # Delete the second record
                            df = df.drop(idx2)

                            # Save the updated dataframe
                            if save_data(df, region):
                                st.success("Successfully merged records")
                                # Clear selection state without full page rerun
                                st.session_state.selected_indices = set()
                                for i in duplicates.index:
                                    st.session_state[f"checkbox_{i}"] = False
                                duplicates = find_duplicates(
                                    df, duplicate_name, duplicate_email,
                                    duplicate_phone, duplicate_position)
                            else:
                                st.error("Failed to save data")
                        else:
                            st.error(
                                "Please select exactly 2 records to merge")

                # Display count of selected records
                st.info(f"{selection_count} records selected")
        else:
            st.info("No duplicate records found")


if __name__ == "__main__":
    main()