
import streamlit as st
import pandas as pd
import os
from datetime import datetime

def candidate_update_section(region):
    """Handle candidate data update functionality"""
    st.title("Candidate Data Update Tool")
    st.subheader("Update Individual Candidate Records")

    # Show data overview based on region
    st.write(f"### {region} Data Overview")

    # Load data based on region
    df = None
    if region == "India":
        modified_dir = os.path.join('database', 'Modified Data')
        if os.path.exists(modified_dir):
            modified_files = sorted([f for f in os.listdir(modified_dir) if f.endswith('.csv')], reverse=True)
            if modified_files:
                latest_file = os.path.join(modified_dir, modified_files[0])
                print(f"Using latest modified data from: {modified_files[0]}")
                df = pd.read_csv(latest_file)
                print(f"Loaded latest data from: {modified_files[0]}")
                st.dataframe(df, use_container_width=True)
                st.text(f"Total records: {len(df)}")
            else:
                st.warning("No data files found for India region")
    else:  # US region
        modified_dir = os.path.join('database', 'Modified Data US')
        if os.path.exists(modified_dir):
            modified_files = sorted([f for f in os.listdir(modified_dir) if f.endswith('.csv')], reverse=True)
            if modified_files:
                latest_file = os.path.join(modified_dir, modified_files[0])
                print(f"Using latest modified data from: {modified_files[0]}")
                df = pd.read_csv(latest_file)
                print(f"Loaded latest data from: {modified_files[0]}")
                st.dataframe(df, use_container_width=True)
                st.text(f"Total records: {len(df)}")
            else:
                st.warning("No data files found for US region")

    if df is None:
        st.error(f"No data found for {region}")
        return

    # Add search functionality
    st.sidebar.subheader("Search for Candidate")
    search_method = st.sidebar.radio("Search by", ["Name", "Email", "Phone"])
    search_term = st.sidebar.text_input(f"Enter {search_method.lower()} to search")

    if search_term:
        if search_method == "Name":
            filtered_df = df[df['name'].str.contains(search_term, case=False, na=False)]
        elif search_method == "Email":
            filtered_df = df[df['email'].str.contains(search_term, case=False, na=False)]
        else:  # Phone
            filtered_df = df[df['phone'].astype(str).str.contains(search_term, na=False)]

        if len(filtered_df) == 0:
            st.warning(f"No candidates found with {search_method.lower()} containing '{search_term}'")
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

                        with col1:
                            name = st.text_input("Name", selected_candidate.get('name', ''))
                            email = st.text_input("Email", selected_candidate.get('email', ''))
                            phone = st.text_input("Phone", selected_candidate.get('phone', ''))
                            location = st.text_input("Location", selected_candidate.get('location', ''))

                        with col2:
                            experience = st.text_input("Experience", selected_candidate.get('experience', ''))
                            position = st.text_input("Position", selected_candidate.get('position', ''))
                            status = st.text_input("Status", selected_candidate.get('status', ''))

                        with col3:
                            stage_options = ["Call Stage", "Scheduled", "Rejected"]
                            current_stage = selected_candidate.get('stage', 'Call Stage')
                            stage = st.selectbox(
                                "Stage",
                                options=stage_options,
                                index=stage_options.index(current_stage) if current_stage in stage_options else 0)
                            date = st.text_input("Date (Calendly)", selected_candidate.get('date', ''))

                        with col4:
                            source = st.text_input("Source", selected_candidate.get('source', ''))
                            notes = st.text_area("Notes", selected_candidate.get('notes', ''))

                    else:  # India
                        col1, col2 = st.columns(2)
                        col3, col4 = st.columns(2)

                        with col1:
                            name = st.text_input("Name", selected_candidate.get('name', ''))
                            email = st.text_input("Email", selected_candidate.get('email', ''))
                            phone = st.text_input("Phone", selected_candidate.get('phone', ''))
                            location = st.text_input("Location", selected_candidate.get('location', ''))

                        with col2:
                            total_experience = st.text_input("Total Experience", selected_candidate.get('total_experience', ''))
                            annual_salary = st.text_input("Annual Salary", selected_candidate.get('annual_salary', ''))
                            notice_period = st.text_input("Notice Period", selected_candidate.get('notice_period', ''))

                        with col3:
                            position = st.text_input("Position", selected_candidate.get('position', ''))
                            current_company = st.text_input("Current Company", selected_candidate.get('current_company', ''))
                            stage_options = ["Call Stage", "Scheduled", "Rejected"]
                            current_stage = selected_candidate.get('stage', 'Call Stage')
                            stage = st.selectbox(
                                "Stage",
                                options=stage_options,
                                index=stage_options.index(current_stage) if current_stage in stage_options else 0)

                        with col4:
                            source = st.text_input("Source", selected_candidate.get('source', ''))
                            date = st.text_input("Date (Calendly)", selected_candidate.get('date', ''))
                            notes = st.text_area("Notes", selected_candidate.get('notes', ''))

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("Save Changes"):
                            try:
                                # Update the dataframe with new values
                                df.loc[selected_index, 'name'] = name
                                df.loc[selected_index, 'email'] = email
                                df.loc[selected_index, 'phone'] = phone
                                df.loc[selected_index, 'location'] = location
                                df.loc[selected_index, 'position'] = position
                                df.loc[selected_index, 'stage'] = stage
                                df.loc[selected_index, 'source'] = source
                                df.loc[selected_index, 'date'] = date
                                df.loc[selected_index, 'notes'] = notes

                                if region == "US":
                                    df.loc[selected_index, 'experience'] = experience
                                    df.loc[selected_index, 'status'] = status
                                else:  # India
                                    df.loc[selected_index, 'total_experience'] = total_experience
                                    df.loc[selected_index, 'annual_salary'] = annual_salary
                                    df.loc[selected_index, 'notice_period'] = notice_period
                                    df.loc[selected_index, 'current_company'] = current_company

                                # Save to Modified Data directory
                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                modified_dir = os.path.join('database', f'Modified Data{" US" if region == "US" else ""}')
                                os.makedirs(modified_dir, exist_ok=True)
                                modified_file = os.path.join(modified_dir, f'modified_{region.lower()}_data_{timestamp}.csv')
                                df.to_csv(modified_file, index=False)
                                st.success("Changes saved successfully!")
                                
                            except Exception as e:
                                st.error(f"Error updating record: {str(e)}")

                    with col2:
                        if st.button("Delete Record"):
                            try:
                                # Create backup before deletion
                                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                                backup_dir = os.path.join('database', 'backups')
                                if not os.path.exists(backup_dir):
                                    os.makedirs(backup_dir)
                                backup_file = os.path.join(backup_dir, f'backup_before_deletion_{timestamp}.csv')
                                df.to_csv(backup_file, index=False)

                                # Remove the record
                                df = df.drop(selected_index).reset_index(drop=True)

                                # Save to Modified Data directory
                                modified_dir = os.path.join('database', f'Modified Data{" US" if region == "US" else ""}')
                                os.makedirs(modified_dir, exist_ok=True)
                                modified_file = os.path.join(modified_dir, f'modified_{region.lower()}_data_{timestamp}.csv')
                                df.to_csv(modified_file, index=False)

                                st.success(f"Successfully deleted record for {name}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error deleting record: {str(e)}")
