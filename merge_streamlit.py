import streamlit as st
import pandas as pd
import os
from datetime import datetime
from add_stage_column import add_stage_column_to_file
from clean_india_data import clean_india_data
from clean_us_data_columns import clean_us_columns
from fix_experience_mapping import fix_experience_mapping
from fix_linkedin_mapping import fix_linkedin_mapping
from map_current_title_to_experience import map_current_title_to_experience
from map_experience_title import map_experience_title
from map_project_details import map_project_details
from map_project_details_to_status import map_project_details_to_status
from process_calendly_data import process_calendly_data
from merge_calendly_data import merge_calendly_data
from remove_active_project import remove_active_project
from remove_duplicate_columns import remove_duplicate_columns
from remove_extra_us_columns import remove_extra_columns

def main():
    st.set_page_config(page_title="Data Merger", layout="wide")
    st.title("US and India Data Merger Tool")

    # File upload section
    st.header("Upload Data Files")
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("US Data")
        us_indeed = st.file_uploader("Upload Indeed US Data", type=['csv', 'xlsx'], key='us_indeed')
        us_linkedin = st.file_uploader("Upload LinkedIn US Data", type=['csv', 'xlsx'], key='us_linkedin')

        if us_indeed and us_linkedin:
            if st.button("Save US Files"):
                os.makedirs('uploads', exist_ok=True)

                # Save US files
                us_indeed_path = os.path.join('uploads', f"indeed_us.{us_indeed.name.split('.')[-1]}")
                us_linkedin_path = os.path.join('uploads', f"linkedin_us.{us_linkedin.name.split('.')[-1]}")

                try:
                    us_indeed_df = pd.read_csv(us_indeed) if us_indeed.name.endswith('.csv') else pd.read_excel(us_indeed)
                    us_linkedin_df = pd.read_csv(us_linkedin) if us_linkedin.name.endswith('.csv') else pd.read_excel(us_linkedin)

                    # Initialize current_title column if it doesn't exist
                    if 'current_title' not in us_indeed_df.columns:
                        us_indeed_df['current_title'] = us_indeed_df['experience'] if 'experience' in us_indeed_df.columns else None
                    if 'current_title' not in us_linkedin_df.columns:
                        us_linkedin_df['current_title'] = us_linkedin_df['experience'] if 'experience' in us_linkedin_df.columns else None

                    us_indeed_df.to_csv(us_indeed_path, index=False)
                    us_linkedin_df.to_csv(us_linkedin_path, index=False)

                    # Merge US data
                    from merge_files import merge_indeed_linkedin_us
                    merge_indeed_linkedin_us()

                    st.success("US files merged and saved successfully!")
                except Exception as e:
                    st.error(f"Error processing US files: {str(e)}")

    with col2:
        st.subheader("India Data")
        india_naukri = st.file_uploader("Upload Naukri India Data", type=['csv', 'xlsx'], key='india_naukri')
        india_linkedin = st.file_uploader("Upload LinkedIn India Data", type=['csv', 'xlsx'], key='india_linkedin')

        if india_naukri and india_linkedin:
            if st.button("Save India Files"):
                os.makedirs('uploads', exist_ok=True)

                # Save India files
                india_naukri_path = os.path.join('uploads', f"naukri_india.{india_naukri.name.split('.')[-1]}")
                india_linkedin_path = os.path.join('uploads', f"linkedin_india.{india_linkedin.name.split('.')[-1]}")

                try:
                    india_naukri_df = pd.read_csv(india_naukri) if india_naukri.name.endswith('.csv') else pd.read_excel(india_naukri)
                    india_linkedin_df = pd.read_csv(india_linkedin) if india_linkedin.name.endswith('.csv') else pd.read_excel(india_linkedin)

                    india_naukri_df.to_csv(india_naukri_path, index=False)
                    india_linkedin_df.to_csv(india_linkedin_path, index=False)

                    # Merge India data
                    from merge_files import merge_naukri_linkedin_india
                    merge_naukri_linkedin_india()

                    st.success("India files merged and saved successfully!")
                except Exception as e:
                    st.error(f"Error processing India files: {str(e)}")

    # Create tabs for different operations
    tab1, tab2, tab3, tab4 = st.tabs(["Process Data", "View Data", "Download Data", "Data Statistics"])

    with tab1:
        st.header("Process US and India Data")
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Process US Data"):
                with st.spinner("Processing US data..."):
                    try:
                        if os.path.exists('merged_us_data.csv'):
                            us_df = pd.read_csv('merged_us_data.csv')
                            us_df = clean_us_columns(us_df)
                            us_df = fix_experience_mapping(us_df)
                            us_df = fix_linkedin_mapping(us_df)
                            us_df = map_current_title_to_experience(us_df)
                            us_df = map_experience_title(us_df)
                            us_df = map_project_details(us_df)
                            us_df = map_project_details_to_status(us_df)
                            us_df = remove_extra_columns(us_df)
                            us_df = remove_duplicate_columns(us_df)
                            us_df.to_csv('merged_us_data.csv', index=False)
                            st.success(f"Processed {len(us_df)} US records")
                    except Exception as e:
                        st.error(f"Error processing US data: {str(e)}")

        with col2:
            if st.button("Process India Data"):
                with st.spinner("Processing India data..."):
                    try:
                        if os.path.exists('merged_india_data.csv'):
                            india_df = pd.read_csv('merged_india_data.csv')
                            india_df = clean_india_data(india_df)
                            india_df = remove_active_project(india_df)
                            india_df = remove_duplicate_columns(india_df)
                            india_df.to_csv('merged_india_data.csv', index=False)
                            st.success(f"Processed {len(india_df)} India records")
                    except Exception as e:
                        st.error(f"Error processing India data: {str(e)}")

        if st.button("Merge All Data"):
            with st.spinner("Merging all data..."):
                try:
                    merge_us_india_data()
                    process_calendly_data()
                    merge_calendly_data()

                    # Create backup
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_dir = os.path.join('database', timestamp)
                    os.makedirs(backup_dir, exist_ok=True)

                    if os.path.exists('merged_all_data.csv'):
                        df = pd.read_csv('merged_all_data.csv')
                        backup_file = os.path.join(backup_dir, 'merged_all_data.csv')
                        df.to_csv(backup_file, index=False)
                        st.success(f"Merged {len(df)} total records. Backup created at {backup_file}")
                except Exception as e:
                    st.error(f"Error during merge: {str(e)}")

    with tab2:
        st.header("View Current Data")
        if os.path.exists('merged_all_data.csv'):
            df = pd.read_csv('merged_all_data.csv')
            st.dataframe(df)
            st.text(f"Total records: {len(df)}")

    with tab3:
        st.header("Download Data")
        files = ['merged_us_data.csv', 'merged_india_data.csv', 'merged_all_data.csv']
        for file in files:
            if os.path.exists(file):
                with open(file, 'rb') as f:
                    st.download_button(
                        label=f"Download {file}",
                        data=f,
                        file_name=file,
                        mime='text/csv'
                    )

    with tab4:
        st.header("Data Statistics")
        if os.path.exists('merged_all_data.csv'):
            df = pd.read_csv('merged_all_data.csv')
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", len(df))
                if 'source' in df.columns:
                    st.write("Records by Source:")
                    st.write(df['source'].value_counts())
            with col2:
                if 'stage' in df.columns:
                    st.write("Records by Stage:")
                    st.write(df['stage'].value_counts())

if __name__ == "__main__":
    main()