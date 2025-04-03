import streamlit as st
import pandas as pd
import os
from utils.file_merging import find_files_by_type
from utils.deduplication import remove_duplicates_from_dataframe
from process_calendly_data import preprocess_calendly, merge_calendly_with_main_data
from main import (
    identify_source_type, preprocess_linkedin_india, preprocess_calendly_india,
    run_merge_all_files, run_copy_title_experience, run_clean_india_data,
    run_remove_duplicates, run_process_calendly_data, run_remove_duplicate_columns,
    run_remove_excel_duplicates, run_add_stage_column
)

def save_uploaded_file(uploaded_file, source_type):
    """Save uploaded file to the uploads directory"""
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    file_path = os.path.join('uploads', f"{source_type}_{uploaded_file.name}")
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path

def preprocess_file(file_path, source_type):
    """Process a single file based on its source type"""
    try:
        print(f"Preprocessing {source_type} file: {os.path.basename(file_path)}")
        # Read the file
        if file_path.endswith('.xlsx'):
            is_linkedin = 'linkedin' in source_type.lower()
            try:
                df = pd.read_excel(file_path, sheet_name=0, skiprows=1 if is_linkedin else 0, engine='openpyxl')
                if is_linkedin:
                    print(f"Successfully read Excel file: {file_path} (skipped first row for LinkedIn)")
                else:
                    print(f"Successfully read Excel file: {file_path}")
            except Exception as e:
                print(f"Error reading Excel file {file_path}: {e}")
                return None
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
            print(f"Successfully read CSV file: {file_path}")
        else:
            print(f"Unsupported file type: {file_path}")
            return None

        # Identify source type and preprocess accordingly
        source_type, source_tags = identify_source_type(df)

        if 'linkedin' in source_type.lower() and 'india' in source_type.lower():
            df = preprocess_linkedin_india(df)
        elif 'calendly' in source_type.lower() and 'india' in source_type.lower():
            df = preprocess_calendly_india(df)

        return df
    except Exception as e:
        print(f"Error preprocessing file {file_path}: {e}")
        return None

def main():
    st.set_page_config(page_title="India Data Merger", layout="wide")
    st.title("India Data Merger Tool")

    # File upload section
    st.header("Upload India Data Files")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Naukri Data")
        naukri_files = st.file_uploader("Upload Naukri Files", type=['csv', 'xlsx'], accept_multiple_files=True, key='naukri')

    with col2:
        st.subheader("LinkedIn Data")
        linkedin_files = st.file_uploader("Upload LinkedIn Files", type=['csv', 'xlsx'], accept_multiple_files=True, key='linkedin')

    with col3:
        st.subheader("Calendly Data")
        calendly_files = st.file_uploader("Upload Calendly Files", type=['csv', 'xlsx'], accept_multiple_files=True, key='calendly')

    if st.button("Process and Merge Files"):
        with st.spinner("Processing files..."):
            try:
                # Run initial deduplication on Excel files
                run_remove_excel_duplicates()

                # Save uploaded files
                naukri_dfs = []
                linkedin_dfs = []
                calendly_dfs = []

                # Process Naukri data
                for naukri_file in naukri_files:
                    try:
                        df = pd.read_excel(naukri_file, engine='openpyxl')
                        if df is not None:
                            # Map Naukri columns
                            df['source'] = 'Naukri_India'
                            if 'name' not in df.columns and 'Name' in df.columns:
                                df['name'] = df['Name']
                            if 'email' not in df.columns and 'Email ID' in df.columns:
                                df['email'] = df['Email ID']
                            if 'phone' not in df.columns and 'Phone Number' in df.columns:
                                df['phone'] = df['Phone Number']
                            naukri_dfs.append(df)
                    except Exception as e:
                        st.error(f"Error processing Naukri file {os.path.basename(naukri_file)}: {str(e)}")

                # Process LinkedIn data
                for linkedin_file in linkedin_files:
                    try:
                        df = pd.read_excel(linkedin_file, skiprows=1, engine='openpyxl')
                        if df is not None:
                            # Map LinkedIn columns
                            df['source'] = 'LinkedIn_India'
                            # Handle LinkedIn name columns with proper null handling
                            name_cols = {
                                'first_name': ['First Name', 'first name', 'firstname'],
                                'last_name': ['Last Name', 'last name', 'lastname']
                            }
                            
                            # Find actual column names
                            first_name_col = next((col for col in df.columns for variant in name_cols['first_name'] if variant.lower() == col.lower()), None)
                            last_name_col = next((col for col in df.columns for variant in name_cols['last_name'] if variant.lower() == col.lower()), None)
                            
                            if first_name_col and last_name_col:
                                df['name'] = df[first_name_col].fillna('').str.strip() + ' ' + df[last_name_col].fillna('').str.strip()
                                df['name'] = df['name'].str.strip()
                                df = df.drop([first_name_col, last_name_col], axis=1)
                                print(f"Successfully combined {first_name_col} and {last_name_col} into 'name' column")
                            if 'Email Address' in df.columns:
                                df['email'] = df['Email Address']
                            if 'Phone Number' in df.columns:
                                df['phone'] = df['Phone Number']
                            if 'Current Title' in df.columns:
                                df['position'] = df['Current Title']
                            linkedin_dfs.append(df)
                            print(f"Successfully processed LinkedIn file with {len(df)} records")
                    except Exception as e:
                        st.error(f"Error processing LinkedIn file {os.path.basename(linkedin_file)}: {str(e)}")

                # Process Calendly data
                for calendly_file in calendly_files:
                    save_uploaded_file(calendly_file, "Calendly_India")


                # Run the merge process
                run_merge_all_files()

                # Run data cleaning and processing steps
                run_copy_title_experience()
                run_clean_india_data()
                run_remove_duplicate_columns()
                run_remove_duplicates()
                run_process_calendly_data()
                run_add_stage_column()

                # Load final data
                if os.path.exists('merged_india_data.csv'):
                    final_df = pd.read_csv('merged_india_data.csv')

                    # Display results
                    st.success("Data processing completed!")
                    st.header("Final Merged Data")
                    st.dataframe(final_df)

                    # Display source distribution
                    st.subheader("Records by Source")
                    source_counts = final_df['source'].value_counts()
                    for source, count in source_counts.items():
                        st.write(f"- {source}: {count} records")

                    # Add download button
                    st.download_button(
                        label="Download Merged Data",
                        data=final_df.to_csv(index=False),
                        file_name="merged_india_data.csv",
                        mime="text/csv"
                    )

            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()