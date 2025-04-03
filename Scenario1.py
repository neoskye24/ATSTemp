
import streamlit as st
import pandas as pd
import os
from datetime import datetime

def process_uploaded_file(uploaded_file, file_type):
    """Process uploaded file and return dataframe"""
    if uploaded_file is not None:
        try:
            file_ext = uploaded_file.name.split('.')[-1]
            df = pd.read_csv(uploaded_file) if file_ext == 'csv' else pd.read_excel(uploaded_file)
            df['source'] = file_type
            return df
        except Exception as e:
            st.error(f"Error processing {file_type}: {str(e)}")
            return None
    return None

def merge_dataframes(dfs):
    """Merge list of dataframes"""
    if not dfs:
        return None
    
    # Get all unique columns
    all_columns = set()
    for df in dfs:
        if df is not None:
            all_columns.update(df.columns)
    
    # Add missing columns
    for df in dfs:
        if df is not None:
            for col in all_columns:
                if col not in df.columns:
                    df[col] = None
    
    # Concatenate
    return pd.concat([df for df in dfs if df is not None], ignore_index=True)

def main():
    st.set_page_config(page_title="File Upload System", layout="wide")
    st.title("Candidate Data File Upload System")

    # Create three columns for better organization
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.subheader("US Data")
        indeed_us_files = st.file_uploader("Upload Indeed US Data (Multiple)", type=['csv', 'xlsx'], accept_multiple_files=True, key='indeed_us')
        linkedin_us_files = st.file_uploader("Upload LinkedIn US Data (Multiple)", type=['csv', 'xlsx'], accept_multiple_files=True, key='linkedin_us')
        
        
        if st.button("Process US Files"):
            us_dfs = []
            for indeed_file in indeed_us_files:
                df = process_uploaded_file(indeed_file, "Indeed_US")
                if df is not None:
                    us_dfs.append(df)
            for linkedin_file in linkedin_us_files:
                df = process_uploaded_file(linkedin_us, "LinkedIn_US")
                if df is not None:
                    us_dfs.append(df)
            
            
            if us_dfs:
                merged_us = merge_dataframes(us_dfs)
                st.write("Merged US Data:")
                st.dataframe(merged_us)
                st.success(f"Successfully processed {len(us_dfs)} US files with {len(merged_us)} total records")

    with col2:
        st.subheader("India Data")
        linkedin_india_files = st.file_uploader("Upload LinkedIn India Data (Multiple)", type=['csv', 'xlsx'], accept_multiple_files=True, key='linkedin_india')
        naukri_india = st.file_uploader("Upload Naukri India Data", type=['csv', 'xlsx'], key='naukri_india')
        
        if st.button("Process India Files"):
            india_dfs = []
            for linkedin_file in linkedin_india_files:
                df = process_uploaded_file(linkedin_india, "LinkedIn_India")
                if df is not None:
                    india_dfs.append(df)
            if naukri_india:
                df = process_uploaded_file(naukri_india, "Naukri_India")
                if df is not None:
                    india_dfs.append(df)
            
            if india_dfs:
                merged_india = merge_dataframes(india_dfs)
                st.write("Merged India Data:")
                st.dataframe(merged_india)
                st.success(f"Successfully processed {len(india_dfs)} India files with {len(merged_india)} total records")

    with col3:
        st.subheader("Calendly India Data")
        calendly_india = st.file_uploader("Upload Calendly India Data", type=['csv', 'xlsx'], key='calendly_india')
        
        if st.button("Process Calendly India File"):
            if calendly_india:
                if save_uploaded_file(calendly_india, "calendly_india"):
                    st.success("Successfully saved Calendly India file")

    with col4:
        st.subheader("Calendy US Data")
        calendly_us = st.file_uploader("Upload Calendly US Data", type=['csv', 'xlsx'], key='calendly_us')
        if calendly_us:
            df = process_uploaded_file(calendly_us, "Calendly_US")
            if df is not None:
                us_dfs.append(df)
        

    # Add a section to show currently uploaded files
    st.header("Currently Uploaded Files")
    if os.path.exists('uploads'):
        files = os.listdir('uploads')
        if files:
            for file in files:
                st.text(f"• {file}")
        else:
            st.info("No files have been uploaded yet.")

if __name__ == "__main__":
    main()
