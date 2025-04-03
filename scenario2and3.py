import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io
from openpyxl import Workbook


def load_or_create_data():
    """Load existing data or create new DataFrame"""
    current_file = 'merged_all_data.csv'
    backup_file = 'database/20250307_235609/merged_all_data.csv'
    
    if os.path.exists(current_file):
        return pd.read_csv(current_file)
    elif os.path.exists(backup_file):
        return pd.read_csv(backup_file)
    return pd.DataFrame(columns=[
        'stage', 'name', 'email', 'phone', 'location', 'experience',
        'position', 'status', 'date', 'profile', 'salary', 'declaration',
        'source', 'file_source'
    ])


def save_data(df):
    """Save DataFrame to CSV"""
    df.to_csv('merged_all_data.csv', index=False)


def main():
    st.set_page_config(page_title="Candidate Data Management", layout="wide")
    st.title("Candidate Data Template Management")

    # Load existing data
    existing_df = load_or_create_data()

    # Display data overview
    st.subheader("Current Data Overview")
    st.dataframe(existing_df, use_container_width=True)
    st.text(f"Total records: {len(existing_df)}")

    # Add download button
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        existing_df.to_excel(writer, index=False)
    
    st.download_button(
        label="Download Current Data as Excel",
        data=buffer.getvalue(),
        file_name="current_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Add upload functionality for the downloaded file
    st.subheader("Upload Modified Data")
    uploaded_file = st.file_uploader("Upload the modified Excel file", type=['xlsx'])
    
    if uploaded_file is not None:
        try:
            new_df = pd.read_excel(uploaded_file)
            if set(new_df.columns) == set(existing_df.columns):
                existing_df = new_df
                save_data(existing_df)
                st.success("Successfully updated the data!")
                
                # Refresh the data overview
                st.subheader("Updated Data Overview")
                st.dataframe(existing_df, use_container_width=True)
                st.text(f"Total records: {len(existing_df)}")
            else:
                st.error("Column mismatch. Please ensure the file structure matches the original.")
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

    # Create empty template DataFrame with same columns
    template_df = pd.DataFrame(columns=existing_df.columns)

    # Add download button for empty template
    st.subheader("1. Download Empty Template")
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        template_df.to_excel(writer, index=False)
    st.download_button(
        label="Download Empty Template Excel",
        data=buffer.getvalue(),
        file_name="candidate_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # File upload section
    st.subheader("2. Upload Filled Template")
    uploaded_file = st.file_uploader("Choose a file", type=['xlsx', 'xls'])

    if uploaded_file is not None:
        try:
            # Read uploaded file
            new_df = pd.read_excel(uploaded_file)

            # Validate columns
            if not all(col in new_df.columns for col in existing_df.columns):
                st.error(
                    "Invalid template format. Please use the provided template."
                )
                return

            # Process each row
            updates = 0
            additions = 0
            for _, row in new_df.iterrows():
                # Check if record exists (by email or phone)
                existing_record = existing_df[
                    (existing_df['email'] == row['email']) |
                    (existing_df['phone'] == row['phone'])]

                if len(existing_record) > 0:
                    # Update existing record
                    idx = existing_record.index[0]
                    for col in existing_df.columns:
                        if pd.notna(row[col]):  # Only update non-null values
                            existing_df.loc[idx, col] = row[col]
                    updates += 1
                else:
                    # Add new record
                    existing_df.loc[len(existing_df)] = row
                    additions += 1

            # Save updated data to current directory
            existing_df.to_csv('merged_all_data.csv', index=False)

            # Save to latest database folder
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = os.path.join('database', timestamp)
            os.makedirs(backup_dir, exist_ok=True)
            backup_path = os.path.join(backup_dir, 'merged_all_data.csv')
            existing_df.to_csv(backup_path, index=False)

            st.success(f"Successfully saved data and created backup in {backup_dir}")
            # Show success message
            #st.success(f"Successfully processed the file:\n"
                       #f"- Updated {updates} existing records\n"
                       #f"- Added {additions} new records")

            # Display data overview
            st.subheader("Data Overview")
            st.markdown(
                "**Data from backup: database/20250307_235609/merged_all_data.csv**"
            )

            # Display statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Records", len(existing_df))
            with col2:
                st.metric("Unique Positions",
                          len(existing_df['position'].unique()))
            with col3:
                st.metric("Sources", len(existing_df['source'].unique()))

            # Display full data
            st.subheader("All Records")
            st.dataframe(existing_df, use_container_width=True)

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")


if __name__ == "__main__":
    main()