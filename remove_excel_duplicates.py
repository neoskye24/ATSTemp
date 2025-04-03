import pandas as pd
import os
from utils.deduplication import create_deduplication_identifiers

def list_excel_files(directory='uploads'):
    """List all Excel files in the given directory"""
    excel_files = []
    for file in os.listdir(directory):
        if file.lower().endswith(('.xlsx', '.xls')):
            excel_files.append(os.path.join(directory, file))
    return excel_files

def remove_duplicates_from_excel(file_path, output_dir='uploads/deduped'):
    """Remove duplicate records based on name+email or name+phone from Excel files"""
    print(f"Processing {file_path}...")

    try:
        # Read Excel file
        if 'linkedin' in file_path.lower():
            # LinkedIn files often have header info in first row
            df = pd.read_excel(file_path, skiprows=1)
        else:
            df = pd.read_excel(file_path)

        original_count = len(df)
        print(f"Original record count: {original_count}")

        # Convert all column names to lowercase for easier matching
        df.columns = [col.lower() if isinstance(col, str) else col for col in df.columns]

        # Find name, email and phone columns
        name_cols = [col for col in df.columns if 'name' in str(col).lower()]
        email_cols = [col for col in df.columns if 'email' in str(col).lower()]
        phone_cols = [col for col in df.columns if any(phone_term in str(col).lower() for phone_term in ['phone', 'mobile', 'contact'])]

        # Check if we found the necessary columns
        if not name_cols:
            print("Error: No name column found")
            return

        name_col = name_cols[0]
        print(f"Using '{name_col}' as name column")

        # Rename columns to standard format for the deduplication utility
        if name_cols and name_col != 'name':
            df['name'] = df[name_col]

        if email_cols:
            email_col = email_cols[0]
            print(f"Using '{email_col}' as email column")
            if email_col != 'email':
                df['email'] = df[email_col]

        if phone_cols:
            phone_col = phone_cols[0]
            print(f"Using '{phone_col}' as phone column")
            if phone_col != 'phone':
                df['phone'] = df[phone_col]

        # Create deduplication identifiers
        identifiers = create_deduplication_identifiers(df)

        if not identifiers:
            print("Error: Could not create identifiers for deduplication")
            return

        # Remove duplicates based on all available methods
        df_deduped = df.drop_duplicates(subset=identifiers, keep='first')

        # Clean up temporary columns
        for col in ['name_email_id', 'name_phone_id', 'phone_norm']:
            if col in df_deduped.columns:
                df_deduped = df_deduped.drop(columns=[col])

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Get the filename without the path
        filename = os.path.basename(file_path)
        output_path = os.path.join(output_dir, f"deduped_{filename}")

        # Save deduped file
        df_deduped.to_excel(output_path, index=False)

        # Print summary
        removed = original_count - len(df_deduped)
        print(f"Removed {removed} duplicate records")
        print(f"Saved deduplicated file with {len(df_deduped)} records to {output_path}")

        return output_path

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

def main():
    """Process all Excel files in the uploads directory"""
    print("===== REMOVING DUPLICATES FROM EXCEL FILES =====")

    # Get all Excel files
    excel_files = list_excel_files()

    if not excel_files:
        print("No Excel files found in 'uploads' directory")
        return

    print(f"Found {len(excel_files)} Excel files to process")

    # Process each file
    processed_files = []
    for file_path in excel_files:
        output_path = remove_duplicates_from_excel(file_path)
        if output_path:
            processed_files.append(output_path)

    print("\n===== SUMMARY =====")
    print(f"Processed {len(processed_files)} Excel files")
    print("Deduplicated files saved to 'uploads/deduped' directory")

if __name__ == "__main__":
    main()