
import pandas as pd
import os
from datetime import datetime

def preprocess_calendly(file_path, is_us=True):
    """Process Calendly data from US or India"""
    country_name = "US" if is_us else "India"
    print(f"\n===== PROCESSING CALENDLY {country_name} DATA =====")
    print(f"Processing Calendly {country_name} data from {file_path}...")
    
    try:
        # Read the Calendly file
        df = pd.read_csv(file_path)
        print(f"Read {len(df)} records from {file_path}")
        
        # Apply appropriate preprocessing based on country
        if is_us:
            # Process US Calendly data - keep only specific columns
            columns_to_keep = [2, 5, 8, 13, 18, 20, 22, 26, 37, 38]  # 0-based indices
            
            # Get all column indices
            all_columns = list(range(len(df.columns)))
            
            # Get columns to drop
            columns_to_drop = [i for i in all_columns if i not in columns_to_keep]
            
            # Drop columns
            if columns_to_drop:
                df = df.drop(df.columns[columns_to_drop], axis=1)
                
            # Rename remaining columns
            new_column_names = [
                'name',
                'email',
                'phone',
                'date',
                'profile',
                'salary',
                'declaration',
                'position',
                'no-show',
                'status'
            ]
            
            # Apply column names if counts match
            if len(df.columns) == len(new_column_names):
                df.columns = new_column_names
                print(f"  Renamed {len(new_column_names)} columns for Calendly US data")
            else:
                print(f"  Warning: Column count mismatch in Calendly US data")
        else:
            # Process India Calendly data - keep only specific columns
            columns_to_keep = [2, 5, 10, 18, 20, 22, 26, 28, 30, 41, 42]  # 0-based indices
            
            # Get all column indices
            all_columns = list(range(len(df.columns)))
            
            # Get columns to drop
            columns_to_drop = [i for i in all_columns if i not in columns_to_keep]
            
            # Drop columns
            if columns_to_drop:
                df = df.drop(df.columns[columns_to_drop], axis=1)
                
            # Rename remaining columns
            new_column_names = [
                'name',
                'email',
                'date',
                'profile',
                'salary',
                'consent',
                'position',
                'source',
                'phone',
                'no-show',
                'status'
            ]
            
            # Apply column names if counts match
            if len(df.columns) == len(new_column_names):
                df.columns = new_column_names
                print(f"  Renamed {len(new_column_names)} columns for Calendly India data")
            else:
                print(f"  Warning: Column count mismatch in Calendly India data")
        
        # Add source column
        df['source'] = f'Calendly_{country_name}'
        
        # Convert date strings to datetime objects and sort by date
        if 'date' in df.columns:
            try:
                # Try multiple date formats
                date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%B %d, %Y']
                
                for date_format in date_formats:
                    try:
                        df['date'] = pd.to_datetime(df['date'], format=date_format)
                        print(f"  Successfully parsed dates using format: {date_format}")
                        break
                    except:
                        continue
                
                # Sort by date (most recent first)
                df = df.sort_values('date', ascending=False)
                print("  Sorted records by date (most recent first)")
            except Exception as e:
                print(f"  Warning: Could not parse date column - {e}")
        
        # Print sample data
        print("\nCalendly data sample (first 3 rows):")
        print(df[['name', 'email', 'phone']].head(3))
        
        return df
    
    except Exception as e:
        print(f"Error processing Calendly {country_name} file: {e}")
        return None

def merge_calendly_with_main_data(calendly_df, main_file_path):
    """Merge Calendly data with main dataset (US or India), handling duplicates"""
    print(f"\nMerging Calendly data with {main_file_path}...")
    
    try:
        # Read main dataset
        main_df = pd.read_csv(main_file_path)
        print(f"Read {len(main_df)} records from {main_file_path}")
        
        # Print sample data from main dataset
        print("\nMain dataset sample (first 3 rows):")
        print(main_df[['name', 'email', 'phone']].head(3))
        
        # Create identifiers for deduplication
        # For main dataset
        main_df['name_lower'] = main_df['name'].str.lower().fillna('')
        main_df['email_lower'] = main_df['email'].str.lower().fillna('')
        main_df['phone_norm'] = main_df['phone'].astype(str).str.replace(r'\D', '', regex=True).fillna('')
        
        # For Calendly dataset
        calendly_df['name_lower'] = calendly_df['name'].str.lower().fillna('')
        calendly_df['email_lower'] = calendly_df['email'].str.lower().fillna('')
        calendly_df['phone_norm'] = calendly_df['phone'].astype(str).str.replace(r'\D', '', regex=True).fillna('')
        
        # Create matching keys
        main_df['name_email_key'] = main_df['name_lower'] + '_' + main_df['email_lower']
        main_df['name_phone_key'] = main_df['name_lower'] + '_' + main_df['phone_norm']
        
        calendly_df['name_email_key'] = calendly_df['name_lower'] + '_' + calendly_df['email_lower']
        calendly_df['name_phone_key'] = calendly_df['name_lower'] + '_' + calendly_df['phone_norm']
        
        # Debug: print some sample keys from both datasets
        print("\nSample matching keys from main dataset:")
        print(main_df[['name', 'name_email_key', 'name_phone_key']].head(3))
        
        print("\nSample matching keys from Calendly dataset:")
        print(calendly_df[['name', 'name_email_key', 'name_phone_key']].head(3))
        
        # Check for empty keys that might cause matching issues
        empty_email_keys_main = (main_df['name_email_key'] == '_').sum()
        empty_phone_keys_main = (main_df['name_phone_key'] == '_').sum()
        empty_email_keys_calendly = (calendly_df['name_email_key'] == '_').sum()
        empty_phone_keys_calendly = (calendly_df['name_phone_key'] == '_').sum()
        
        print(f"\nEmpty key check - Main dataset: {empty_email_keys_main} empty email keys, {empty_phone_keys_main} empty phone keys")
        print(f"Empty key check - Calendly dataset: {empty_email_keys_calendly} empty email keys, {empty_phone_keys_calendly} empty phone keys")
        
        # Find matches by name+email
        main_email_keys = set(main_df['name_email_key'].tolist())
        calendly_matches_by_email = calendly_df['name_email_key'].isin(main_email_keys)
        email_matches = calendly_matches_by_email.sum()
        print(f"Found {email_matches} name+email matches between datasets")
        
        # Find matches by name+phone
        main_phone_keys = set(main_df['name_phone_key'].tolist())
        calendly_matches_by_phone = calendly_df['name_phone_key'].isin(main_phone_keys)
        phone_matches = calendly_matches_by_phone.sum()
        print(f"Found {phone_matches} name+phone matches between datasets")
        
        # Show some examples of matching records
        if email_matches > 0:
            print("\nExamples of email matches:")
            matched_emails = calendly_df[calendly_matches_by_email].head(3)
            for idx, row in matched_emails.iterrows():
                print(f"Calendly: {row['name']} ({row['email']})")
                # Find the matching record in main_df
                match = main_df[main_df['name_email_key'] == row['name_email_key']].iloc[0]
                print(f"Main: {match['name']} ({match['email']})")
                print("---")
        
        if phone_matches > 0:
            print("\nExamples of phone matches:")
            matched_phones = calendly_df[calendly_matches_by_phone].head(3)
            for idx, row in matched_phones.iterrows():
                print(f"Calendly: {row['name']} ({row['phone']})")
                # Find the matching record in main_df
                match = main_df[main_df['name_phone_key'] == row['name_phone_key']].iloc[0]
                print(f"Main: {match['name']} ({match['phone']})")
                print("---")
        
        # Combined matches
        all_matches = calendly_matches_by_email | calendly_matches_by_phone
        print(f"Total of {all_matches.sum()} matching records found")
        
        # Split Calendly data into matching and non-matching records
        matching_calendly = calendly_df[all_matches].copy()
        non_matching_calendly = calendly_df[~all_matches].copy()
        
        print(f"Processing {len(matching_calendly)} matching records and {len(non_matching_calendly)} new records")
        
        # For matching records, we'll update specific fields in main_df
        updated_count = 0
        if len(matching_calendly) > 0:
            # Update main dataset with information from matching Calendly records
            for idx, calendly_row in matching_calendly.iterrows():
                # Find matching rows in main_df
                email_match_mask = main_df['name_email_key'] == calendly_row['name_email_key']
                phone_match_mask = main_df['name_phone_key'] == calendly_row['name_phone_key']
                match_mask = email_match_mask | phone_match_mask
                
                if match_mask.any():
                    # Get the indices of matching rows
                    match_indices = main_df.index[match_mask].tolist()
                    
                    # For each matching row in main_df
                    for main_idx in match_indices:
                        updated = False
                        
                        # Update status if available
                        if 'status' in calendly_row and not pd.isna(calendly_row['status']):
                            main_df.at[main_idx, 'status'] = calendly_row['status']
                            updated = True
                        
                        # Update other relevant fields as needed
                        # For example, you might want to update phone or email if one is missing
                        if pd.isna(main_df.at[main_idx, 'phone']) and not pd.isna(calendly_row['phone']):
                            main_df.at[main_idx, 'phone'] = calendly_row['phone']
                            updated = True
                            
                        if pd.isna(main_df.at[main_idx, 'email']) and not pd.isna(calendly_row['email']):
                            main_df.at[main_idx, 'email'] = calendly_row['email']
                            updated = True
                        
                        if updated:
                            updated_count += 1
        
        print(f"Updated {updated_count} records in the main dataset")
        
        # Clean up temporary columns from both datasets
        columns_to_drop = ['name_lower', 'email_lower', 'phone_norm', 'name_email_key', 'name_phone_key']
        main_df = main_df.drop(columns=columns_to_drop)
        non_matching_calendly = non_matching_calendly.drop(columns=columns_to_drop)
        
        # Ensure both dataframes have the same columns before concatenation
        main_columns = set(main_df.columns)
        calendly_columns = set(non_matching_calendly.columns)
        
        # Add missing columns to Calendly data
        for col in main_columns:
            if col not in calendly_columns:
                non_matching_calendly[col] = None
                
        # Add missing columns to main data
        for col in calendly_columns:
            if col not in main_columns:
                main_df[col] = None
        
        # Concatenate the main dataframe with non-matching Calendly records
        final_df = pd.concat([main_df, non_matching_calendly[main_df.columns]], ignore_index=True)
        
        # Save the merged data
        final_df.to_csv(main_file_path, index=False)
        
        print(f"Successfully merged Calendly data with {main_file_path}")
        print(f"Final record count: {len(final_df)}")
        
        # Print source distribution
        if 'source' in final_df.columns:
            source_counts = final_df['source'].value_counts()
            print("\nSource distribution after merging:")
            for source, count in source_counts.items():
                print(f"- {source}: {count} records")
        
        return final_df
    
    except Exception as e:
        print(f"Error merging Calendly data: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_all_calendly_data():
    """Process all Calendly data and merge with appropriate datasets"""
    uploads_dir = 'uploads'
    
    # Check if uploads directory exists
    if not os.path.exists(uploads_dir):
        print(f"Error: {uploads_dir} directory not found")
        return
    
    # Find Calendly files
    calendly_us_files = []
    calendly_india_files = []
    
    for file in os.listdir(uploads_dir):
        filepath = os.path.join(uploads_dir, file)
        if 'calendly' in file.lower() and 'us' in file.lower():
            calendly_us_files.append(filepath)
            print(f"Found Calendly US file: {file}")
        elif ('calendly' in file.lower() and 'india' in file.lower()) or ('indiacalendly' in file.lower()):
            calendly_india_files.append(filepath)
            print(f"Found Calendly India file: {file}")
    
    # Process US Calendly data if found
    if calendly_us_files and os.path.exists('merged_us_data.csv'):
        # Combine all US Calendly files
        combined_us_calendly = None
        for file_path in calendly_us_files:
            us_calendly_df = preprocess_calendly(file_path, is_us=True)
            if us_calendly_df is not None:
                if combined_us_calendly is None:
                    combined_us_calendly = us_calendly_df
                else:
                    combined_us_calendly = pd.concat([combined_us_calendly, us_calendly_df], ignore_index=True)
                    # Sort by date if present
                    if 'date' in combined_us_calendly.columns:
                        combined_us_calendly = combined_us_calendly.sort_values('date', ascending=False)
        
        if combined_us_calendly is not None:
            print(f"Processed {len(combined_us_calendly)} total Calendly US records from {len(calendly_us_files)} files")
            merge_calendly_with_main_data(combined_us_calendly, 'merged_us_data.csv')
    else:
        print("Skipping US Calendly processing - files not found or merged_us_data.csv missing")
    
    # Process India Calendly data if found
    if calendly_india_files and os.path.exists('merged_india_data.csv'):
        # Combine all India Calendly files
        combined_india_calendly = None
        for file_path in calendly_india_files:
            india_calendly_df = preprocess_calendly(file_path, is_us=False)
            if india_calendly_df is not None:
                if combined_india_calendly is None:
                    combined_india_calendly = india_calendly_df
                else:
                    combined_india_calendly = pd.concat([combined_india_calendly, india_calendly_df], ignore_index=True)
                    # Sort by date if present
                    if 'date' in combined_india_calendly.columns:
                        combined_india_calendly = combined_india_calendly.sort_values('date', ascending=False)
        
        if combined_india_calendly is not None:
            print(f"Processed {len(combined_india_calendly)} total Calendly India records from {len(calendly_india_files)} files")
            merge_calendly_with_main_data(combined_india_calendly, 'merged_india_data.csv')
    else:
        print("Skipping India Calendly processing - files not found or merged_india_data.csv missing")

if __name__ == "__main__":
    process_all_calendly_data()
