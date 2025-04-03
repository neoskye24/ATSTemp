
import pandas as pd
import os

def standardize_name(name):
    """Standardize name format to First Last with proper capitalization"""
    if pd.isna(name) or name == '':
        return name
    
    # Convert to string if not already
    name = str(name).strip()
    
    # Split the name into words
    words = name.split()
    
    # Capitalize each word
    capitalized_words = [word.capitalize() for word in words]
    
    # Join words back together
    return ' '.join(capitalized_words)

def merge_records(main_record, calendly_record):
    """Merge a Calendly record with a main record"""
    merged = main_record.copy()
    
    # Fields to prioritize from Calendly (these will overwrite existing data)
    priority_fields = ['status', 'date', 'no-show', 'salary']
    
    # Copy priority fields from Calendly record
    for field in priority_fields:
        if field in calendly_record and not pd.isna(calendly_record[field]):
            merged[field] = calendly_record[field]
    
    # For other fields, only copy if main record has empty values
    for field in calendly_record.index:
        if field not in priority_fields and field in merged.index:
            if pd.isna(merged[field]) or merged[field] == '':
                merged[field] = calendly_record[field]
    
    # Set stage to "Scheduled" if it's a Calendly record with a date
    if 'date' in calendly_record and not pd.isna(calendly_record['date']):
        merged['stage'] = 'Scheduled'
    
    # If there's a no-show or rejection status, mark as rejected
    if ('no-show' in calendly_record and calendly_record['no-show'] == 'Yes') or \
       ('status' in calendly_record and isinstance(calendly_record['status'], str) and 'reject' in calendly_record['status'].lower()):
        merged['stage'] = 'Rejected'
    
    return merged

def merge_calendly_data(file_path):
    """Merge Calendly records with Indeed/LinkedIn data in the same file"""
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist. Skipping.")
        return
    
    print(f"Processing {file_path}...")
    
    # Read the file
    df = pd.read_csv(file_path)
    
    # Make a backup of the original file
    backup_path = f"{file_path}.bak"
    df.to_csv(backup_path, index=False)
    print(f"Created backup at {backup_path}")
    
    # Standardize names for better matching
    df['name'] = df['name'].apply(standardize_name)
    
    # Identify Calendly records
    calendly_mask = df['source'].str.contains('Calendly', case=False, na=False)
    other_mask = ~calendly_mask
    
    if not calendly_mask.any():
        print(f"No Calendly records found in {file_path}. Skipping.")
        return
    
    if not other_mask.any():
        print(f"No non-Calendly records found in {file_path}. Skipping.")
        return
    
    calendly_records = df[calendly_mask].copy()
    other_records = df[other_mask].copy()
    
    print(f"Found {len(calendly_records)} Calendly records and {len(other_records)} other records")
    
    # Track which Calendly records have been merged
    merged_indices = []
    
    # Create a dictionary of merged records
    merged_dict = {}
    
    # For each Calendly record, try to find a match
    for idx, calendly_row in calendly_records.iterrows():
        # Create field for matching
        if pd.isna(calendly_row['name']):
            continue
        
        # Find matches by name
        name_matches = other_records[other_records['name'] == calendly_row['name']]
        
        if len(name_matches) > 0:
            # If phone is available, further filter by phone
            if 'phone' in df.columns and not pd.isna(calendly_row['phone']):
                phone_matches = name_matches[name_matches['phone'] == calendly_row['phone']]
                
                if len(phone_matches) > 0:
                    # We found a match by name and phone
                    match = phone_matches.iloc[0]
                    match_idx = phone_matches.index[0]
                    
                    # Merge the records
                    merged_record = merge_records(match, calendly_row)
                    
                    # Store in dictionary for later updating the dataframe
                    merged_dict[match_idx] = merged_record
                    merged_indices.append(idx)
                    
                    print(f"Merged Calendly record for {calendly_row['name']} with matching phone")
                    continue
            
            # If no phone match or phone not available, use just the name match
            match = name_matches.iloc[0]
            match_idx = name_matches.index[0]
            
            # Merge the records
            merged_record = merge_records(match, calendly_row)
            
            # Store in dictionary for later updating the dataframe
            merged_dict[match_idx] = merged_record
            merged_indices.append(idx)
            
            print(f"Merged Calendly record for {calendly_row['name']} based on name match")
    
    # Update the dataframe with merged records
    for idx, record in merged_dict.items():
        for col, val in record.items():
            df.at[idx, col] = val
    
    # Remove the merged Calendly records to avoid duplication
    df = df.drop(merged_indices)
    
    # Save the updated dataframe
    df.to_csv(file_path, index=False)
    
    print(f"Merged {len(merged_indices)} Calendly records with existing records")
    print(f"Updated file saved to {file_path}")
    
    # Print summary of sources after merging
    source_counts = df['source'].value_counts()
    print("\nSource distribution after merging:")
    for source, count in source_counts.items():
        print(f"- {source}: {count} records")

def main():
    """Main function to process all data files"""
    print("Starting Calendly data merging process...")
    
    # Process US data
    if os.path.exists('merged_us_data.csv'):
        merge_calendly_data('merged_us_data.csv')
    else:
        print("US data file not found.")
    
    # Process India data
    if os.path.exists('merged_india_data.csv'):
        merge_calendly_data('merged_india_data.csv')
    else:
        print("India data file not found.")
    
    print("\nCalendly data merging complete!")

if __name__ == "__main__":
    main()
