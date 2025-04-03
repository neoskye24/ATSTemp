
import pandas as pd
import os
import re

def standardize_name(name):
    """Standardize name format to First Last with proper capitalization"""
    if pd.isna(name) or name == '':
        return name
    
    # Convert to string and remove extra whitespace
    name = str(name).strip()
    
    # Handle multiple spaces between words
    name = ' '.join(name.split())
    
    # Split the name into words
    words = name.split()
    
    # Capitalize first letter of each word, rest lowercase
    standardized_words = []
    for word in words:
        if word:
            # First lowercase the entire word
            word = word.lower()
            # Then capitalize just the first letter
            word = word[0].upper() + word[1:]
            standardized_words.append(word)
    
    # Join words back together with single space
    return ' '.join(standardized_words)

def standardize_phone(phone):
    """Standardize phone numbers to be 10, 11, or 12 digits"""
    if pd.isna(phone) or phone == '':
        return phone
    
    # Convert to string if not already
    phone = str(phone)
    
    # Remove all non-numeric characters
    digits = re.sub(r'\D', '', phone)
    
    # Check if the number has 10, 11 or 12 digits
    if len(digits) == 10:
        # 10 digits (no country code)
        return digits
    elif len(digits) == 11 and digits.startswith('1'):
        # 11 digits with US country code
        return digits
    elif len(digits) == 12 and digits.startswith('91'):
        # 12 digits with India country code
        return digits
    elif len(digits) > 10:
        # If more digits, try to extract last 10 digits as the main number
        return digits[-10:]
    else:
        # Return whatever we have if less than 10 digits
        return digits

def preprocess_files():
    """Preprocess database files to standardize names and phone numbers"""
    files_to_process = [
        'merged_us_data.csv',
        'merged_india_data.csv'
    ]
    
    for file_path in files_to_process:
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist. Skipping.")
            continue
        
        print(f"Processing {file_path}...")
        
        # Read the file
        df = pd.read_csv(file_path)
        
        # Keep track of changes
        name_changes = 0
        phone_changes = 0
        
        # Standardize names
        if 'name' in df.columns:
            original_names = df['name'].copy()
            #df['name'] = df['name'].apply(standardize_name)
            df['name']=df['name'].str.title()
            name_changes = (original_names != df['name']).sum()
            print(f"Standardized {name_changes} names")
        
        # Standardize phone numbers
        if 'phone' in df.columns:
            original_phones = df['phone'].copy()
            df['phone'] = df['phone'].apply(standardize_phone)
            phone_changes = (original_phones != df['phone']).sum()
            print(f"Standardized {phone_changes} phone numbers")
        
        # Save the changes
        df.to_csv(file_path, index=False)
        print(f"Saved changes to {file_path}")
        
        # Look for potential matches after standardization
        find_potential_matches(df)

def find_potential_matches(df):
    """Find potential matches between different sources in the dataframe"""
    if 'source' not in df.columns:
        print("No 'source' column found. Cannot find matches between sources.")
        return
    
    # Get unique sources
    sources = df['source'].unique()
    
    if len(sources) < 2:
        print("Only one source found. No cross-source matches to find.")
        return
    
    print("\nLooking for potential matches between sources...")
    
    # Group by name and check for multiple sources
    name_groups = df.groupby('name')
    potential_matches = 0
    
    for name, group in name_groups:
        if len(group['source'].unique()) > 1:
            # This name appears in multiple sources
            potential_matches += 1
            
            # Check if phone numbers also match
            if 'phone' in df.columns:
                phone_match = len(group['phone'].unique()) == 1 and not pd.isna(group['phone'].iloc[0])
                if phone_match:
                    print(f"Found match on name '{name}' and phone across sources: {', '.join(group['source'].unique())}")
    
    print(f"Found {potential_matches} potential matches across different sources")

def merge_calendly_with_main_data():
    """Specifically find and merge Calendly records with Indeed/LinkedIn data"""
    # Process US data
    if os.path.exists('merged_us_data.csv'):
        df_us = pd.read_csv('merged_us_data.csv')
        
        # Identify Calendly records
        calendly_mask = df_us['source'].str.contains('Calendly', case=False, na=False)
        other_mask = ~calendly_mask
        
        if calendly_mask.any() and other_mask.any():
            calendly_records = df_us[calendly_mask]
            other_records = df_us[other_mask]
            
            print(f"\nUS Data: Found {len(calendly_records)} Calendly records and {len(other_records)} other records")
            
            # Find matches based on standardized name and phone
            match_count = 0
            
            for idx, calendly_row in calendly_records.iterrows():
                # Find matches in other sources by name and phone
                name_matches = other_records[other_records['name'] == calendly_row['name']]
                
                if len(name_matches) > 0:
                    # Check for phone matches
                    if 'phone' in df_us.columns:
                        phone_matches = name_matches[name_matches['phone'] == calendly_row['phone']]
                        
                        if len(phone_matches) > 0:
                            match_count += 1
                            print(f"Match found: {calendly_row['name']} with phone {calendly_row['phone']}")
            
            print(f"Total matches found in US data: {match_count}")
    
    # Process India data
    if os.path.exists('merged_india_data.csv'):
        df_india = pd.read_csv('merged_india_data.csv')
        
        # Identify Calendly records
        calendly_mask = df_india['source'].str.contains('Calendly', case=False, na=False)
        other_mask = ~calendly_mask
        
        if calendly_mask.any() and other_mask.any():
            calendly_records = df_india[calendly_mask]
            other_records = df_india[other_mask]
            
            print(f"\nIndia Data: Found {len(calendly_records)} Calendly records and {len(other_records)} other records")
            
            # Find matches based on standardized name and phone
            match_count = 0
            
            for idx, calendly_row in calendly_records.iterrows():
                # Find matches in other sources by name and phone
                name_matches = other_records[other_records['name'] == calendly_row['name']]
                
                if len(name_matches) > 0:
                    # Check for phone matches
                    if 'phone' in df_india.columns:
                        phone_matches = name_matches[name_matches['phone'] == calendly_row['phone']]
                        
                        if len(phone_matches) > 0:
                            match_count += 1
                            print(f"Match found: {calendly_row['name']} with phone {calendly_row['phone']}")
            
            print(f"Total matches found in India data: {match_count}")

if __name__ == "__main__":
    print("Starting database preprocessing...")
    preprocess_files()
    print("\nLooking for Calendly records to merge...")
    merge_calendly_with_main_data()
    print("\nPreprocessing complete!")
