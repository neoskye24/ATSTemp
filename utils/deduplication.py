
import pandas as pd

def create_deduplication_identifiers(df):
    """Create name+email and name+phone identifiers for deduplication"""
    identifiers = []
    
    # Check if required columns exist
    has_name = 'name' in df.columns
    has_email = 'email' in df.columns
    has_phone = 'phone' in df.columns
    
    if not has_name:
        return []
        
    # Create identifier based on name+email
    if has_name and has_email:
        df['name_email_id'] = df['name'].str.lower().fillna('') + '_' + df['email'].str.lower().fillna('')
        identifiers.append('name_email_id')
        
    # Create identifier based on name+phone
    if has_name and has_phone:
        # Normalize phone numbers by removing non-numeric characters
        df['phone_norm'] = df['phone'].astype(str).str.replace(r'\D', '', regex=True)
        df['name_phone_id'] = df['name'].str.lower().fillna('') + '_' + df['phone_norm'].fillna('')
        identifiers.append('name_phone_id')
        
    return identifiers

def remove_duplicates_from_dataframe(df):
    """Remove duplicates from a dataframe using name+email and name+phone identifiers"""
    original_count = len(df)
    
    # Create identifiers
    identifiers = create_deduplication_identifiers(df)
    
    if not identifiers:
        return df, 0
    
    # Remove duplicates based on identifiers
    df = df.drop_duplicates(subset=identifiers, keep='first')
    
    # Clean up temporary columns
    for col in ['name_email_id', 'name_phone_id', 'phone_norm']:
        if col in df.columns:
            df = df.drop(columns=[col])
    
    removed_count = original_count - len(df)
    return df, removed_count

def find_duplicates_by_criteria(df, name, email=None, phone=None, position=None):
    """Find potential duplicate records based on name, email, or phone"""
    # Create query conditions
    conditions = []

    # Always search by name (case-insensitive partial match)
    if name:
        conditions.append(df['name'].str.contains(name, case=False, na=False))

    # Add email condition if provided
    if email and 'email' in df.columns:
        conditions.append(df['email'].str.contains(email, case=False, na=False))

    # Add phone condition if provided
    if phone and 'phone' in df.columns:
        conditions.append(df['phone'].astype(str).str.contains(phone, na=False))

    # Add position if provided
    if 'position' in df.columns:
        conditions.append(df['position'].str.contains(position, case=False, na=False))

    # Combine conditions with AND logic for more precise matching
    if conditions:
        mask = conditions[0]
        for condition in conditions[1:]:
            mask = mask & condition
        return df[mask].copy()

    return pd.DataFrame()  # Return empty dataframe if no conditions

def merge_records(record1, record2):
    """Merge two candidate records, keeping the non-null values"""
    merged = record1.copy()

    # For each field in record2, use its value if record1's value is empty
    for col in record2.index:
        if pd.isna(merged[col]) or merged[col] == '':
            merged[col] = record2[col]

    return merged
