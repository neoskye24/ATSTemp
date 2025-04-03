
import pandas as pd

def determine_stage(df):
    """Determine stage based on rejection status and Calendly data"""
    # Create a new stage column with default "Call Stage"
    df['stage'] = "Call Stage"

    # Check for rejection in any column - specifically status, no-show, and other relevant columns
    columns_to_check = ['status', 'no-show']

    # Add any additional columns that might contain status information
    for col in df.columns:
        if 'status' in col.lower() or 'rejection' in col.lower() or 'result' in col.lower():
            if col not in columns_to_check:
                columns_to_check.append(col)

    # Find rows with rejection status
    rejection_mask = pd.Series(False, index=df.index)

    for col in columns_to_check:
        if col in df.columns:
            # Check for variations of "reject" in any status column
            col_check = df[col].astype(str).str.lower().str.contains('reject|rejected|no show', na=False)
            rejection_mask = rejection_mask | col_check

    # Set stage to "Rejected" for rejected candidates
    if rejection_mask.any():
        df.loc[rejection_mask, 'stage'] = "Rejected"

    # Check for Calendly evidence - if source contains 'Calendly' or specific columns exist
    calendly_mask = pd.Series(False, index=df.index)

    # Check source column
    if 'source' in df.columns:
        source_check = df['source'].astype(str).str.lower().str.contains('calendly', na=False)
        calendly_mask = calendly_mask | source_check

    # Check if date column exists and has values (typically indicates Calendly scheduling)
    if 'date' in df.columns:
        date_check = df['date'].notna()
        calendly_mask = calendly_mask | date_check

    # Set stage to "Scheduled" for candidates with Calendly evidence who aren't rejected
    scheduled_mask = calendly_mask & ~rejection_mask
    if scheduled_mask.any():
        df.loc[scheduled_mask, 'stage'] = "Scheduled"
        
    return df

def add_stage_column_to_file(file_path):
    """Add stage column to a data file"""
    try:
        # Read the file
        df = pd.read_csv(file_path)
        
        # Determine stage
        df = determine_stage(df)
        
        # Move stage column to the first position
        cols = df.columns.tolist()
        cols.remove('stage')
        df = df[['stage'] + cols]
        
        # Save the modified dataframe
        df.to_csv(file_path, index=False)
        
        return True
    except Exception as e:
        print(f"Error adding stage column to {file_path}: {e}")
        return False
