import pandas as pd
import os
from utils.deduplication import remove_duplicates_from_dataframe

def remove_duplicates(file_path):
    """Remove duplicate records based on name+email or name+phone"""
    print(f"Reading {file_path}...")

    try:
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist")
            return

        df = pd.read_csv(file_path)
        original_count = len(df)
        print(f"Original record count: {original_count}")

        # Use the common deduplication utility
        df, removed_count = remove_duplicates_from_dataframe(df)

        # Save the deduplicated dataset
        df.to_csv(file_path, index=False)

        # Print summary
        print(f"Removed {removed_count} duplicate records")
        print(f"Final record count: {len(df)}")

        # Print source counts if available
        if 'source' in df.columns:
            source_counts = df['source'].value_counts()
            print("\nData summary after deduplication:")
            for source, count in source_counts.items():
                print(f"- {source}: {count} records")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Process both merged datasets
print("\n===== REMOVING DUPLICATES FROM US DATA =====")
remove_duplicates('merged_us_data.csv')

print("\n===== REMOVING DUPLICATES FROM INDIA DATA =====")
remove_duplicates('merged_india_data.csv')