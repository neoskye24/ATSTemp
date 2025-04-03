import os
import pandas as pd
from utils.file_merging import find_files_by_type, preprocess_file, merge_dataframes

def main():
    """Main function to merge all LinkedIn and Indeed files"""
    print("Starting the process to merge all files...")

    # Find all files by type
    files = find_files_by_type()

    # Report findings
    for file_type, paths in files.items():
        if paths:
            print(f"Found {len(paths)} {file_type.replace('_', ' ')} files:")
            for path in paths:
                print(f"  - {os.path.basename(path)}")
        else:
            print(f"No {file_type.replace('_', ' ')} files found")

    # Process and merge US data files
    indeed_us_df = None
    if files['indeed_us']:
        indeed_us_df = preprocess_file(files['indeed_us'][0], 'Indeed_US')

    linkedin_us_df = None
    if files['linkedin_us']:
        linkedin_us_df = preprocess_file(files['linkedin_us'][0], 'Linkedin_US')

    # Process and merge India data files
    linkedin_india_df = None
    if files['linkedin_india']:
        linkedin_india_df = preprocess_file(files['linkedin_india'][0], 'Linkedin_India')

    naukri_df = None
    if files['naukri']:
        naukri_df = preprocess_file(files['naukri'][0], 'Naukri_India')

    # Merge US data (Indeed + LinkedIn)
    us_merged = merge_dataframes(
        [indeed_us_df, linkedin_us_df],
        'merged_us_data.csv'
    )

    # Merge India data (Naukri + LinkedIn)
    india_merged = merge_dataframes(
        [naukri_df, linkedin_india_df],
        'merged_india_data.csv'
    )

    # Merge all data
    all_merged = merge_dataframes(
        [indeed_us_df, linkedin_us_df, naukri_df, linkedin_india_df],
        'merged_all_data.csv'
    )

    # Print summary
    print("\nMerging process complete!")
    if us_merged is not None:
        print(f"US data (Indeed + LinkedIn): {len(us_merged)} total records merged")
    if india_merged is not None:
        print(f"India data (Naukri + LinkedIn): {len(india_merged)} total records merged")
    if all_merged is not None:
        print(f"All data combined: {len(all_merged)} total records merged")

    print("\nThe following files have been created:")
    if us_merged is not None:
        print("- merged_us_data.csv (Indeed US + LinkedIn US)")
    if india_merged is not None:
        print("- merged_india_data.csv (Naukri + LinkedIn India)")
    if all_merged is not None:
        print("- merged_all_data.csv (All sources combined)")

if __name__ == "__main__":
    main()