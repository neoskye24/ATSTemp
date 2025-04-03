import pandas as pd
import os
from utils.stage_management import add_stage_column_to_file

def main():
    """Add stage column to all merged data files"""
    # Process US data
    us_success = add_stage_column_to_file('merged_us_data.csv')
    if us_success:
        print("Successfully added stage column to US data")
    else:
        print("Failed to add stage column to US data")

    # Process India data
    india_success = add_stage_column_to_file('merged_india_data.csv')
    if india_success:
        print("Successfully added stage column to India data")
    else:
        print("Failed to add stage column to India data")

    # Process all data
    all_success = add_stage_column_to_file('merged_all_data.csv')
    if all_success:
        print("Successfully added stage column to all data")
    else:
        print("Failed to add stage column to all data")

if __name__ == "__main__":
    main()