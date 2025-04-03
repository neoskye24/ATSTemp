
import pandas as pd

def standardize_merged_data():
    """Standardize names in both merged US and India data"""
    # Process US data
    try:
        df_us = pd.read_csv('merged_us_data.csv')
        print("Processing US data names...")
        # Apply title() to names
        df_us['name'] = df_us['name'].str.title()
        df_us.to_csv('merged_us_data.csv', index=False)
        print(f"Processed {len(df_us)} US data names")
    except Exception as e:
        print(f"Error processing US data: {str(e)}")
    
    # Process India data
    try:
        df_india = pd.read_csv('merged_india_data.csv')
        print("Processing India data names...")
        # Apply title() to names
        df_india['name'] = df_india['name'].str.title()
        df_india.to_csv('merged_india_data.csv', index=False)
        print(f"Processed {len(df_india)} India data names")
    except Exception as e:
        print(f"Error processing India data: {str(e)}")

if __name__ == "__main__":
    standardize_merged_data()
