
import pandas as pd

# Read the merged India CSV file
print("Reading merged_india_data.csv...")
df = pd.read_csv('merged_india_data.csv')

# Check if active_project column exists
if 'active_project' in df.columns:
    # Remove the active_project column
    print("Removing active_project column...")
    df = df.drop(columns=['active_project'])
    
    # Save the modified dataframe back to CSV
    df.to_csv('merged_india_data.csv', index=False)
    print("Successfully removed active_project column from merged_india_data.csv")
else:
    print("active_project column not found in the CSV file")
