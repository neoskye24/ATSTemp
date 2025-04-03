
import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime

def create_dashboard(region='US', dashboard_type='call_stage'):
    """Create dashboards for different candidate stages
    
    Args:
        region: 'US' or 'INDIA'
        dashboard_type: 'call_stage' or 'calendly_next_steps'
    """
    # Define the input file based on region
    if region.upper() == 'US':
        input_file = 'merged_us_data.csv'
    elif region.upper() == 'INDIA':
        input_file = 'merged_india_data.csv'
    else:
        return

    if not os.path.exists(input_file):
        return

    try:
        # Read the data
        df = pd.read_csv(input_file)

        # Check if stage column exists
        if 'stage' not in df.columns:
            return

        # Filter based on dashboard type
        if dashboard_type == 'call_stage':
            # Filter for Call Stage candidates (not scheduled, not rejected)
            filtered_df = df[df['stage'] == 'Call Stage'].copy()
            title_suffix = 'Call Stage Candidates'
        elif dashboard_type == 'calendly_next_steps':
            # Filter for non-rejected candidates from Calendly
            # Create mask for Calendly records
            calendly_mask = pd.Series(False, index=df.index)
            
            # Check source column first
            if 'source' in df.columns:
                source_mask = df['source'].str.contains('Calendly', case=False, na=False)
                calendly_mask = calendly_mask | source_mask
                print(f"Found {source_mask.sum()} records with Calendly in source column")
            
            # Also check if date column exists and has values (indicating a scheduled meeting)
            if 'date' in df.columns:
                date_mask = df['date'].notna()
                calendly_mask = calendly_mask | date_mask
                print(f"Found {date_mask.sum()} records with date values")
            
            # Exclude rejected candidates
            not_rejected_mask = df['stage'] != 'Rejected'
            print(f"Found {not_rejected_mask.sum()} non-rejected records")
            
            # Combine masks to get final filter
            filtered_df = df[calendly_mask & not_rejected_mask].copy()
            
            print(f"Final count for {region} Calendly Next Steps: {len(filtered_df)} records")
            if len(filtered_df) == 0:
                print(f"No Calendly Next Steps candidates found for {region}")
            
            title_suffix = 'Calendly Next Steps'
        else:
            return

        if len(filtered_df) == 0:
            return

        # Create dashboard directory if it doesn't exist
        dashboard_dir = 'dashboards'
        if not os.path.exists(dashboard_dir):
            os.makedirs(dashboard_dir)

        # Current date for filename
        current_date = datetime.now().strftime('%Y%m%d')

        # Export to CSV
        output_csv = os.path.join(dashboard_dir, f'{region.lower()}_{dashboard_type}_{current_date}.csv')
        
        # Select and reorder columns for dashboard
        dashboard_columns = ['stage']

        # Add important columns in a specific order if they exist
        important_cols = ['name', 'email', 'phone', 'source', 'position', 'experience', 
                          'location', 'date', 'status', 'no-show', 'file_source']

        for col in important_cols:
            if col in filtered_df.columns:
                dashboard_columns.append(col)

        # Add any remaining columns
        for col in filtered_df.columns:
            if col not in dashboard_columns:
                dashboard_columns.append(col)

        # Filter to only columns that exist
        dashboard_columns = [col for col in dashboard_columns if col in filtered_df.columns]

        # Write to CSV
        filtered_df[dashboard_columns].to_csv(output_csv, index=False)

        # Generate summary statistics
        if 'source' in filtered_df.columns:
            source_counts = filtered_df['source'].value_counts()
        else:
            source_counts = pd.Series({"No source data": len(filtered_df)})

        # Create summary figures directory
        figures_dir = os.path.join(dashboard_dir, 'figures')
        if not os.path.exists(figures_dir):
            os.makedirs(figures_dir)

        # Plot source distribution
        plt.figure(figsize=(10, 6))
        source_counts.plot(kind='bar')
        plt.title(f'{region} {title_suffix} by Source')
        plt.xlabel('Source')
        plt.ylabel('Number of Candidates')
        plt.tight_layout()
        
        source_fig_path = os.path.join(figures_dir, f'{region.lower()}_{dashboard_type}_sources_{current_date}.png')
        plt.savefig(source_fig_path, dpi=300)
        print(f"Saved source distribution plot to: {source_fig_path}")
        plt.close()

        # If position column exists, plot position distribution (top 10)
        if 'position' in filtered_df.columns:
            position_counts = filtered_df['position'].value_counts().head(10)

            plt.figure(figsize=(12, 6))
            position_counts.plot(kind='barh')
            plt.title(f'{region} {title_suffix} - Top 10 Positions')
            plt.xlabel('Number of Candidates')
            plt.ylabel('Position')
            plt.tight_layout()
            
            position_fig_path = os.path.join(figures_dir, f'{region.lower()}_{dashboard_type}_positions_{current_date}.png')
            plt.savefig(position_fig_path, dpi=300)
            print(f"Saved position distribution plot to: {position_fig_path}")
            plt.close()

        # Create a summary text file
        summary_file = os.path.join(dashboard_dir, f'{region.lower()}_{dashboard_type}_summary_{current_date}.txt')

        with open(summary_file, 'w') as f:
            f.write(f"{region.upper()} {title_suffix.upper()} SUMMARY\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total candidates: {len(filtered_df)}\n\n")

            f.write("SOURCE DISTRIBUTION:\n")
            for source, count in source_counts.items():
                f.write(f"{source}: {count} candidates\n")

            if 'position' in filtered_df.columns:
                f.write("\nTOP 10 POSITIONS:\n")
                for position, count in position_counts.items():
                    f.write(f"{position}: {count} candidates\n")

            # Additional metrics specific to Calendly Next Steps dashboard
            if dashboard_type == 'calendly_next_steps':
                f.write("\nSTATUS BREAKDOWN:\n")
                if 'status' in filtered_df.columns:
                    status_counts = filtered_df['status'].value_counts()
                    for status, count in status_counts.items():
                        f.write(f"{status}: {count} candidates\n")
                
                f.write("\nSCHEDULED DATE BREAKDOWN:\n")
                if 'date' in filtered_df.columns:
                    # Try to convert to datetime if not already
                    if not pd.api.types.is_datetime64_any_dtype(filtered_df['date']):
                        try:
                            filtered_df['date'] = pd.to_datetime(filtered_df['date'])
                        except:
                            pass
                    
                    # Group by date if successful
                    if pd.api.types.is_datetime64_any_dtype(filtered_df['date']):
                        date_counts = filtered_df['date'].dt.date.value_counts().sort_index()
                        for date, count in date_counts.items():
                            f.write(f"{date}: {count} candidates\n")
                    else:
                        f.write("Could not parse date information for breakdown\n")

    except Exception as e:
        print(f"Error creating {dashboard_type} dashboard for {region}: {e}")

def main():
    """Create dashboards for both US and India regions"""
    # Create US Call Stage dashboard
    create_dashboard('US', 'call_stage')
    
    # Create India Call Stage dashboard
    create_dashboard('INDIA', 'call_stage')
    
    # Create US Calendly Next Steps dashboard
    create_dashboard('US', 'calendly_next_steps')
    
    # Create India Calendly Next Steps dashboard
    create_dashboard('INDIA', 'calendly_next_steps')

if __name__ == "__main__":
    main()
