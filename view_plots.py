
import os
import sys
from PIL import Image
import matplotlib.pyplot as plt

def view_plots():
    """Display the most recent plots in the console"""
    figures_dir = os.path.join('dashboards', 'figures')
    
    if not os.path.exists(figures_dir):
        print("Error: Figures directory not found. Run the dashboard creation first.")
        return
    
    # Get all png files in the figures directory
    plot_files = [f for f in os.listdir(figures_dir) if f.endswith('.png')]
    
    if not plot_files:
        print("No plot files found in the figures directory.")
        return
    
    # Sort by modification time (newest first)
    plot_files.sort(key=lambda x: os.path.getmtime(os.path.join(figures_dir, x)), reverse=True)
    
    print(f"Found {len(plot_files)} plot files:")
    for i, plot_file in enumerate(plot_files):
        print(f"{i+1}. {plot_file}")
    
    print("\nTo view plots in Replit, you can:")
    print("1. Open the Files panel and navigate to dashboards/figures")
    print("2. Click on any PNG file to view it in the Replit editor")
    print("3. Alternatively, use the Replit Files tool to browse to the figures")
    
    # Print information about the Excel dashboards too
    dashboard_dir = 'dashboards'
    excel_files = [f for f in os.listdir(dashboard_dir) if f.endswith('.xlsx')]
    
    if excel_files:
        print("\nExcel dashboards available:")
        for excel_file in excel_files:
            print(f"- {excel_file}")
        print("\nTo view Excel files, you can download them or open them in the Files panel")

if __name__ == "__main__":
    view_plots()
