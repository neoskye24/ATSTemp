
import os
import zipfile
import shutil
from create_template_files import main as create_templates

def create_download_package():
    """Create a downloadable zip file with all templates"""
    # First, make sure all templates are created
    if not os.path.exists('templates'):
        print("Creating template files...")
        create_templates()
    
    # Create a zip file containing all templates
    zip_filename = 'candidate_data_templates.zip'
    
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        # Add all files from the templates directory
        for root, _, files in os.walk('templates'):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, 'templates')
                zipf.write(file_path, arcname)
    
    print(f"Download package created: {zip_filename}")
    return zip_filename

if __name__ == "__main__":
    zip_file = create_download_package()
    print(f"\nTemplate package ready for download: {zip_file}")
    print("This package contains CSV and Excel templates for all data sources:")
    print("- US templates (Indeed, LinkedIn, Calendly)")
    print("- India templates (Naukri, LinkedIn, Calendly)")
    print("- Database update templates")
    print("- README with detailed instructions")
