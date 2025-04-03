
import pandas as pd
import os

def create_directory_if_not_exists(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def create_us_templates():
    """Create template files for US data"""
    templates_dir = 'templates'
    create_directory_if_not_exists(templates_dir)
    
    # Create Indeed US template
    indeed_us_template = pd.DataFrame(columns=[
        'name', 'email', 'phone', 'status', 'location', 'experience', 'position'
    ])
    
    # Add sample row for reference
    indeed_us_template.loc[0] = [
        'John Doe', 'john.doe@example.com', '(123) 456-7890', 'Applied', 
        'New York, NY', 'Software Engineer (5 years)', 'Senior Developer'
    ]
    
    # Save the template
    indeed_us_template.to_csv(os.path.join(templates_dir, 'indeed_us_template.csv'), index=False)
    indeed_us_template.to_excel(os.path.join(templates_dir, 'indeed_us_template.xlsx'), index=False)
    
    # Create LinkedIn US template
    linkedin_us_template = pd.DataFrame(columns=[
        'name', 'location', 'current_title', 'email', 'phone', 'profile_url', 'active_project', 'status'
    ])
    
    # Add sample row for reference
    linkedin_us_template.loc[0] = [
        'Jane Smith', 'San Francisco, CA', 'Full Stack Developer', 
        'jane.smith@example.com', '(987) 654-3210', 'https://linkedin.com/in/janesmith',
        'Frontend Developer', 'Interested'
    ]
    
    # Save the template
    linkedin_us_template.to_csv(os.path.join(templates_dir, 'linkedin_us_template.csv'), index=False)
    linkedin_us_template.to_excel(os.path.join(templates_dir, 'linkedin_us_template.xlsx'), index=False)
    
    # Create Calendly US template
    calendly_us_template = pd.DataFrame(columns=[
        'name', 'email', 'phone', 'date', 'profile', 'salary', 'declaration', 'position', 'no-show', 'status'
    ])
    
    # Add sample row for reference
    calendly_us_template.loc[0] = [
        'Michael Johnson', 'michael.j@example.com', '(555) 123-4567', '2025-03-15 14:30:00',
        'https://linkedin.com/in/michaelj', '$120,000', 'Yes', 'Software Engineer', 'No', 'Scheduled'
    ]
    
    # Save the template
    calendly_us_template.to_csv(os.path.join(templates_dir, 'calendly_us_template.csv'), index=False)
    calendly_us_template.to_excel(os.path.join(templates_dir, 'calendly_us_template.xlsx'), index=False)
    
    print("US templates created successfully!")

def create_india_templates():
    """Create template files for India data"""
    templates_dir = 'templates'
    create_directory_if_not_exists(templates_dir)
    
    # Create Naukri India template
    naukri_template = pd.DataFrame(columns=[
        'name', 'email', 'phone', 'location', 'total_experience', 'annual_salary', 'notice_period', 'position'
    ])
    
    # Add sample row for reference
    naukri_template.loc[0] = [
        'Raj Patel', 'raj.patel@example.com', '+91 98765 43210', 
        'Bangalore', '3.5 years', '12 LPA', '30 days', 'Software Developer'
    ]
    
    # Save the template
    naukri_template.to_csv(os.path.join(templates_dir, 'naukri_india_template.csv'), index=False)
    naukri_template.to_excel(os.path.join(templates_dir, 'naukri_india_template.xlsx'), index=False)
    
    # Create LinkedIn India template
    linkedin_india_template = pd.DataFrame(columns=[
        'name', 'location', 'current_title', 'current_company', 'email', 'phone', 'profile_url', 'active_project', 'status'
    ])
    
    # Add sample row for reference
    linkedin_india_template.loc[0] = [
        'Priya Sharma', 'Mumbai', 'Frontend Developer', 'Tech Solutions', 
        'priya.sharma@example.com', '+91 87654 32109', 'https://linkedin.com/in/priyasharma',
        'React Developer', 'Contacted'
    ]
    
    # Save the template
    linkedin_india_template.to_csv(os.path.join(templates_dir, 'linkedin_india_template.csv'), index=False)
    linkedin_india_template.to_excel(os.path.join(templates_dir, 'linkedin_india_template.xlsx'), index=False)
    
    # Create Calendly India template
    calendly_india_template = pd.DataFrame(columns=[
        'name', 'email', 'date', 'profile', 'salary', 'consent', 'position', 'source', 'phone', 'no-show', 'status'
    ])
    
    # Add sample row for reference
    calendly_india_template.loc[0] = [
        'Amit Kumar', 'amit.kumar@example.com', '2025-03-16 10:00:00',
        'https://linkedin.com/in/amitkumar', '15 LPA', 'Yes', 'Full Stack Developer', 
        'Naukri Referral', '+91 76543 21098', 'No', 'Scheduled'
    ]
    
    # Save the template
    calendly_india_template.to_csv(os.path.join(templates_dir, 'calendly_india_template.csv'), index=False)
    calendly_india_template.to_excel(os.path.join(templates_dir, 'calendly_india_template.xlsx'), index=False)
    
    print("India templates created successfully!")

def create_database_update_template():
    """Create template for updating the entire database"""
    templates_dir = 'templates'
    create_directory_if_not_exists(templates_dir)
    
    # Create template with all possible columns for US data
    us_db_template = pd.DataFrame(columns=[
        'stage', 'name', 'email', 'phone', 'location', 'experience', 'position', 
        'status', 'date', 'profile', 'salary', 'declaration', 'no-show', 'source'
    ])
    
    # Create template with all possible columns for India data
    india_db_template = pd.DataFrame(columns=[
        'stage', 'name', 'email', 'phone', 'location', 'total_experience', 'annual_salary', 
        'notice_period', 'position', 'current_title', 'current_company', 'profile_url',
        'active_project', 'status', 'date', 'profile', 'consent', 'source', 'no-show'
    ])
    
    # Save the templates
    us_db_template.to_csv(os.path.join(templates_dir, 'us_database_update_template.csv'), index=False)
    us_db_template.to_excel(os.path.join(templates_dir, 'us_database_update_template.xlsx'), index=False)
    
    india_db_template.to_csv(os.path.join(templates_dir, 'india_database_update_template.csv'), index=False)
    india_db_template.to_excel(os.path.join(templates_dir, 'india_database_update_template.xlsx'), index=False)
    
    print("Database update templates created successfully!")

def create_readme():
    """Create a README file with instructions"""
    templates_dir = 'templates'
    create_directory_if_not_exists(templates_dir)
    
    readme_content = """# Candidate Data Templates

This directory contains template files for managing candidate data. Use these templates for different scenarios:

## Import Scenarios

### Scenario 1: Calendly File Import (Daily)
- Use the calendly_us_template or calendly_india_template
- Update at the end of each day
- System will match to existing records or create new rows
- Ensures most recent date is preserved for each candidate

### Scenario 2: Naukri, Indeed, LinkedIn File Import
- Use the appropriate template for your data source
- System will ignore duplicates and add only new records
- Matching uses name+email or name+phone

### Scenario 3: Mass Database Update
- Use us_database_update_template or india_database_update_template
- Reuploading the entire database will override current data
- Includes all possible columns for complete data management

## File Format
- Templates are available in both CSV (.csv) and Excel (.xlsx) formats
- Choose the format that works best for your workflow
- Include header row with column names exactly as shown

## Upload Instructions
1. Fill in the appropriate template with your data
2. Save the file with a descriptive name (e.g., "calendly_us_2025-03-15.csv")
3. Place the file in the 'uploads' directory
4. Run the main.py script to process the data

## Column Descriptions

### US Data Columns
- name: Full name of the candidate
- email: Email address
- phone: Phone number
- location: Current location
- experience/current_title: Work experience or current job title
- position: Position applied for
- status: Current application status
- date: For Calendly - scheduled meeting date/time
- stage: Processing stage (Call Stage, Scheduled, Rejected)

### India Data Columns
- name: Full name of the candidate
- email: Email address
- phone: Phone number
- location: Current location
- total_experience: Years of experience
- annual_salary: Current or expected salary
- notice_period: Notice period in current job
- position/current_title: Position applied for or current role
- current_company: Current employer
- stage: Processing stage (Call Stage, Scheduled, Rejected)

For any questions, please contact the system administrator.
"""
    
    with open(os.path.join(templates_dir, 'README.md'), 'w') as f:
        f.write(readme_content)
    
    print("README file created with instructions!")

def main():
    """Generate all template files"""
    print("Creating template files for candidate data management...")
    
    create_us_templates()
    create_india_templates()
    create_database_update_template()
    create_readme()
    
    print("\nAll templates created successfully in the 'templates' directory!")
    print("Use these templates to prepare data for upload following the README instructions.")

if __name__ == "__main__":
    main()
