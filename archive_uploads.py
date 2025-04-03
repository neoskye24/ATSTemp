
import os
import shutil
from datetime import datetime

def archive_uploads():
    """Archive uploaded files after processing to prevent confusion with new uploads"""
    # Create archive directory with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_dir = os.path.join('archives', f'uploads_{timestamp}')
    
    if not os.path.exists('archives'):
        os.makedirs('archives')
        
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    
    # Move files from uploads directory to archive
    uploads_dir = 'uploads'
    if not os.path.exists(uploads_dir):
        return
        
    files_moved = 0
    for filename in os.listdir(uploads_dir):
        file_path = os.path.join(uploads_dir, filename)
        if os.path.isfile(file_path):
            shutil.move(file_path, os.path.join(archive_dir, filename))
            files_moved += 1
            
    # Create placeholder file in uploads to maintain directory
    with open(os.path.join(uploads_dir, 'README.txt'), 'w') as f:
        f.write('Place new data files in this directory for processing.')
    
    return archive_dir, files_moved

if __name__ == "__main__":
    archive_dir, count = archive_uploads()
    print(f"Archived {count} files to {archive_dir}")
