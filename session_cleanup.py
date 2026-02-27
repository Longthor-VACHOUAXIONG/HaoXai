#!/usr/bin/env python3
"""
Session Cleanup Utility for Flask Application
Automatically cleans up expired session files to prevent folder bloat
"""

import os
import time
import shutil
from datetime import datetime, timedelta
import glob

class SessionCleanup:
    def __init__(self, session_folder='flask_session', max_age_hours=24):
        self.session_folder = session_folder
        self.max_age = timedelta(hours=max_age_hours)
        
    def get_folder_size(self):
        """Get current session folder size in MB"""
        total_size = 0
        if os.path.exists(self.session_folder):
            for root, dirs, files in os.walk(self.session_folder):
                for file in files:
                    file_path = os.path.join(root, file)
                    if os.path.exists(file_path):
                        total_size += os.path.getsize(file_path)
        return total_size / (1024 * 1024)  # Convert to MB
    
    def count_sessions(self):
        """Count total number of session files"""
        count = 0
        if os.path.exists(self.session_folder):
            for root, dirs, files in os.walk(self.session_folder):
                count += len(files)
        return count
    
    def cleanup_expired_sessions(self, dry_run=False):
        """Remove expired session files"""
        if not os.path.exists(self.session_folder):
            print(f"Session folder '{self.session_folder}' does not exist")
            return
        
        current_time = time.time()
        cutoff_time = current_time - self.max_age.total_seconds()
        removed_files = 0
        total_size_removed = 0
        
        print(f"{'[DRY RUN] ' if dry_run else ''}Cleaning sessions older than {self.max_age}")
        
        for root, dirs, files in os.walk(self.session_folder):
            for file in files:
                file_path = os.path.join(root, file)
                
                try:
                    file_mtime = os.path.getmtime(file_path)
                    
                    if file_mtime < cutoff_time:
                        file_size = os.path.getsize(file_path)
                        
                        if not dry_run:
                            os.remove(file_path)
                        
                        removed_files += 1
                        total_size_removed += file_size
                        
                        if not dry_run:
                            print(f"Removed: {file_path} ({file_size / 1024:.1f} KB)")
                        else:
                            print(f"Would remove: {file_path} ({file_size / 1024:.1f} KB)")
                
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
        
        size_mb = total_size_removed / (1024 * 1024)
        action = "Would remove" if dry_run else "Removed"
        print(f"{action} {removed_files} files ({size_mb:.2f} MB)")
        
        return removed_files, total_size_removed
    
    def cleanup_all_sessions(self, dry_run=False):
        """Remove all session files"""
        if not os.path.exists(self.session_folder):
            print(f"Session folder '{self.session_folder}' does not exist")
            return
        
        removed_files = 0
        total_size_removed = 0
        
        for root, dirs, files in os.walk(self.session_folder):
            for file in files:
                file_path = os.path.join(root, file)
                
                try:
                    file_size = os.path.getsize(file_path)
                    
                    if not dry_run:
                        os.remove(file_path)
                    
                    removed_files += 1
                    total_size_removed += file_size
                
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
        
        size_mb = total_size_removed / (1024 * 1024)
        action = "Would remove" if dry_run else "Removed"
        print(f"{action} {removed_files} files ({size_mb:.2f} MB)")
        
        return removed_files, total_size_removed
    
    def get_session_info(self):
        """Get detailed session folder information"""
        if not os.path.exists(self.session_folder):
            return {
                'folder_exists': False,
                'folder_path': self.session_folder,
                'size_mb': 0,
                'file_count': 0,
                'oldest_session': None,
                'newest_session': None
            }
        
        file_count = 0
        total_size = 0
        oldest_time = float('inf')
        newest_time = 0
        oldest_file = None
        newest_file = None
        
        for root, dirs, files in os.walk(self.session_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    file_size = os.path.getsize(file_path)
                    file_mtime = os.path.getmtime(file_path)
                    
                    total_size += file_size
                    file_count += 1
                    
                    if file_mtime < oldest_time:
                        oldest_time = file_mtime
                        oldest_file = file_path
                    
                    if file_mtime > newest_time:
                        newest_time = file_mtime
                        newest_file = file_path
                
                except Exception:
                    continue
        
        return {
            'folder_exists': True,
            'folder_path': self.session_folder,
            'size_mb': total_size / (1024 * 1024),
            'file_count': file_count,
            'oldest_session': datetime.fromtimestamp(oldest_time) if oldest_file else None,
            'newest_session': datetime.fromtimestamp(newest_time) if newest_file else None
        }

def main():
    """Main function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Flask Session Cleanup Utility')
    parser.add_argument('--folder', default='flask_session', help='Session folder path')
    parser.add_argument('--max-age', type=int, default=24, help='Maximum age in hours')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be removed without actually removing')
    parser.add_argument('--clean-all', action='store_true', help='Remove all session files')
    parser.add_argument('--info', action='store_true', help='Show session folder information')
    
    args = parser.parse_args()
    
    cleanup = SessionCleanup(args.folder, args.max_age)
    
    if args.info:
        info = cleanup.get_session_info()
        print("\n=== Session Folder Information ===")
        print(f"Folder: {info['folder_path']}")
        print(f"Exists: {info['folder_exists']}")
        if info['folder_exists']:
            print(f"Size: {info['size_mb']:.2f} MB")
            print(f"Files: {info['file_count']}")
            if info['oldest_session']:
                print(f"Oldest: {info['oldest_session']}")
            if info['newest_session']:
                print(f"Newest: {info['newest_session']}")
        print()
        return
    
    # Show current status
    info = cleanup.get_session_info()
    print(f"Current session folder size: {info['size_mb']:.2f} MB ({info['file_count']} files)")
    
    if args.clean_all:
        cleanup.cleanup_all_sessions(dry_run=args.dry_run)
    else:
        cleanup.cleanup_expired_sessions(dry_run=args.dry_run)
    
    # Show new status
    if not args.dry_run:
        new_info = cleanup.get_session_info()
        print(f"New session folder size: {new_info['size_mb']:.2f} MB ({new_info['file_count']} files)")

if __name__ == '__main__':
    main()
