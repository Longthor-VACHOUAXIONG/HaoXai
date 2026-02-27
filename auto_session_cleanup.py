#!/usr/bin/env python3
"""
Automatic Session Cleanup Scheduler
Runs periodically to clean up expired Flask sessions
"""

import os
import time
import threading
from datetime import timedelta
from session_cleanup import SessionCleanup

class AutoSessionCleanup:
    def __init__(self, session_folder='flask_session', cleanup_interval_hours=6, max_age_hours=24):
        self.session_folder = session_folder
        self.cleanup_interval = timedelta(hours=cleanup_interval_hours)
        self.max_age = timedelta(hours=max_age_hours)
        self.cleanup = SessionCleanup(session_folder, max_age_hours)
        self.running = False
        self.thread = None
        
    def start(self):
        """Start the automatic cleanup scheduler"""
        if self.running:
            print("Auto cleanup is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.thread.start()
        print(f"Auto session cleanup started (runs every {self.cleanup_interval})")
        
    def stop(self):
        """Stop the automatic cleanup scheduler"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print("Auto session cleanup stopped")
        
    def _cleanup_loop(self):
        """Main cleanup loop"""
        while self.running:
            try:
                # Get current status
                info = self.cleanup.get_session_info()
                
                # Only cleanup if there are files and they're taking up significant space
                if info['file_count'] > 0 and info['size_mb'] > 10:  # More than 10MB
                    print(f"\n=== Auto Session Cleanup ===")
                    print(f"Current size: {info['size_mb']:.2f} MB ({info['file_count']} files)")
                    
                    # Clean expired sessions
                    removed_files, size_removed = self.cleanup.cleanup_expired_sessions()
                    
                    if removed_files > 0:
                        print(f"Cleaned up {removed_files} files ({size_removed / (1024*1024):.2f} MB)")
                    else:
                        print("No expired sessions to clean")
                else:
                    print(f"Session folder is small ({info['size_mb']:.2f} MB), skipping cleanup")
                
                # Wait for next cleanup
                time.sleep(self.cleanup_interval.total_seconds())
                
            except Exception as e:
                print(f"Error in auto cleanup: {e}")
                # Wait a shorter time before retrying
                time.sleep(300)  # 5 minutes
    
    def cleanup_now(self):
        """Force an immediate cleanup"""
        print("Running immediate session cleanup...")
        removed_files, size_removed = self.cleanup.cleanup_expired_sessions()
        if removed_files > 0:
            print(f"Cleaned {removed_files} files ({size_removed / (1024*1024):.2f} MB)")
        else:
            print("No sessions to clean")
        return removed_files, size_removed

# Global instance for the application
auto_cleanup = None

def start_auto_cleanup(session_folder='flask_session', cleanup_interval_hours=6, max_age_hours=24):
    """Start automatic session cleanup"""
    global auto_cleanup
    auto_cleanup = AutoSessionCleanup(session_folder, cleanup_interval_hours, max_age_hours)
    auto_cleanup.start()
    return auto_cleanup

def stop_auto_cleanup():
    """Stop automatic session cleanup"""
    global auto_cleanup
    if auto_cleanup:
        auto_cleanup.stop()
        auto_cleanup = None

def cleanup_sessions_now():
    """Force immediate cleanup"""
    global auto_cleanup
    if auto_cleanup:
        return auto_cleanup.cleanup_now()
    else:
        # Fallback to direct cleanup
        cleanup = SessionCleanup()
        return cleanup.cleanup_expired_sessions()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Automatic Session Cleanup')
    parser.add_argument('--start', action='store_true', help='Start automatic cleanup')
    parser.add_argument('--stop', action='store_true', help='Stop automatic cleanup')
    parser.add_argument('--cleanup-now', action='store_true', help='Run immediate cleanup')
    parser.add_argument('--interval', type=int, default=6, help='Cleanup interval in hours')
    parser.add_argument('--max-age', type=int, default=24, help='Maximum session age in hours')
    
    args = parser.parse_args()
    
    if args.start:
        start_auto_cleanup(cleanup_interval_hours=args.interval, max_age_hours=args.max_age)
        print("Press Ctrl+C to stop...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            stop_auto_cleanup()
    
    elif args.stop:
        stop_auto_cleanup()
    
    elif args.cleanup_now:
        cleanup_sessions_now()
    
    else:
        print("Use --start to begin, --stop to end, or --cleanup-now for immediate cleanup")
