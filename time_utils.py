"""
Time Zone Utilities for HaoXai Backup System
Handles consistent time display across different time zones
"""

import os
import json
from datetime import datetime, timezone, timedelta
import pytz

class TimeManager:
    def __init__(self):
        self.timezone = self.get_local_timezone()
        self.utc_offset = self.get_utc_offset()
    
    def get_local_timezone(self):
        """Get the local timezone"""
        try:
            # Try to get timezone from system
            if hasattr(pytz, 'timezone'):
                # Try common timezone names
                local_tz = None
                
                # Windows timezone detection
                if os.name == 'nt':
                    import win32api
                    import win32timezone
                    try:
                        local_tz = win32timezone.TimeZoneInfo.local()
                    except:
                        pass
                
                # Fallback to system detection
                if not local_tz:
                    import tzlocal
                    local_tz = tzlocal.get_localzone()
                
                if local_tz:
                    return local_tz
        except:
            pass
        
        # Fallback: use system offset
        return datetime.now().astimezone().tzinfo
    
    def get_utc_offset(self):
        """Get UTC offset in hours"""
        now = datetime.now()
        offset = now.astimezone().utcoffset()
        return offset.total_seconds() / 3600 if offset else 0
    
    def now_local(self):
        """Get current local time"""
        # Return timezone-aware local time
        return datetime.now().astimezone()
    
    def now_utc(self):
        """Get current UTC time"""
        return datetime.now(timezone.utc)
    
    def to_local_time(self, dt):
        """Convert datetime to local time"""
        if dt.tzinfo is None:
            # Assume it's local time if no timezone info
            return dt
        return dt.astimezone()
    
    def format_local_time(self, dt, format_str="%Y-%m-%d %H:%M:%S"):
        """Format datetime in local time"""
        local_dt = self.to_local_time(dt)
        return local_dt.strftime(format_str)
    
    def format_backup_filename(self, dt=None):
        """Format datetime for backup filename"""
        if dt is None:
            dt = self.now_local()
        return dt.strftime("%Y%m%d_%H%M%S")
    
    def format_display_time(self, dt):
        """Format datetime for display with timezone info"""
        local_dt = self.to_local_time(dt)
        
        # Format with timezone info
        formatted = local_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Add timezone offset
        offset_hours = abs(int(self.utc_offset))
        offset_minutes = int((abs(self.utc_offset) - offset_hours) * 60)
        offset_sign = "+" if self.utc_offset >= 0 else "-"
        timezone_str = f"UTC{offset_sign}{offset_hours:02d}:{offset_minutes:02d}"
        
        return f"{formatted} ({timezone_str})"
    
    def format_relative_time(self, dt):
        """Format datetime as relative time"""
        now = self.now_local()
        local_dt = self.to_local_time(dt)
        
        # Calculate difference
        diff = now - local_dt
        diff_seconds = diff.total_seconds()
        
        if diff_seconds < 60:
            return f"{int(diff_seconds)} seconds ago"
        elif diff_seconds < 3600:
            return f"{int(diff_seconds / 60)} minutes ago"
        elif diff_seconds < 86400:
            return f"{int(diff_seconds / 3600)} hours ago"
        elif diff_seconds < 604800:
            return f"{int(diff_seconds / 86400)} days ago"
        else:
            return self.format_display_time(dt)
    
    def parse_schedule_time(self, time_str):
        """Parse schedule time string to datetime.time"""
        try:
            from datetime import time
            hour, minute = map(int, time_str.split(':'))
            return time(hour=hour, minute=minute)
        except:
            # Default to 2:00 AM
            return time(2, 0)
    
    def is_schedule_time(self, schedule_time_str, current_dt=None):
        """Check if current time matches schedule time"""
        if current_dt is None:
            current_dt = self.now_local()
        
        schedule_time = self.parse_schedule_time(schedule_time_str)
        
        return (current_dt.hour == schedule_time.hour and 
                current_dt.minute >= schedule_time.minute and 
                current_dt.minute < schedule_time.minute + 1)
    
    def get_time_info(self):
        """Get comprehensive time information"""
        now_local = self.now_local()
        now_utc = self.now_utc()
        
        return {
            'local_time': self.format_display_time(now_local),
            'utc_time': self.format_display_time(now_utc),
            'timezone': str(self.timezone),
            'utc_offset': self.utc_offset,
            'timezone_str': f"UTC{'+' if self.utc_offset >= 0 else ''}{self.utc_offset:.1f}"
        }

# Global time manager instance
time_manager = TimeManager()

def get_time_manager():
    """Get the global time manager instance"""
    return time_manager

# Convenience functions
def now_local():
    """Get current local time"""
    return time_manager.now_local()

def now_utc():
    """Get current UTC time"""
    return time_manager.now_utc()

def format_time_display(dt):
    """Format time for display"""
    return time_manager.format_display_time(dt)

def format_time_relative(dt):
    """Format time as relative"""
    return time_manager.format_relative_time(dt)

def format_backup_filename(dt=None):
    """Format time for backup filename"""
    return time_manager.format_backup_filename(dt)

def is_schedule_time(schedule_time_str):
    """Check if it's schedule time"""
    return time_manager.is_schedule_time(schedule_time_str)
