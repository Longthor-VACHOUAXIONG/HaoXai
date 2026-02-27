#!/usr/bin/env python3
"""
Time Zone Diagnostic Tool for HaoXai Backup System
Helps identify and fix time zone issues
"""

import os
import json
from datetime import datetime, timezone, timedelta

def check_time_zone_info():
    """Check current time zone information"""
    print("=" * 60)
    print("TIME ZONE DIAGNOSTIC TOOL")
    print("=" * 60)
    
    # System time information
    now_local = datetime.now()
    now_utc = datetime.now(timezone.utc)
    
    print(f"Local Time: {now_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"UTC Time:   {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Calculate offset
    offset = now_local.astimezone().utcoffset()
    if offset:
        offset_hours = offset.total_seconds() / 3600
        offset_str = f"UTC{'+' if offset_hours >= 0 else ''}{offset_hours:.1f}"
        print(f"Time Zone: {offset_str}")
        print(f"Offset:     {offset}")
    else:
        print("Time Zone: Unknown (no offset information)")
    
    print()
    
    # Check system timezone detection
    try:
        import time
        local_tz = time.tzname
        print(f"System TZ Names: {local_tz}")
    except:
        print("Could not get system timezone names")
    
    print()
    
    # Check if pytz is available
    try:
        import pytz
        print("✅ pytz is available")
        
        # Try to get local timezone
        try:
            import tzlocal
            local_tz = tzlocal.get_localzone()
            print(f"✅ Local timezone detected: {local_tz}")
        except ImportError:
            print("⚠️  tzlocal not available, using pytz timezone detection")
            
            # Try common timezones
            common_tz = ['UTC', 'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific', 
                       'Europe/London', 'Europe/Paris', 'Asia/Shanghai', 'Asia/Tokyo']
            
            for tz_name in common_tz:
                try:
                    tz = pytz.timezone(tz_name)
                    tz_time = datetime.now(tz)
                    print(f"   {tz_name}: {tz_time.strftime('%Y-%m-%d %H:%M:%S')}")
                except:
                    pass
        
    except ImportError:
        print("❌ pytz not available")
        print("   Install with: pip install pytz")
    
    print()
    
    # Check backup configuration
    check_backup_config()
    
    print()
    
    # Test time formatting
    test_time_formatting()

def check_backup_config():
    """Check backup configuration file"""
    print("BACKUP CONFIGURATION CHECK")
    print("-" * 30)
    
    config_files = ['backup_config.json', '../backup_config.json']
    
    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"✅ Found config: {config_file}")
            
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                
                print(f"   Enabled: {config.get('enabled', False)}")
                print(f"   Schedule: {config.get('schedule_time', '02:00')}")
                print(f"   Database: {config.get('db_path', 'Not set')}")
                
                # Check if database exists
                db_path = config.get('db_path')
                if db_path and os.path.exists(db_path):
                    print(f"   ✅ Database exists")
                    
                    # Check backup directory
                    backup_dir = os.path.join(os.path.dirname(db_path), 'backups')
                    if os.path.exists(backup_dir):
                        import glob
                        backup_files = glob.glob(os.path.join(backup_dir, '*.db'))
                        print(f"   ✅ Backup directory exists ({len(backup_files)} backups)")
                        
                        # Show recent backups with times
                        backup_files.sort(key=lambda x: os.path.getctime(x), reverse=True)
                        print("   Recent backups:")
                        for i, backup_file in enumerate(backup_files[:3]):
                            file_time = datetime.fromtimestamp(os.path.getctime(backup_file))
                            print(f"     {i+1}. {os.path.basename(backup_file)}")
                            print(f"        Created: {file_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        print(f"   ⚠️  Backup directory not found: {backup_dir}")
                else:
                    print(f"   ❌ Database not found: {db_path}")
                
            except Exception as e:
                print(f"   ❌ Error reading config: {e}")
            
            break
    else:
        print("❌ No backup configuration found")
        print("   Configure backups through Security Dashboard first")

def test_time_formatting():
    """Test different time formatting methods"""
    print("TIME FORMATTING TEST")
    print("-" * 30)
    
    now = datetime.now()
    
    # Test different formats
    formats = {
        'Filename': now.strftime("%Y%m%d_%H%M%S"),
        'Display': now.strftime("%Y-%m-%d %H:%M:%S"),
        'ISO': now.isoformat(),
        'Local String': str(now),
        'Unix Timestamp': now.timestamp()
    }
    
    for name, value in formats.items():
        print(f"{name:15}: {value}")
    
    print()
    
    # Test timezone conversion
    try:
        import pytz
        
        # Test UTC conversion
        utc_time = now.astimezone(timezone.utc)
        print(f"UTC Time: {utc_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Test different timezones
        test_tz = ['UTC', 'US/Eastern', 'Europe/London', 'Asia/Shanghai']
        for tz_name in test_tz:
            try:
                tz = pytz.timezone(tz_name)
                tz_time = now.astimezone(tz)
                print(f"{tz_name:15}: {tz_time.strftime('%Y-%m-%d %H:%M:%S')}")
            except:
                pass
                
    except ImportError:
        print("pytz not available for timezone testing")

def main():
    """Main diagnostic function"""
    check_time_zone_info()
    
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)
    print("1. Install time zone support: pip install -r time_requirements.txt")
    print("2. Configure backup schedule through Security Dashboard")
    print("3. Use Windows service for reliable operation")
    print("4. Check backup times include timezone information")
    print("5. Verify scheduler runs at correct local time")
    print("=" * 60)

if __name__ == '__main__':
    main()
