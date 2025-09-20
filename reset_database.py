#!/usr/bin/env python3
"""
Simple script to reset the Supabase weather database.
"""

import os
from dotenv import load_dotenv
from database import get_weather_db

def reset_database():
    """Reset the weather_forecasts table."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get database connection
        db = get_weather_db()
        
        # Test connection
        if not db.test_connection():
            print("❌ Database connection failed!")
            return False
        
        print("🔄 Resetting weather_forecasts table...")
        
        # Delete all records
        result = db.supabase.table('weather_forecasts').delete().neq('id', 0).execute()
        
        deleted_count = len(result.data) if result.data else 0
        print(f"✅ Database reset successful!")
        print(f"📊 Deleted {deleted_count} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Error resetting database: {e}")
        return False

if __name__ == "__main__":
    print("🗑️  Weather Database Reset Tool")
    print("=" * 40)
    
    # Confirm before reset
    confirm = input("Are you sure you want to delete ALL weather data? (yes/no): ")
    
    if confirm.lower() in ['yes', 'y']:
        success = reset_database()
        if success:
            print("\n🎉 Database reset completed!")
        else:
            print("\n💥 Database reset failed!")
    else:
        print("❌ Reset cancelled.")
