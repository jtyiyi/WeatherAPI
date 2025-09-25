#!/usr/bin/env python3
"""
Airflow DAG for hourly weather data uploads to Supabase database.
This DAG runs every hour to collect and upload weather data from all APIs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import sys

# Add the project directory to Python path
project_dir = "/Users/justin/Desktop/sgweather"
sys.path.append(project_dir)

# Default arguments for the DAG
default_args = {
    'owner': 'weather-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  # Don't run for past dates
}

# Create the DAG
dag = DAG(
    'weather_data_upload',
    default_args=default_args,
    description='Hourly weather data collection and upload to Supabase',
    schedule='@hourly',  # Run every hour (new syntax)
    max_active_runs=1,  # Only one instance at a time
    tags=['weather', 'data-collection', 'supabase'],
)

def upload_weather_data():
    """
    Upload weather data from all APIs to the database.
    This function is called by the Airflow task.
    """
    print("🌤️  Starting weather data upload to database...")
    
    # Import here to avoid import issues during DAG parsing
    from database import get_weather_db
    from weather_api import WeatherAPI
    from singapore_nea import SingaporeNEA
    from visual_crossing import VisualCrossing
    
    # Initialize database
    db = get_weather_db()
    
    # Initialize APIs
    weather_api = WeatherAPI()
    sg_nea = SingaporeNEA()
    visual_crossing = VisualCrossing()
    
    success_count = 0
    total_apis = 3
    
    try:
        # 1. Upload WeatherAPI data
        print("\n📡 Fetching WeatherAPI data...")
        weather_api_data = weather_api.get_forecasts_for_all_singapore_areas()
        if weather_api_data and 'forecasts' in weather_api_data:
            print(f"✅ Found {len(weather_api_data['forecasts'])} WeatherAPI areas")
            if db.insert_weather_api_data(weather_api_data):
                print("✅ WeatherAPI data uploaded successfully!")
                success_count += 1
            else:
                print("❌ Failed to upload WeatherAPI data")
        else:
            print("❌ No WeatherAPI data found")
        
        # 2. Upload Singapore NEA data
        print("\n📡 Fetching Singapore NEA data...")
        sg_nea_data = sg_nea.get_forecasts_for_all_singapore_areas()
        if sg_nea_data and 'forecasts' in sg_nea_data:
            print(f"✅ Found {len(sg_nea_data['forecasts'])} Singapore NEA areas")
            if db.insert_singapore_nea_data(sg_nea_data):
                print("✅ Singapore NEA data uploaded successfully!")
                success_count += 1
            else:
                print("❌ Failed to upload Singapore NEA data")
        else:
            print("❌ No Singapore NEA data found")
        
        # 3. Upload Visual Crossing data
        print("\n📡 Fetching Visual Crossing data...")
        visual_crossing_data = visual_crossing.get_forecasts_for_all_singapore_areas()
        if visual_crossing_data and 'forecasts' in visual_crossing_data:
            print(f"✅ Found {len(visual_crossing_data['forecasts'])} Visual Crossing areas")
            if db.insert_visual_crossing_data(visual_crossing_data):
                print("✅ Visual Crossing data uploaded successfully!")
                success_count += 1
            else:
                print("❌ Failed to upload Visual Crossing data")
        else:
            print("❌ No Visual Crossing data found")
        
        # Summary
        print(f"\n📊 Upload Summary:")
        print(f"   ✅ Successful: {success_count}/{total_apis} APIs")
        print(f"   ❌ Failed: {total_apis - success_count}/{total_apis} APIs")
        
        if success_count > 0:
            print("\n🎉 Weather data upload completed!")
            # Get database stats
            stats = db.get_database_stats()
            if stats:
                print(f"📈 Database now contains {stats.get('total_records', 0)} records")
        else:
            print("\n❌ No data was uploaded successfully")
            raise Exception("No data was uploaded successfully")
            
    except Exception as e:
        print(f"❌ Error during upload: {e}")
        raise e
    
    return success_count

# Task 1: Upload weather data
upload_task = PythonOperator(
    task_id='upload_weather_data',
    python_callable=upload_weather_data,
    dag=dag,
)

# Task 2: Log completion
log_completion = BashOperator(
    task_id='log_completion',
    bash_command='echo "Weather data upload completed at $(date)"',
    dag=dag,
)

# Set task dependencies
upload_task >> log_completion
