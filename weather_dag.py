from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'weather_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG
dag = DAG(
    'weather_api_upload',
    default_args=default_args,
    description='Run weather API to upload data to Supabase',
    schedule=timedelta(seconds=1),  # Run every 1 second
    start_date=datetime.now(),
    end_date=datetime.now() + timedelta(seconds=5),  # Stop after 5 seconds from now
    catchup=False,
    tags=['weather', 'api'],
)

def run_weather_api():
    """Run the weather API to fetch and upload data to Supabase."""
    try:
        print("🌤️ Running weather API...")
        
        # Make request to your weather API
        response = requests.get("http://localhost:8001/weather", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        db_status = data.get('database_status')
        
        # Check database status
        if db_status == 'saved':
            print(f"✅ Weather data uploaded to Supabase successfully")
            print(f"📍 Location: {data.get('location')}")
            print(f"📊 Sources: {data.get('sources')}")
        elif db_status == 'disabled':
            print(f"⚠️ Database upload is DISABLED (ENABLE_DATABASE=false)")
            print(f"📍 Location: {data.get('location')}")
            print(f"📊 Sources: {data.get('sources')}")
        elif db_status == 'connection_failed':
            print(f"❌ Database connection failed")
        else:
            print(f"❌ Database error: {db_status}")
            
        return data
        
    except Exception as e:
        print(f"❌ Error running weather API: {e}")
        raise

# Define the task
run_weather_task = PythonOperator(
    task_id='run_weather_api',
    python_callable=run_weather_api,
    dag=dag,
)
