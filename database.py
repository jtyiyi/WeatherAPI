import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

load_dotenv()

class WeatherDatabase:
    def __init__(self):
        """Initialize Supabase connection."""
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Supabase credentials not found in environment variables")
        
        # Try different initialization methods
        try:
            self.supabase: Client = create_client(self.url, self.key)
        except Exception as e:
            print(f"Failed to create Supabase client: {e}")
            # Try alternative initialization
            try:
                self.supabase: Client = create_client(
                    supabase_url=self.url,
                    supabase_key=self.key
                )
            except Exception as e2:
                print(f"Alternative initialization also failed: {e2}")
                raise e2
        
        self.sgt = ZoneInfo('Asia/Singapore')
    
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            result = self.supabase.table('weather_forecasts').select('*').limit(1).execute()
            print("✅ Database connection successful!")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def parse_forecast_time(self, time_str: str, api_source: str) -> datetime:
        """Parse forecast time string into datetime object."""
        try:
            current_time = datetime.now(self.sgt)
            
            if api_source == "Singapore NEA":
                # Handle time ranges like "15:00-17:00"
                if '-' in time_str:
                    start_time = time_str.split('-')[0]
                    hour, minute = map(int, start_time.split(':'))
                    forecast_time = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
                else:
                    # Single time like "15:00"
                    hour, minute = map(int, time_str.split(':'))
                    forecast_time = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
            else:
                # Handle single times like "15:00"
                if ':' in time_str:
                    hour, minute = map(int, time_str.split(':'))
                    forecast_time = current_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
                else:
                    # Just hour like "15"
                    hour = int(time_str)
                    forecast_time = current_time.replace(hour=hour, minute=0, second=0, microsecond=0)
            
            # If the forecast time is in the past, assume it's for tomorrow
            if forecast_time < current_time:
                forecast_time += timedelta(days=1)
            
            return forecast_time
            
        except Exception as e:
            print(f"⚠️ Failed to parse time '{time_str}': {e}")
            return datetime.now(self.sgt)
    
    def insert_weather_data(self, api_response: Dict[str, Any]) -> bool:
        """Insert weather data from API response into database."""
        try:
            # Extract data from API response
            location_name = api_response['location']
            lat, lon = map(float, api_response['coordinates'].split(', '))
            hours_requested = api_response['forecast_hours']
            
            # Prepare data for insertion
            weather_records = []
            
            for api_data in api_response['data']:
                if 'forecast' in api_data:
                    api_source = api_data['api']
                    
                    for forecast in api_data['forecast']:
                        time_str = forecast['time']
                        condition = forecast['condition']
                        
                        # Parse forecast time properly
                        forecast_time = self.parse_forecast_time(time_str, api_source)
                        
                        record = {
                            'location_name': location_name,
                            'latitude': lat,
                            'longitude': lon,
                            'api_source': api_source,
                            'forecast_time': forecast_time.isoformat(),
                            'time_period': time_str,
                            'weather_condition': condition,
                            'forecast_hours_requested': hours_requested
                        }
                        weather_records.append(record)
            
            # Insert all records
            if weather_records:
                result = self.supabase.table('weather_forecasts').insert(weather_records).execute()
                print(f"✅ Inserted {len(weather_records)} weather records into database")
                return True
            else:
                print("⚠️ No weather records to insert")
                return False
                
        except Exception as e:
            print(f"❌ Failed to insert weather data: {e}")
            return False
    
    def get_latest_weather(self, lat: float, lon: float, limit: int = 10) -> List[Dict]:
        """Get latest weather data for a location."""
        try:
            result = self.supabase.table('weather_forecasts')\
                .select('*')\
                .eq('latitude', lat)\
                .eq('longitude', lon)\
                .order('created_at', desc=True)\
                .limit(limit)\
                .execute()
            
            return result.data
        except Exception as e:
            print(f"❌ Failed to get weather data: {e}")
            return []
    
    def get_weather_by_api(self, lat: float, lon: float, api_source: str) -> List[Dict]:
        """Get weather data from specific API."""
        try:
            result = self.supabase.table('weather_forecasts')\
                .select('*')\
                .eq('latitude', lat)\
                .eq('longitude', lon)\
                .eq('api_source', api_source)\
                .order('forecast_time')\
                .execute()
            
            return result.data
        except Exception as e:
            print(f"❌ Failed to get weather data: {e}")
            return []
    
    def get_weather_by_time_range(self, lat: float, lon: float, hours_ahead: int = 6) -> List[Dict]:
        """Get weather data for a specific time range."""
        try:
            current_time = datetime.now(self.sgt)
            future_time = current_time + timedelta(hours=hours_ahead)
            
            result = self.supabase.table('weather_forecasts')\
                .select('*')\
                .eq('latitude', lat)\
                .eq('longitude', lon)\
                .gte('forecast_time', current_time.isoformat())\
                .lte('forecast_time', future_time.isoformat())\
                .order('forecast_time')\
                .execute()
            
            return result.data
        except Exception as e:
            print(f"❌ Failed to get weather data: {e}")
            return []
    
    def compare_apis(self, lat: float, lon: float) -> Dict[str, List[Dict]]:
        """Compare weather conditions across different APIs."""
        try:
            result = self.supabase.table('weather_forecasts')\
                .select('api_source, weather_condition, forecast_time, time_period')\
                .eq('latitude', lat)\
                .eq('longitude', lon)\
                .order('forecast_time')\
                .execute()
            
            # Group by API source
            api_data = {}
            for record in result.data:
                api_source = record['api_source']
                if api_source not in api_data:
                    api_data[api_source] = []
                api_data[api_source].append(record)
            
            return api_data
        except Exception as e:
            print(f"❌ Failed to compare APIs: {e}")
            return {}
    
    def get_weather_summary(self, lat: float, lon: float) -> Dict[str, Any]:
        """Get a summary of weather data for a location."""
        try:
            # Get latest data from each API
            apis = ['WeatherAPI', 'Singapore NEA', 'Visual Crossing']
            summary = {
                'location': {'latitude': lat, 'longitude': lon},
                'apis': {},
                'last_updated': None
            }
            
            for api in apis:
                api_data = self.get_weather_by_api(lat, lon, api)
                if api_data:
                    latest_record = api_data[-1]  # Most recent
                    summary['apis'][api] = {
                        'latest_condition': latest_record['weather_condition'],
                        'latest_time': latest_record['time_period'],
                        'total_records': len(api_data),
                        'last_updated': latest_record['created_at']
                    }
                    
                    # Update overall last updated time
                    if not summary['last_updated'] or latest_record['created_at'] > summary['last_updated']:
                        summary['last_updated'] = latest_record['created_at']
            
            return summary
        except Exception as e:
            print(f"❌ Failed to get weather summary: {e}")
            return {}
    
    def delete_old_records(self, days_old: int = 7) -> int:
        """Delete weather records older than specified days."""
        try:
            cutoff_date = datetime.now(self.sgt) - timedelta(days=days_old)
            
            result = self.supabase.table('weather_forecasts')\
                .delete()\
                .lt('created_at', cutoff_date.isoformat())\
                .execute()
            
            deleted_count = len(result.data) if result.data else 0
            print(f"🗑️ Deleted {deleted_count} old weather records")
            return deleted_count
        except Exception as e:
            print(f"❌ Failed to delete old records: {e}")
            return 0
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            # Get total records
            total_result = self.supabase.table('weather_forecasts').select('id', count='exact').execute()
            total_records = total_result.count if total_result.count else 0
            
            # Get records by API
            api_stats = {}
            apis = ['WeatherAPI', 'Singapore NEA', 'Visual Crossing']
            
            for api in apis:
                api_result = self.supabase.table('weather_forecasts')\
                    .select('id', count='exact')\
                    .eq('api_source', api)\
                    .execute()
                api_stats[api] = api_result.count if api_result.count else 0
            
            # Get unique locations
            locations_result = self.supabase.table('weather_forecasts')\
                .select('latitude, longitude')\
                .execute()
            
            unique_locations = set()
            for record in locations_result.data:
                unique_locations.add((record['latitude'], record['longitude']))
            
            return {
                'total_records': total_records,
                'api_breakdown': api_stats,
                'unique_locations': len(unique_locations),
                'last_updated': datetime.now(self.sgt).isoformat()
            }
        except Exception as e:
            print(f"❌ Failed to get database stats: {e}")
            return {}

# Convenience function for easy import
def get_weather_db() -> WeatherDatabase:
    """Get a WeatherDatabase instance."""
    return WeatherDatabase()