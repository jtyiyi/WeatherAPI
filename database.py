#!/usr/bin/env python3
"""
Optimized database class for separate API tables with their own JSON schemas.
Each API gets its own table with optimized structure for their specific data format.
"""

import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

load_dotenv()

class OptimizedWeatherDatabase:
    """Optimized database class for separate API tables with individual JSON schemas."""
    
    def __init__(self):
        """Initialize Supabase connection."""
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("Supabase credentials not found in environment variables")
        
        try:
            self.supabase: Client = create_client(self.url, self.key)
        except Exception as e:
            print(f"Failed to create Supabase client: {e}")
            try:
                self.supabase: Client = create_client(
                    supabase_url=self.url,
                    supabase_key=self.key
                )
            except Exception as e2:
                print(f"Alternative initialization also failed: {e2}")
                raise e2
        
        self.sgt = ZoneInfo('Asia/Singapore')
        
        # API to table mapping for separate tables
        self.api_tables = {
            'WeatherAPI': 'weather_api_forecasts',
            'Singapore NEA': 'singapore_nea_forecasts',
            'Visual Crossing': 'visual_crossing_forecasts'
        }
    
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            # Test connection with the first table
            result = self.supabase.table('weather_api_forecasts').select('*').limit(1).execute()
            print("✅ Database connection successful!")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def get_table_name(self, api_source: str) -> str:
        """Get the table name for a specific API."""
        return self.api_tables.get(api_source, 'weather_api_forecasts')
    
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
            
            # If the forecast time hour is earlier than current hour, assume it's for tomorrow
            # Only add a day if the forecast hour is significantly earlier (more than 2 hours ago)
            current_hour = current_time.hour
            forecast_hour = forecast_time.hour
            
            # If forecast hour is more than 2 hours behind current hour, it's likely for tomorrow
            if forecast_hour < (current_hour - 2):
                forecast_time += timedelta(days=1)
            
            return forecast_time
            
        except Exception as e:
            print(f"⚠️ Failed to parse time '{time_str}': {e}")
            return datetime.now(self.sgt)
    
    def insert_weather_api_data(self, all_areas_data: Dict[str, Any]) -> bool:
        """
        Insert WeatherAPI data with optimized structure for detailed weather parameters.
        
        Args:
            all_areas_data: Data from weather_api.get_forecasts_for_all_singapore_areas()
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not all_areas_data or 'forecasts' not in all_areas_data:
                print("❌ No WeatherAPI data to insert")
                return False
            
            table_name = "weather_api_forecasts"
            weather_records = []
            
            print(f"📊 Processing {len(all_areas_data['forecasts'])} WeatherAPI areas...")
            
            for forecast in all_areas_data['forecasts']:
                area_name = forecast['area_name']
                lat = forecast['latitude']
                lon = forecast['longitude']
                condition = forecast['condition']
                time_str = forecast['forecast_time']
                
                # Parse forecast time properly
                forecast_timestamp = self.parse_forecast_time(time_str, "WeatherAPI")
                
                # Extract date and time from the parsed timestamp
                # forecast_date always uses the date of forecast_start, even if forecast_end crosses midnight
                forecast_date = forecast_timestamp.strftime('%Y-%m-%d')
                forecast_start = forecast_timestamp.strftime('%H:%M:%S')
                forecast_end = (forecast_timestamp + timedelta(hours=1)).strftime('%H:%M:%S')
                
                # Get current Singapore time for created_at
                current_sgt = datetime.now(self.sgt)
                
                # Extract meta data from the full forecast
                meta_data = {}
                if 'full_forecast' in forecast:
                    full_forecast = forecast['full_forecast']
                    
                    # Store the complete API response in meta
                    meta_data = {
                        'api_response': full_forecast,
                        'api_type': 'WeatherAPI',
                        'extracted_parameters': {}
                    }
                    
                    # Extract detailed weather parameters for easy access
                    if 'forecast' in full_forecast and full_forecast['forecast']:
                        first_forecast = full_forecast['forecast'][0]
                        meta_data['extracted_parameters'] = {
                            'temperature': first_forecast.get('temperature'),
                            'humidity': first_forecast.get('humidity'),
                            'wind_speed': first_forecast.get('wind_speed'),
                            'wind_direction': first_forecast.get('wind_direction'),
                            'pressure': first_forecast.get('pressure'),
                            'precipitation': first_forecast.get('precipitation'),
                            'cloud_cover': first_forecast.get('cloud_cover'),
                            'feels_like': first_forecast.get('feels_like'),
                            'uv_index': first_forecast.get('uv_index'),
                            'visibility': first_forecast.get('visibility')
                        }
                    
                    # Add location information
                    if 'location' in full_forecast:
                        meta_data['location_info'] = full_forecast['location']
                    
                    # Add current time
                    if 'current_time' in full_forecast:
                        meta_data['current_time'] = full_forecast['current_time']
                
                record = {
                    'area_name': area_name,
                    'latitude': lat,
                    'longitude': lon,
                    'forecast_date': forecast_date,
                    'forecast_start': forecast_start,
                    'forecast_end': forecast_end,
                    'weather_condition': condition,
                    'meta': meta_data,  # Store all extra parameters as JSON
                    'created_at': current_sgt.isoformat()
                }
                
                weather_records.append(record)
            
            # Insert all records into the WeatherAPI table
            if weather_records:
                result = self.supabase.table(table_name).insert(weather_records).execute()
                print(f"✅ Inserted {len(weather_records)} WeatherAPI area records into {table_name}")
                return True
            else:
                print("⚠️ No WeatherAPI records to insert")
                return False
                
        except Exception as e:
            print(f"❌ Failed to insert WeatherAPI data: {e}")
            return False
    
    def insert_singapore_nea_data(self, all_areas_data: Dict[str, Any]) -> bool:
        """
        Insert Singapore NEA data with optimized structure for government weather data.
        
        Args:
            all_areas_data: Data from singapore_nea.get_forecasts_for_all_singapore_areas()
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not all_areas_data or 'forecasts' not in all_areas_data:
                print("❌ No Singapore NEA data to insert")
                return False
            
            table_name = "singapore_nea_forecasts"
            weather_records = []
            
            print(f"📊 Processing {len(all_areas_data['forecasts'])} Singapore NEA areas...")
            
            for forecast in all_areas_data['forecasts']:
                area_name = forecast['area_name']
                lat = forecast['latitude']
                lon = forecast['longitude']
                condition = forecast['condition']
                time_str = forecast['forecast_time']
                
                # Parse forecast time properly
                forecast_timestamp = self.parse_forecast_time(time_str, "Singapore NEA")
                
                # Extract date and time from the parsed timestamp
                # forecast_date always uses the date of forecast_start, even if forecast_end crosses midnight
                forecast_date = forecast_timestamp.strftime('%Y-%m-%d')
                forecast_start = forecast_timestamp.strftime('%H:%M:%S')
                forecast_end = (forecast_timestamp + timedelta(hours=2)).strftime('%H:%M:%S')
                
                # Get current Singapore time for created_at
                current_sgt = datetime.now(self.sgt)
                
                # Extract meta data from the full forecast
                meta_data = {}
                if 'full_forecast' in forecast:
                    full_forecast = forecast['full_forecast']
                    
                    # Store the complete API response in meta
                    meta_data = {
                        'api_response': full_forecast,
                        'api_type': 'Singapore NEA',
                        'area_metadata': {},
                        'time_period': {}
                    }
                    
                    # Extract area metadata (Singapore NEA specific)
                    if 'area_metadata' in full_forecast:
                        meta_data['area_metadata'] = full_forecast['area_metadata']
                    
                    # Extract time period details
                    meta_data['time_period'] = {
                        'start': forecast_start,
                        'end': forecast_end,
                        'duration_hours': 2
                    }
                    
                    # Add current time
                    if 'current_time' in full_forecast:
                        meta_data['current_time'] = full_forecast['current_time']
                
                record = {
                    'area_name': area_name,
                    'latitude': lat,
                    'longitude': lon,
                    'forecast_date': forecast_date,
                    'forecast_start': forecast_start,
                    'forecast_end': forecast_end,
                    'weather_condition': condition,
                    'meta': meta_data,  # Store all extra parameters as JSON
                    'created_at': current_sgt.isoformat()
                }
                
                weather_records.append(record)
            
            # Insert all records into the Singapore NEA table
            if weather_records:
                result = self.supabase.table(table_name).insert(weather_records).execute()
                print(f"✅ Inserted {len(weather_records)} Singapore NEA area records into {table_name}")
                return True
            else:
                print("⚠️ No Singapore NEA records to insert")
                return False
                
        except Exception as e:
            print(f"❌ Failed to insert Singapore NEA data: {e}")
            return False
    
    def insert_visual_crossing_data(self, all_areas_data: Dict[str, Any]) -> bool:
        """
        Insert Visual Crossing data with optimized structure for comprehensive weather data.
        
        Args:
            all_areas_data: Data from visual_crossing.get_forecasts_for_all_singapore_areas()
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not all_areas_data or 'forecasts' not in all_areas_data:
                print("❌ No Visual Crossing data to insert")
                return False
            
            table_name = "visual_crossing_forecasts"
            weather_records = []
            
            print(f"📊 Processing {len(all_areas_data['forecasts'])} Visual Crossing areas...")
            
            for forecast in all_areas_data['forecasts']:
                area_name = forecast['area_name']
                lat = forecast['latitude']
                lon = forecast['longitude']
                condition = forecast['condition']
                time_str = forecast['forecast_time']
                
                # Parse forecast time properly
                forecast_timestamp = self.parse_forecast_time(time_str, "Visual Crossing")
                
                # Extract date and time from the parsed timestamp
                # forecast_date always uses the date of forecast_start, even if forecast_end crosses midnight
                forecast_date = forecast_timestamp.strftime('%Y-%m-%d')
                forecast_start = forecast_timestamp.strftime('%H:%M:%S')
                forecast_end = (forecast_timestamp + timedelta(hours=1)).strftime('%H:%M:%S')
                
                # Get current Singapore time for created_at
                current_sgt = datetime.now(self.sgt)
                
                # Extract meta data from the full forecast
                meta_data = {}
                if 'full_forecast' in forecast:
                    full_forecast = forecast['full_forecast']
                    
                    # Store the complete API response in meta
                    meta_data = {
                        'api_response': full_forecast,
                        'api_type': 'Visual Crossing',
                        'extracted_parameters': {}
                    }
                    
                    # Extract detailed weather parameters for easy access
                    if 'forecast' in full_forecast and full_forecast['forecast']:
                        first_forecast = full_forecast['forecast'][0]
                        meta_data['extracted_parameters'] = {
                            'temperature': first_forecast.get('temperature'),
                            'humidity': first_forecast.get('humidity'),
                            'wind_speed': first_forecast.get('wind_speed'),
                            'wind_direction': first_forecast.get('wind_direction'),
                            'pressure': first_forecast.get('pressure'),
                            'precipitation': first_forecast.get('precipitation'),
                            'cloud_cover': first_forecast.get('cloud_cover'),
                            'feels_like': first_forecast.get('feels_like'),
                            'uv_index': first_forecast.get('uv_index'),
                            'visibility': first_forecast.get('visibility')
                        }
                    
                    # Add location information
                    if 'location' in full_forecast:
                        meta_data['location_info'] = full_forecast['location']
                    
                    # Add current time
                    if 'current_time' in full_forecast:
                        meta_data['current_time'] = full_forecast['current_time']
                
                record = {
                    'area_name': area_name,
                    'latitude': lat,
                    'longitude': lon,
                    'forecast_date': forecast_date,
                    'forecast_start': forecast_start,
                    'forecast_end': forecast_end,
                    'weather_condition': condition,
                    'meta': meta_data,  # Store all extra parameters as JSON
                    'created_at': current_sgt.isoformat()
                }
                
                weather_records.append(record)
            
            # Insert all records into the Visual Crossing table
            if weather_records:
                result = self.supabase.table(table_name).insert(weather_records).execute()
                print(f"✅ Inserted {len(weather_records)} Visual Crossing area records into {table_name}")
                return True
            else:
                print("⚠️ No Visual Crossing records to insert")
                return False
                
        except Exception as e:
            print(f"❌ Failed to insert Visual Crossing data: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics for all API tables."""
        try:
            total_records = 0
            api_stats = {}
            
            for api_source, table_name in self.api_tables.items():
                result = self.supabase.table(table_name).select('id', count='exact').execute()
                count = result.count if result.count else 0
                api_stats[api_source] = count
                total_records += count
            
            return {
                'total_records': total_records,
                'api_breakdown': api_stats,
                'last_updated': datetime.now(self.sgt).isoformat()
            }
        except Exception as e:
            print(f"❌ Failed to get database stats: {e}")
            return {}

# Convenience function for easy import
def get_optimized_weather_db() -> OptimizedWeatherDatabase:
    """Get an OptimizedWeatherDatabase instance."""
    return OptimizedWeatherDatabase()

def get_weather_db() -> OptimizedWeatherDatabase:
    """Get a WeatherDatabase instance (alias for compatibility)."""
    return OptimizedWeatherDatabase()
