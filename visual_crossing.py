"""
Visual Crossing API Integration Module
Handles all interactions with the Visual Crossing weather service.
"""

import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# Load environment variables
load_dotenv()

# API Configuration
VISUAL_CROSSING_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

class VisualCrossing:
    """Visual Crossing weather service integration."""
    
    def __init__(self):
        """Initialize Visual Crossing with API key."""
        self.api_key = os.getenv('VISUAL_CROSSING_API_KEY')
        self.sgt = ZoneInfo('Asia/Singapore')
    
    def get_coordinates_from_area_name(self, area_name: str) -> Optional[tuple]:
        """
        Get coordinates for a Singapore area name using Singapore NEA area metadata.
        
        Args:
            area_name: Name of the Singapore area (e.g., "Serangoon", "Orchard")
            
        Returns:
            Tuple of (latitude, longitude) or None if area not found
        """
        try:
            import requests
            
            # Get Singapore NEA area metadata
            sg_api_url = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"
            response = requests.get(sg_api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'area_metadata' in data:
                # Search for the area by name (case-insensitive)
                area_name_lower = area_name.lower().strip()
                
                for area in data['area_metadata']:
                    if area['name'].lower() == area_name_lower:
                        coords = area['label_location']
                        return (coords['latitude'], coords['longitude'])
                
                # If exact match not found, try partial match
                for area in data['area_metadata']:
                    if area_name_lower in area['name'].lower():
                        coords = area['label_location']
                        return (coords['latitude'], coords['longitude'])
                
                print(f"Visual Crossing: Area '{area_name}' not found in Singapore NEA areas")
                return None
            else:
                print("Visual Crossing: No area metadata available from Singapore NEA")
                return None
                
        except Exception as e:
            print(f"Visual Crossing: Error getting coordinates for area '{area_name}': {e}")
            return None

    def get_forecasts_for_all_singapore_areas(self, hours: int = 2) -> Optional[Dict[str, Any]]:
        """
        Get weather forecasts for all Singapore NEA areas using their coordinates.
        
        Args:
            hours: Number of hours to forecast (default: 2)
            
        Returns:
            Dictionary containing forecasts for all areas or None if error
        """
        try:
            import requests
            
            # Get Singapore NEA area metadata
            sg_api_url = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"
            response = requests.get(sg_api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'area_metadata' not in data:
                print("Visual Crossing: No area metadata available from Singapore NEA")
                return None
            
            all_forecasts = []
            condition_summary = {}
            
            print(f"Visual Crossing: Processing {len(data['area_metadata'])} Singapore NEA areas...")
            
            for area in data['area_metadata']:
                area_name = area['name']
                coords = area['label_location']
                lat = coords['latitude']
                lon = coords['longitude']
                
                # Get Visual Crossing forecast for this area
                forecast = self.fetch_forecast_data(lat, lon, hours)
                if forecast and 'forecast' in forecast:
                    # Get the first forecast condition
                    first_forecast = forecast['forecast'][0]
                    condition = first_forecast.get('condition', 'Unknown')
                    time_str = first_forecast.get('time', 'N/A')
                    
                    # Group by condition for summary
                    if condition not in condition_summary:
                        condition_summary[condition] = []
                    condition_summary[condition].append(area_name)
                    
                    all_forecasts.append({
                        'area_name': area_name,
                        'latitude': lat,
                        'longitude': lon,
                        'forecast_time': time_str,
                        'condition': condition,
                        'full_forecast': forecast
                    })
            
            return {
                'api': 'Visual Crossing',
                'total_areas': len(all_forecasts),
                'forecasts': all_forecasts,
                'condition_summary': condition_summary,
                'current_time': datetime.now(self.sgt).strftime('%d/%m/%Y %H:%M +08:00')
            }
            
        except Exception as e:
            print(f"Visual Crossing: Error getting forecasts for all Singapore areas: {e}")
            return None

    def fetch_forecast_data(self, lat: float, lon: float, hours: int = 6) -> Optional[Dict[str, Any]]:
        """
        Fetch forecast data from Visual Crossing.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            hours: Number of hours to forecast (default: 6)
            
        Returns:
            Dictionary containing forecast data or None if error
        """
        try:
            if not self.api_key:
                print("Visual Crossing API key not found in environment variables")
                return None

            current_dt = datetime.now(self.sgt)
            start_hour = current_dt.hour

            # Make API request
            params = {
                'location': f'{lat},{lon}',
                'key': self.api_key,
                'unitGroup': 'metric',
                'include': 'hours',
                'elements': 'datetime,conditions,temp,humidity,windspeed,winddir,pressure,precip,cloudcover,feelslike,uvindex,visibility'
            }

            response = requests.get(VISUAL_CROSSING_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'days' in data and len(data['days']) > 0:
                # Get current time in Singapore timezone
                current_time = current_dt.strftime("%d/%m/%Y %H:%M %z")
                formatted_time = current_time
                if len(formatted_time) >= 5:
                    formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
                
                
                # Flatten all hours from all days and add date information
                all_hours = []
                for day in data['days']:
                    day_date = day.get('datetime', '')
                    hours_data = day.get('hours', [])
                    for hour in hours_data:
                        hour['date'] = day_date  # Add date to each hour
                        all_hours.append(hour)

                # Process forecast data
                forecast_data = []
                count = 0
                
                for hour_data in all_hours:
                    if count >= hours:
                        break
                        
                    # Parse the time and date
                    date_str = hour_data.get('date', '')
                    time_str = hour_data.get('datetime', '')
                    
                    if date_str and time_str:
                        # Combine date and time for parsing
                        datetime_str = f"{date_str}T{time_str}"
                        dt_utc = datetime.fromisoformat(datetime_str).replace(tzinfo=ZoneInfo('UTC'))
                        dt = dt_utc.astimezone(self.sgt)  # Convert to Singapore time
                        forecast_hour = dt.hour
                        
                        if forecast_hour >= start_hour or count > 0:
                            # Format time to HH:MM
                            time_formatted = f"{forecast_hour:02d}:00"
                            condition = hour_data.get('conditions', 'Unknown')
                            
                            forecast_data.append({
                                'time': time_formatted,
                                'condition': condition,
                                'temperature': hour_data.get('temp'),
                                'humidity': hour_data.get('humidity'),
                                'wind_speed': hour_data.get('windspeed'),
                                'wind_direction': hour_data.get('winddir'),
                                'pressure': hour_data.get('pressure'),
                                'precipitation': hour_data.get('precip'),
                                'cloud_cover': hour_data.get('cloudcover'),
                                'feels_like': hour_data.get('feelslike'),
                                'uv_index': hour_data.get('uvindex'),
                                'visibility': hour_data.get('visibility')
                            })
                            
                            count += 1
                
                return {
                    'api': 'Visual Crossing',
                    'forecast': forecast_data,
                    'current_time': formatted_time
                }
            else:
                print("Visual Crossing: No forecast data available")
                return None

        except Exception as e:
            print(f"Visual Crossing Forecast Error: {e}")
            return None
    
    def get_current_weather(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Get current weather conditions from Visual Crossing.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            
        Returns:
            Dictionary containing current weather data or None if error
        """
        try:
            if not self.api_key:
                print("Visual Crossing API key not found in environment variables")
                return None

            # Make API request for current weather
            params = {
                'location': f'{lat},{lon}',
                'key': self.api_key,
                'unitGroup': 'metric',
                'include': 'current'
            }

            response = requests.get(VISUAL_CROSSING_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'currentConditions' in data:
                current = data['currentConditions']
                return {
                    'api': 'Visual Crossing',
                    'current_weather': {
                        'condition': current.get('conditions', 'Unknown')
                    }
                }
            else:
                print("Visual Crossing: No current weather data available")
                return None

        except Exception as e:
            print(f"Visual Crossing Current Weather Error: {e}")
            return None
    
    def get_historical_weather(self, lat: float, lon: float, date: str) -> Optional[Dict[str, Any]]:
        """
        Get historical weather data from Visual Crossing.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            date: Date in YYYY-MM-DD format
            
        Returns:
            Dictionary containing historical weather data or None if error
        """
        try:
            if not self.api_key:
                print("Visual Crossing API key not found in environment variables")
                return None

            # Make API request for historical data
            params = {
                'location': f'{lat},{lon}',
                'key': self.api_key,
                'unitGroup': 'metric',
                'include': 'days',
                'elements': 'datetime,tempmax,tempmin,conditions,humidity,windspeed,precip'
            }

            # Add date parameter for historical data
            url = f"{VISUAL_CROSSING_URL}/{date}"

            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'days' in data and len(data['days']) > 0:
                day_data = data['days'][0]
                return {
                    'api': 'Visual Crossing',
                    'date': date,
                    'historical_weather': {
                        'condition': day_data.get('conditions')
                    }
                }
            else:
                print(f"Visual Crossing: No historical data available for {date}")
                return None

        except Exception as e:
            print(f"Visual Crossing Historical Weather Error: {e}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test the Visual Crossing API connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if not self.api_key:
                print("Visual Crossing API key not found")
                return False
            
            # Test with Singapore coordinates
            test_data = self.fetch_forecast_data(1.2915, 103.8585, 1)
            return test_data is not None
            
        except Exception as e:
            print(f"Visual Crossing connection test failed: {e}")
            return False

# Convenience function for backward compatibility
def fetch_visual_crossing_data(lat: float, lon: float, hours: int = 6) -> Optional[Dict[str, Any]]:
    """
    Legacy function for backward compatibility.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate
        hours: Number of hours to forecast
        
    Returns:
        Dictionary containing forecast data or None if error
    """
    visual_crossing = VisualCrossing()
    return visual_crossing.fetch_forecast_data(lat, lon, hours)

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get coordinates from .env or use defaults
    lat = float(os.getenv('DEFAULT_LAT', '1.2915'))
    lon = float(os.getenv('DEFAULT_LON', '103.8585'))
    
    # Initialize Visual Crossing
    visual_crossing = VisualCrossing()
    
    print("🌤️ Visual Crossing Forecast - Singapore")
    print("=" * 50)
    
    try:
        # Add timestamp like other APIs
        print(f"🕐 Created At: {datetime.now(visual_crossing.sgt).strftime('%d/%m/%Y %H:%M +08:00')}")
        print()
        
        # Demonstrate area name functionality
        print("🔍 Testing Area Name to Coordinates:")
        test_areas = ["Serangoon", "City", "Marine Parade"]
        for area in test_areas:
            coords = visual_crossing.get_coordinates_from_area_name(area)
            if coords:
                print(f"   {area}: {coords[0]:.4f}, {coords[1]:.4f}")
            else:
                print(f"   {area}: Not found")
        print()
        
        # Demonstrate new functionality: Get forecasts for all Singapore areas
        print("🌤️ Getting Visual Crossing forecasts for all Singapore NEA areas...")
        all_forecasts = visual_crossing.get_forecasts_for_all_singapore_areas(2)
        if all_forecasts:
            print(f"✅ Successfully processed {all_forecasts['total_areas']} areas")
            print()
            
            # Show condition summary
            print("📋 Weather Conditions Summary:")
            for condition, areas in all_forecasts['condition_summary'].items():
                print(f"🌤️ {condition} ({len(areas)} areas):")
                # Display first few areas for each condition
                display_areas = areas[:6]  # Show first 6 areas
                print(f"   {' | '.join(f'{area:<15}' for area in display_areas)}")
                if len(areas) > 6:
                    print(f"   ... and {len(areas) - 6} more areas")
                print()
        else:
            print("❌ Failed to get forecasts for all areas")
        print()
            
    except Exception as e:
        print(f"❌ Error: {e}")
