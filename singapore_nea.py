"""
Singapore NEA (National Environment Agency) API Integration Module
Handles all interactions with the Singapore government weather API.
"""

import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Any, Optional

# API Configuration
SG_API_URL = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"

class SingaporeNEA:
    """Singapore NEA weather service integration."""
    
    def __init__(self):
        """Initialize Singapore NEA API."""
        self.sgt = ZoneInfo('Asia/Singapore')
    
    def find_closest_area(self, lat: float, lon: float, area_metadata: List[Dict]) -> Optional[str]:
        """
        Find the closest area to the given coordinates.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            area_metadata: List of area metadata from API
            
        Returns:
            Name of the closest area or None if not found
        """
        closest_area = None
        min_distance = float('inf')
        
        for area in area_metadata:
            area_lat = area['label_location']['latitude']
            area_lon = area['label_location']['longitude']
            
            # Calculate simple distance
            distance = ((lat - area_lat) ** 2 + (lon - area_lon) ** 2) ** 0.5
            
            if distance < min_distance:
                min_distance = distance
                closest_area = area['name']
        
        return closest_area
    
    def fetch_forecast_data(self, lat: float, lon: float, hours: int = 2) -> Optional[Dict[str, Any]]:
        """
        Fetch forecast data from Singapore NEA API.
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            hours: Number of hours to forecast (default: 2, max: 2 for NEA)
            
        Returns:
            Dictionary containing forecast data or None if error
        """
        try:
            # Make API request
            response = requests.get(SG_API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Check if we have area metadata
            if 'area_metadata' in data:
                # Find closest area to the given coordinates
                closest_area = self.find_closest_area(lat, lon, data['area_metadata'])
                
                if closest_area:
                    # Get current time in Singapore timezone
                    current_time = datetime.now(self.sgt).strftime("%d/%m/%Y %H:%M %z")
                    formatted_time = current_time
                    if len(formatted_time) >= 5:
                        formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
                    
                    print(f"=== Singapore 2-Hour Forecast - closest area: {closest_area} ({formatted_time}) ===")
                    
                    # Get forecast data
                    if 'items' in data and len(data['items']) > 0:
                        latest_forecast = data['items'][0]
                        
                        # Get the time period from the item level
                        valid_period = latest_forecast.get('valid_period', {})
                        start_time = valid_period.get('start', '')
                        end_time = valid_period.get('end', '')
                        
                        if start_time and end_time:
                            # Parse the time period
                            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                            
                            start_sgt = start_dt.astimezone(self.sgt)
                            end_sgt = end_dt.astimezone(self.sgt)
                            
                            # Format time period
                            start_time_str = start_sgt.strftime("%H:%M")
                            end_time_str = end_sgt.strftime("%H:%M")
                            time_period = f"{start_time_str}-{end_time_str}"
                            
                            # Find the forecast for the closest area
                            if 'forecasts' in latest_forecast:
                                for forecast in latest_forecast['forecasts']:
                                    if forecast['area'] == closest_area:
                                        condition = forecast.get('forecast', '')
                                        
                                        if condition:
                                            print(f"{time_period}: {condition}")
                                            
                                            return {
                                                'api': 'Singapore NEA',
                                                'forecast': [{
                                                    'time': time_period,
                                                    'condition': condition,
                                                    'area': closest_area,
                                                    'start_time': start_time,
                                                    'end_time': end_time
                                                }],
                                                'area_metadata': {
                                                    'closest_area': closest_area,
                                                    'all_areas': [area['name'] for area in data['area_metadata']]
                                                },
                                                'current_time': formatted_time
                                            }
                                        break
                        else:
                            print("Singapore NEA: No valid time period found")
                    else:
                        print("Singapore NEA: No forecast items available")
                else:
                    print("Singapore NEA: No area data available")
            else:
                print("Singapore NEA: No forecast data available")
            
            return None

        except Exception as e:
            print(f"Singapore NEA Forecast Error: {e}")
            return None
    
    def get_all_areas(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get all available areas from Singapore NEA API.
        
        Returns:
            List of area metadata or None if error
        """
        try:
            response = requests.get(SG_API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'area_metadata' in data:
                return data['area_metadata']
            else:
                print("Singapore NEA: No area metadata available")
                return None

        except Exception as e:
            print(f"Singapore NEA Areas Error: {e}")
            return None
    
    def get_forecast_for_area(self, area_name: str) -> Optional[Dict[str, Any]]:
        """
        Get forecast data for a specific area.
        
        Args:
            area_name: Name of the area to get forecast for
            
        Returns:
            Dictionary containing forecast data or None if error
        """
        try:
            response = requests.get(SG_API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'items' in data and len(data['items']) > 0:
                latest_forecast = data['items'][0]
                
                # Get the time period from the item level
                valid_period = latest_forecast.get('valid_period', {})
                start_time = valid_period.get('start', '')
                end_time = valid_period.get('end', '')
                
                time_period = 'N/A'
                if start_time and end_time:
                    # Parse the time period
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                    
                    start_sgt = start_dt.astimezone(self.sgt)
                    end_sgt = end_dt.astimezone(self.sgt)
                    
                    # Format time period
                    start_time_str = start_sgt.strftime("%H:%M")
                    end_time_str = end_sgt.strftime("%H:%M")
                    time_period = f"{start_time_str}-{end_time_str}"
                
                if 'forecasts' in latest_forecast:
                    for forecast in latest_forecast['forecasts']:
                        if forecast['area'] == area_name:
                            condition = forecast.get('forecast', '')
                            
                            if condition:
                                return {
                                    'api': 'Singapore NEA',
                                    'area': area_name,
                                    'forecast': condition,
                                    'time': time_period,
                                    'timestamp': latest_forecast.get('timestamp', '')
                                }
            
            print(f"Singapore NEA: No forecast found for area '{area_name}'")
            return None

        except Exception as e:
            print(f"Singapore NEA Area Forecast Error: {e}")
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
            # Get all areas first
            areas = self.get_all_areas()
            if not areas:
                print("Singapore NEA: No areas available")
                return None
            
            all_forecasts = []
            condition_summary = {}
            
            print(f"Singapore NEA: Processing {len(areas)} areas...")
            
            for area in areas:
                area_name = area['name']
                coords = area['label_location']
                lat = coords['latitude']
                lon = coords['longitude']
                
                # Get forecast for this area
                forecast = self.get_forecast_for_area(area_name)
                if forecast and 'forecast' in forecast:
                    condition = forecast['forecast']
                    time_str = forecast.get('time', 'N/A')
                    
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
                'api': 'Singapore NEA',
                'total_areas': len(all_forecasts),
                'forecasts': all_forecasts,
                'condition_summary': condition_summary,
                'current_time': datetime.now(self.sgt).strftime('%d/%m/%Y %H:%M +08:00')
            }
            
        except Exception as e:
            print(f"Singapore NEA: Error getting forecasts for all areas: {e}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test the Singapore NEA API connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Test with Singapore coordinates
            test_data = self.fetch_forecast_data(1.2915, 103.8585, 2)
            return test_data is not None
            
        except Exception as e:
            print(f"Singapore NEA connection test failed: {e}")
            return False

# Convenience function for backward compatibility
def fetch_sg_api_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """
    Legacy function for backward compatibility.
    
    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate
        
    Returns:
        Dictionary containing forecast data or None if error
    """
    sg_nea = SingaporeNEA()
    return sg_nea.fetch_forecast_data(lat, lon, 2)

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Initialize Singapore NEA API
    sg_nea = SingaporeNEA()
    
    # Get all available areas
    print("🌤️ Singapore Weather Forecast - All Areas")
    print("=" * 50)
    
    # Get all available areas and their forecasts
    try:
        # Get all areas first
        areas = sg_nea.get_all_areas()
        if not areas:
            print("❌ Failed to retrieve area data")
        else:
            print(f"🕐 Created At: {datetime.now(sg_nea.sgt).strftime('%d/%m/%Y %H:%M +08:00')}")
            print(f"📊 Total Areas: {len(areas)}")
            print()
            
            # Get forecast for each area
            all_forecasts = []
            for area in areas:
                area_name = area['name']
                forecast = sg_nea.get_forecast_for_area(area_name)
                if forecast and 'forecast' in forecast:
                    all_forecasts.append({
                        'area': area_name,
                        'condition': forecast['forecast'],
                        'time': forecast.get('time', 'N/A')
                    })
            
            if all_forecasts:
                # Group forecasts by condition and time for better readability
                conditions = {}
                for forecast in all_forecasts:
                    condition = forecast['condition']
                    time = forecast['time']
                    key = f"{condition} ({time})"
                    if key not in conditions:
                        conditions[key] = []
                    conditions[key].append(forecast['area'])
                
                # Display forecasts grouped by condition and time
                for condition_time, areas in conditions.items():
                    print(f"🌤️ {condition_time}:")
                    # Sort areas alphabetically
                    areas.sort()
                    # Display areas in columns for better readability
                    for i in range(0, len(areas), 4):
                        row_areas = areas[i:i+4]
                        print(f"   {' | '.join(f'{area:<15}' for area in row_areas)}")
                    print()
            else:
                print("❌ Failed to retrieve forecast data")
            
    except Exception as e:
        print(f"❌ Error: {e}")
