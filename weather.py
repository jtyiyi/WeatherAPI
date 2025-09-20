import os
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API URLs
WEATHERAPI_URL = "http://api.weatherapi.com/v1/forecast.json"
SG_API_URL = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"
VISUAL_CROSSING_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

def fetch_weatherapi_data(api_key, lat, lon):
    """Fetch 6-hour forecast data from WeatherAPI starting from current hour."""
    try:
        if not api_key:
            print("WeatherAPI key not found in environment variables")
            return

        sgt = ZoneInfo('Asia/Singapore')
        current_dt = datetime.now(sgt)
        start_hour = current_dt.hour

        # Make API request
        params = {
            'key': api_key,
            'q': f'{lat},{lon}',
            'days': 2,  # Need 2 days to get 6 hours if starting late in day
            'aqi': 'no',
            'alerts': 'no'
        }

        response = requests.get(WEATHERAPI_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'forecast' in data and 'forecastday' in data['forecast']:
            # Get current time in Singapore timezone
            current_time = current_dt.strftime("%d/%m/%Y %H:%M %z")
            formatted_time = current_time
            if len(formatted_time) >= 5:
                formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
            print(f"=== WeatherAPI 6-Hour Forecast (starting from {formatted_time}) ===")
            
            # Show 6 hours starting from start_hour (allow next day)
            count = 0
            for day in data['forecast']['forecastday']:
                for hour_data in day['hour']:
                    hour_time = datetime.fromisoformat(hour_data['time'].replace('Z', '+00:00'))
                    hour_sgt = hour_time.astimezone(sgt)
                    forecast_hour = hour_sgt.hour
                    
                    if forecast_hour >= start_hour or count > 0:
                        if count >= 6:
                            break
                        
                        # Format time to HH:MM
                        time_str = f"{forecast_hour:02d}:00"
                        condition = hour_data['condition']['text']
                        print(f"{time_str}: {condition}")
                        count += 1
                
                if count >= 6:
                    break
        else:
            print("WeatherAPI: No forecast data available")

    except Exception as e:
        print(f"WeatherAPI Forecast Error: {e}")

def fetch_sg_api_data(lat, lon):
    """Fetch 2-hour forecast data from Singapore NEA API."""
    try:
        # Make API request
        response = requests.get(SG_API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Check if we have area metadata (at root level, not under 'data')
        if 'area_metadata' in data:
            # Find closest area to the given coordinates
            closest_area = None
            min_distance = float('inf')
            
            for area in data['area_metadata']:
                area_lat = area['label_location']['latitude']
                area_lon = area['label_location']['longitude']
                
                # Calculate simple distance
                distance = ((lat - area_lat) ** 2 + (lon - area_lon) ** 2) ** 0.5
                
                if distance < min_distance:
                    min_distance = distance
                    closest_area = area['name']
            
            if closest_area:
                # Get current time in Singapore timezone
                sgt = ZoneInfo('Asia/Singapore')
                current_time = datetime.now(sgt).strftime("%d/%m/%Y %H:%M %z")
                formatted_time = current_time
                if len(formatted_time) >= 5:
                    formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
                
                print(f"=== Singapore 2-Hour Forecast - closest area: {closest_area} ({formatted_time}) ===")
                
                # Get forecast data (items are at root level, not under 'data')
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
                        
                        start_sgt = start_dt.astimezone(sgt)
                        end_sgt = end_dt.astimezone(sgt)
                        
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
                                    break
                    else:
                        print("Singapore NEA: No valid time period found")
                else:
                    print("Singapore NEA: No forecast items available")
            else:
                print("Singapore NEA: No area data available")
        else:
            print("Singapore NEA: No forecast data available")

    except Exception as e:
        print(f"Singapore NEA Forecast Error: {e}")


def fetch_visual_crossing_data(lat, lon):
    """Fetch 6-hour forecast data from Visual Crossing starting from current hour."""
    try:
        api_key = os.getenv('VISUAL_CROSSING_API_KEY')
        if not api_key:
            print("Visual Crossing API key not found in environment variables")
            return

        sgt = ZoneInfo('Asia/Singapore')
        current_dt = datetime.now(sgt)
        start_hour = current_dt.hour

        # Make API request
        params = {
            'location': f'{lat},{lon}',
            'key': api_key,
            'unitGroup': 'metric',
            'include': 'hours',
            'elements': 'datetime,conditions'
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
            print(f"\n=== Visual Crossing 6-Hour Forecast (starting from {formatted_time}) ===")
            
            # Flatten all hours from all days and add date information
            all_hours = []
            for day in data['days']:
                day_date = day.get('datetime', '')
                hours = day.get('hours', [])
                for hour in hours:
                    hour['date'] = day_date  # Add date to each hour
                    all_hours.append(hour)

            # Show 6 hours starting from start_hour
            count = 0
            for hour_data in all_hours:
                if count >= 6:
                    break
                    
                # Parse the time and date
                date_str = hour_data.get('date', '')
                time_str = hour_data.get('datetime', '')
                
                if date_str and time_str:
                    # Combine date and time for parsing
                    datetime_str = f"{date_str}T{time_str}"
                    dt_utc = datetime.fromisoformat(datetime_str).replace(tzinfo=ZoneInfo('UTC'))
                    dt = dt_utc.astimezone(sgt)  # Convert to Singapore time
                    forecast_hour = dt.hour
                    
                    if forecast_hour >= start_hour or count > 0:
                        # Format time to HH:MM
                        time_formatted = f"{forecast_hour:02d}:00"
                        condition = hour_data.get('conditions', 'Unknown')
                        
                        print(f"{time_formatted}: {condition}")
                        count += 1
        else:
            print("Visual Crossing: No forecast data available")

    except Exception as e:
        print(f"Visual Crossing Forecast Error: {e}")

if __name__ == "__main__":
    # Load API keys from environment variables
    api_key = os.getenv('WEATHERAPI_KEY')
    
    # Default coordinates (Singapore)
    lat = 1.2915
    lon = 103.8585
    
    # Fetch data from all APIs
    fetch_weatherapi_data(api_key, lat, lon)
    fetch_sg_api_data(lat, lon)
    fetch_visual_crossing_data(lat, lon)