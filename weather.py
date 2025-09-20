import requests
import json
import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

# API URLs
WEATHERAPI_URL = "http://api.weatherapi.com/v1/forecast.json"
SG_API_URL = "https://api-open.data.gov.sg/v2/real-time/api/two-hr-forecast"
TOMORROW_URL = "https://api.tomorrow.io/v4/timelines"
VISUAL_CROSSING_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

# =============================================================================
# WEATHER API FUNCTIONS
# =============================================================================

def fetch_weatherapi_data(api_key, lat, lon):
    """Fetch 4-hour forecast data from WeatherAPI starting from current hour."""
    try:
        params = {
            "key": api_key,
            "q": f"{lat},{lon}",
            "days": 2,
            "aqi": "no",
            "alerts": "no"
        }
        
        response = requests.get(WEATHERAPI_URL, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # Extract hourly forecast data from multiple days
        forecast = data.get('forecast', {}).get('forecastday', [])
        if forecast:
            # Combine hourly data from all forecast days
            hourly = []
            for day in forecast:
                hourly.extend(day.get('hour', []))
            
            # Use current hour
            location = data.get('location', {})
            tz_id = location.get('tz_id', 'UTC')
            try:
                tz = ZoneInfo(tz_id)
            except:
                tz = ZoneInfo("UTC")
            start_hour = datetime.now(tz).hour
            current_dt = datetime.now(tz)
            formatted_time = current_dt.strftime("%d/%m/%Y %H:%M %z")
            if len(formatted_time) >= 5:
                formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
            print(f"=== WeatherAPI 4-Hour Forecast (starting from {formatted_time}) ===")
            
            # Show 4 hours starting from start_hour (allow next day)
            count = 0
            start_found = False
            
            for i, hour_data in enumerate(hourly):
                if count >= 4:
                    break
                    
                # Get the hour from the time string
                time_str = hour_data.get('time', '')
                if time_str:
                    # Parse the full datetime string properly
                    try:
                        # WeatherAPI returns format: "2025-09-18 00:00" in Singapore time
                        # We need to treat it as Singapore timezone
                        sgt = ZoneInfo('Asia/Singapore')
                        dt = datetime.fromisoformat(time_str).replace(tzinfo=sgt)
                        current_hour = dt.hour
                        
                        # Find the first hour that matches our start hour
                        if not start_found and current_hour >= start_hour:
                            start_found = True
                        
                        # Show hours starting from our start hour
                        if start_found:
                            time_display = dt.strftime("%H:%M")
                            condition = hour_data.get('condition', {}).get('text', 'Unknown')
                            
                            print(f"{time_display}: {condition}")
                            count += 1
                    except ValueError:
                        # Fallback to old method if parsing fails
                        hour_part = time_str.split(' ')[1][:2] if ' ' in time_str else "00"
                        current_hour = int(hour_part)
                        
                        if not start_found and current_hour >= start_hour:
                            start_found = True
                        
                        if start_found:
                            time_display = time_str.split(' ')[1][:5] if ' ' in time_str else f"{current_hour:02d}:00"
                            condition = hour_data.get('condition', {}).get('text', 'Unknown')
                            
                            print(f"{time_display}: {condition}")
                            count += 1
        
    except Exception as e:
        print(f"WeatherAPI Error: {e}")

def simple_distance_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points using flat Earth approximation."""
    import math
    km_per_deg = 111.32
    mean_lat_rad = math.radians((lat1 + lat2) / 2.0)
    dx = (lon2 - lon1) * km_per_deg * math.cos(mean_lat_rad)
    dy = (lat2 - lat1) * km_per_deg
    return math.hypot(dx, dy)

def fetch_sg_api_data(lat, lon):
    """Fetch data from Singapore 2-hour forecast API and show forecast for closest area."""
    try:
        response = requests.get(SG_API_URL, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # Get area coordinates
        area_metadata = data.get('data', {}).get('area_metadata', [])
        if not area_metadata:
            print("No area metadata found")
            return
            
        # Find closest area
        closest_area = None
        min_distance = float('inf')
        
        for area in area_metadata:
            area_lat = area.get('label_location', {}).get('latitude')
            area_lon = area.get('label_location', {}).get('longitude')
            if area_lat is not None and area_lon is not None:
                distance = simple_distance_km(lat, lon, area_lat, area_lon)
                if distance < min_distance:
                    min_distance = distance
                    closest_area = area
        
        if not closest_area:
            print("Could not find closest area")
            return
            
        closest_name = closest_area.get('name', 'Unknown')
        
        # Get forecasts for the closest area
        items = data.get('data', {}).get('items', [])
        if not items:
            print("No forecast items found")
            return
            
        # Get the date and timezone from the first item for the header
        first_item = items[0] if items else None
        if first_item:
            valid_period = first_item.get('valid_period', {})
            start_time = valid_period.get('start')
            if start_time:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                sgt = ZoneInfo('Asia/Singapore')
                start_sgt = start_dt.astimezone(sgt)
                header_time = start_sgt.strftime("%d/%m/%Y %H:%M %z")
                if len(header_time) >= 5:
                    header_time = f"{header_time[:-5]}{header_time[-5:-2]}:{header_time[-2:]}"
                print(f"\n=== Singapore 2-Hour Forecast - closest area: {closest_name} ({header_time}) ===")
            else:
                print(f"\n=== Singapore 2-Hour Forecast - closest area: {closest_name} ===")
        else:
            print(f"\n=== Singapore 2-Hour Forecast - closest area: {closest_name} ===")
        
        # Show next 4 hours (2 two-hour blocks)
        for i in range(min(2, len(items))):
            item = items[i]
            valid_period = item.get('valid_period', {})
            start_time = valid_period.get('start')
            end_time = valid_period.get('end')
            
            if start_time and end_time:
                # Parse and format timestamps
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                
                # Convert to Singapore timezone
                sgt = ZoneInfo('Asia/Singapore')
                start_sgt = start_dt.astimezone(sgt)
                end_sgt = end_dt.astimezone(sgt)
                
                # Format time as HH:MM
                start_time_str = start_sgt.strftime("%H:%M")
                end_time_str = end_sgt.strftime("%H:%M")
                
                # Find forecast for closest area
                forecasts = item.get('forecasts', [])
                for forecast in forecasts:
                    if forecast.get('area') == closest_name:
                        forecast_text = forecast.get('forecast', 'No forecast available')
                        print(f"{start_time_str}-{end_time_str}: {forecast_text}")
                        break
        
    except Exception as e:
        print(f"SG API Error: {e}")

def get_weather_description(code):
    """Convert weather code to description."""
    weather_codes = {
        1000: "Clear",
        1001: "Cloudy", 
        1100: "Mostly Clear",
        1101: "Partly Cloudy",
        1102: "Mostly Cloudy",
        2000: "Fog",
        2100: "Light Fog",
        3000: "Light Precipitation",
        4000: "Light Rain",
        4001: "Rain",
        4200: "Light Rain Showers",
        5000: "Heavy Rain",
        6000: "Freezing Rain",
        7000: "Snow",
        8000: "Severe Weather"
    }
    return weather_codes.get(code, f"Unknown ({code})")

def fetch_tomorrow_forecast_data(api_key, lat, lon):
    """Fetch and parse Tomorrow.io forecast data."""
    try:
        params = {
            "location": f"{lat},{lon}",
            "fields": "weatherCode",
            "timesteps": "1h",
            "apikey": api_key
        }
        
        response = requests.get(TOMORROW_URL, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # Extract timeline data
        timelines = data.get('data', {}).get('timelines', [])
        if not timelines:
            print("No timeline data found")
            return
            
        timeline = timelines[0]
        intervals = timeline.get('intervals', [])
        
        # Use current hour
        sgt = ZoneInfo('Asia/Singapore')
        current_dt = datetime.now(sgt)
        start_hour = current_dt.hour
        formatted_time = current_dt.strftime("%d/%m/%Y %H:%M %z")
        if len(formatted_time) >= 5:
            formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
        print(f"\n=== Tomorrow.io 4-Hour Forecast (starting from {formatted_time}) ===")
        
        # Show next 4 hours (filter duplicates)
        count = 0
        seen_hours = set()
        start_found = False
        
        for interval in intervals:
            if count >= 4:
                break
                
            start_time = interval.get('startTime', '')
            values = interval.get('values', {})
            
            if start_time:
                # Parse and format time
                dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                # Convert to Singapore timezone for consistent comparison
                sgt = ZoneInfo('Asia/Singapore')
                dt_sgt = dt.astimezone(sgt)
                forecast_hour = dt_sgt.hour
                
                # Find the first hour that matches our start hour
                if not start_found and forecast_hour >= start_hour:
                    start_found = True
                
                # Show hours starting from our start hour (allow next day)
                if start_found and forecast_hour not in seen_hours:
                    time_str = dt_sgt.strftime("%H:%M")
                    
                    # Get weather data
                    weather_code = values.get('weatherCode', 'N/A')
                    weather_desc = get_weather_description(weather_code)
                    
                    print(f"{time_str}: {weather_desc}")
                    seen_hours.add(forecast_hour)
                    count += 1
        
    except Exception as e:
        print(f"Tomorrow.io Forecast Error: {e}")

def get_openmeteo_weather_description(code):
    """Convert Open-Meteo weather code to description."""
    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        56: "Light freezing drizzle",
        57: "Dense freezing drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        66: "Light freezing rain",
        67: "Heavy freezing rain",
        71: "Slight snow fall",
        73: "Moderate snow fall",
        75: "Heavy snow fall",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail"
    }
    return weather_codes.get(code, f"Unknown ({code})")

def fetch_visual_crossing_data(lat, lon):
    """Fetch 4-hour forecast data from Visual Crossing starting from current hour."""
    try:
        # Get API key
        api_key = os.getenv('VISUAL_CROSSING_API_KEY')
        if not api_key:
            print("Visual Crossing API key not found in environment variables")
            return
        
        # Create location string for Visual Crossing API
        location = f"{lat},{lon}"
        
        # Get current date and next day for the API call
        sgt = ZoneInfo('Asia/Singapore')
        today = datetime.now(sgt).date()
        tomorrow = today.replace(day=today.day + 1) if today.day < 28 else today.replace(month=today.month + 1, day=1)
        
        # Build the URL with location and date range
        url = f"{VISUAL_CROSSING_URL}/{location}/{today}/{tomorrow}"
        
        params = {
            "unitGroup": "metric",
            "include": "hours",
            "key": api_key
        }
        
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        # Extract hourly data
        days = data.get('days', [])
        if not days:
            print("No forecast data found")
            return
        
        # Flatten all hours from all days and add date information
        all_hours = []
        for day in days:
            day_date = day.get('datetime', '')
            hours = day.get('hours', [])
            for hour in hours:
                # Add the date to each hour entry
                hour['date'] = day_date
                all_hours.append(hour)
        
        if not all_hours:
            print("No hourly forecast data found")
            return
        
        # Use current hour
        current_dt = datetime.now(sgt)
        start_hour = current_dt.hour
        formatted_time = current_dt.strftime("%d/%m/%Y %H:%M %z")
        if len(formatted_time) >= 5:
            formatted_time = f"{formatted_time[:-5]}{formatted_time[-5:-2]}:{formatted_time[-2:]}"
        print(f"\n=== Visual Crossing 4-Hour Forecast (starting from {formatted_time}) ===")
        
        # Show next 4 hours
        count = 0
        start_found = False
        
        for hour_data in all_hours:
            if count >= 4:
                break
                
            # Parse time
            time_str = hour_data.get('datetime', '')  # This is "HH:MM:SS"
            date_str = hour_data.get('date', '')      # This is "YYYY-MM-DD"
            
            if not time_str or not date_str:
                continue
                
            # Combine date and time to create full datetime
            datetime_str = f"{date_str}T{time_str}"
            # Visual Crossing returns time in UTC, but we need Singapore time
            dt_utc = datetime.fromisoformat(datetime_str).replace(tzinfo=ZoneInfo('UTC'))
            # Convert to Singapore time
            sgt = ZoneInfo('Asia/Singapore')
            dt = dt_utc.astimezone(sgt)
            forecast_hour = dt.hour
            
            # Find the first forecast that matches our start hour
            if not start_found and forecast_hour >= start_hour:
                start_found = True
                time_display = dt.strftime("%H:%M")
                condition = hour_data.get('conditions', 'Unknown')
                
                print(f"{time_display}: {condition}")
                count += 1
            # Continue with subsequent hours
            elif start_found:
                time_display = dt.strftime("%H:%M")
                condition = hour_data.get('conditions', 'Unknown')
                
                print(f"{time_display}: {condition}")
                count += 1
        
    except Exception as e:
        print(f"Visual Crossing Forecast Error: {e}")

if __name__ == "__main__":
    # Load API keys and configuration from environment variables
    api_key = os.getenv('WEATHERAPI_KEY')
    tomorrow_api_key = os.getenv('TOMORROW_API_KEY')
    visual_crossing_key = os.getenv('VISUAL_CROSSING_API_KEY')
    lat = float(os.getenv('LAT'))
    lon = float(os.getenv('LON'))
    
    # Fetch data from all four APIs (all use current hour automatically)
    fetch_weatherapi_data(api_key, lat, lon)
    fetch_sg_api_data(lat, lon)
    fetch_tomorrow_forecast_data(tomorrow_api_key, lat, lon)
    fetch_visual_crossing_data(lat, lon)