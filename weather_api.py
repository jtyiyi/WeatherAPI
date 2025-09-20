from flask import Flask, request, jsonify
import os
import sys
import io
import re
from contextlib import redirect_stdout
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo

# Import functions from your existing weather.py
from weather import (
    fetch_weatherapi_data, 
    fetch_sg_api_data, 
    fetch_tomorrow_forecast_data, 
    fetch_visual_crossing_data
)

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

# API Keys
WEATHERAPI_KEY = os.getenv('WEATHERAPI_KEY')
TOMORROW_KEY = os.getenv('TOMORROW_API_KEY')

def get_location_name(lat, lon):
    """Convert latitude and longitude to human-readable address using OpenStreetMap Nominatim API."""
    try:
        import requests
        
        # Use OpenStreetMap Nominatim API (free, no API key required)
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1,
            'zoom': 18
        }
        
        headers = {
            'User-Agent': 'Singapore Weather API/1.0'  # Required by Nominatim
        }
        
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'display_name' in data:
            # Extract the most relevant parts of the address
            address_parts = data.get('address', {})
            
            # Build a clean address
            location_parts = []
            
            # Add neighborhood or suburb
            if 'neighbourhood' in address_parts:
                location_parts.append(address_parts['neighbourhood'])
            elif 'suburb' in address_parts:
                location_parts.append(address_parts['suburb'])
            
            # Add city or town
            if 'city' in address_parts:
                location_parts.append(address_parts['city'])
            elif 'town' in address_parts:
                location_parts.append(address_parts['town'])
            elif 'village' in address_parts:
                location_parts.append(address_parts['village'])
            
            # Add state or region
            if 'state' in address_parts:
                location_parts.append(address_parts['state'])
            elif 'region' in address_parts:
                location_parts.append(address_parts['region'])
            
            # Add country
            if 'country' in address_parts:
                location_parts.append(address_parts['country'])
            
            # Join the parts
            if location_parts:
                return ', '.join(location_parts)
            else:
                return data['display_name']
        else:
            return f"Location at {lat}, {lon}"
            
    except Exception as e:
        print(f"Geocoding error: {e}")
        return f"Location at {lat}, {lon}"

def format_api_response(api_name, data, lat, lon):
    """Format the response from weather functions to consistent API format."""
    if "error" in data:
        return {"api": api_name, "error": data["error"]}
    
    # Extract forecast data and format consistently
    forecast_data = []
    
    if api_name == "WeatherAPI":
        # WeatherAPI returns structured data
        for hour_data in data.get('forecast', []):
            forecast_data.append({
                "time": hour_data.get('time', ''),
                "condition": hour_data.get('condition', 'Unknown')
            })
    elif api_name == "Singapore NEA":
        # Singapore NEA returns structured data
        for hour_data in data.get('forecast', []):
            forecast_data.append({
                "time": hour_data.get('time', ''),
                "condition": hour_data.get('condition', 'Unknown')
            })
    elif api_name == "Tomorrow.io":
        # Tomorrow.io returns structured data
        for hour_data in data.get('forecast', []):
            forecast_data.append({
                "time": hour_data.get('time', ''),
                "condition": hour_data.get('condition', 'Unknown')
            })
    elif api_name == "Visual Crossing":
        # Visual Crossing returns structured data
        for hour_data in data.get('forecast', []):
            forecast_data.append({
                "time": hour_data.get('time', ''),
                "condition": hour_data.get('condition', 'Unknown')
            })
    
    return {
        "api": api_name,
        "current_time": data.get('current_time', ''),
        "forecast": forecast_data
    }

def fetch_all_weather_data(lat, lon, hours=3):
    """Fetch weather data from all 4 APIs using existing functions."""
    results = []
    
    # Fetch from all APIs using your existing functions
    apis = [
        ("WeatherAPI", lambda: fetch_weatherapi_data(WEATHERAPI_KEY, lat, lon)),
        ("Singapore NEA", lambda: fetch_sg_api_data(lat, lon)),
        ("Tomorrow.io", lambda: fetch_tomorrow_forecast_data(TOMORROW_KEY, lat, lon)),
        ("Visual Crossing", lambda: fetch_visual_crossing_data(lat, lon))
    ]
    
    for api_name, fetch_func in apis:
        try:
            # Capture the print output from your weather functions
            f = io.StringIO()
            with redirect_stdout(f):
                result = fetch_func()
            
            # Get the captured output
            output = f.getvalue()
            
            # Create a simple response based on the output
            if output:
                # Check for error messages in output
                if "Error:" in output or "401" in output or "Unauthorized" in output:
                    results.append({
                        "api": api_name,
                        "error": "API key not configured or invalid"
                    })
                elif "429" in output or "Too Many Calls" in output or "rate limit" in output.lower():
                    results.append({
                        "api": api_name,
                        "error": "Rate limit exceeded - please try again later"
                    })
                else:
                    # Parse the output to extract forecast data
                    lines = output.strip().split('\n')
                    forecast_data = []
                    
                    for line in lines:
                        if ':' in line and not line.startswith('==='):
                            # Handle different time formats
                            if '-' in line and ':' in line:
                                # Singapore NEA format: "20:00-22:00: Partly Cloudy (Night)"
                                # Find the pattern: time_range: condition
                                match = re.match(r'^([0-9:]+-[0-9:]+):\s*(.+)$', line)
                                if match:
                                    time_part = match.group(1).strip()
                                    condition = match.group(2).strip()
                                    forecast_data.append({
                                        "time": time_part,
                                        "condition": condition
                                    })
                            else:
                                # Standard format: "16:00: Patchy rain nearby"
                                parts = line.split(':', 2)
                                if len(parts) >= 3:
                                    time_part = parts[0].strip()
                                    condition = parts[2].strip()
                                    
                                    # Skip header lines
                                    if not any(word in time_part.lower() for word in ['forecast', 'closest', 'area', 'starting']):
                                        # Format time to HH:MM
                                        if len(time_part) == 1 or len(time_part) == 2:
                                            # Convert "15" to "15:00"
                                            time_formatted = f"{time_part.zfill(2)}:00"
                                        elif ':' in time_part:
                                            # Already formatted like "15:00"
                                            time_formatted = time_part
                                        else:
                                            time_formatted = time_part
                                        
                                        forecast_data.append({
                                            "time": time_formatted,
                                            "condition": condition
                                        })
                    
                    if forecast_data:
                        # Get current time in Singapore timezone
                        sgt = ZoneInfo('Asia/Singapore')
                        current_time = datetime.now(sgt).strftime("%d/%m/%Y %H:%M %z")
                        if len(current_time) >= 5:
                            current_time = f"{current_time[:-5]}{current_time[-5:-2]}:{current_time[-2:]}"
                        
                        results.append({
                            "api": api_name,
                            "current_time": current_time,
                            "forecast": forecast_data[:hours]  # Limit to requested hours
                        })
                    else:
                        results.append({
                            "api": api_name,
                            "error": "No forecast data available"
                        })
            else:
                results.append({
                    "api": api_name,
                    "error": "No data returned"
                })
                
        except Exception as e:
            results.append({
                "api": api_name,
                "error": f"Failed to fetch data: {str(e)}"
            })
    
    return results

# Basic route
@app.route('/')
def hello():
    return jsonify({
        "message": "🌤️ Singapore Weather API - Clean Format with Geocoding",
        "version": "4.0",
        "description": "Returns weather data in clean, readable format with human-readable location names",
                "features": [
                    "4 Weather APIs (WeatherAPI, Singapore NEA, Tomorrow.io, Visual Crossing)",
                    "Geocoding: Converts lat/lon to human-readable addresses",
                    "Clean output format matching weather.py"
                ],
        "endpoints": {
            "weather": "/weather?lat=1.2915&lon=103.8585&hours=3",
            "health": "/health"
        },
                "apis": ["WeatherAPI", "Singapore NEA", "Tomorrow.io", "Visual Crossing", "OpenStreetMap (Geocoding)"],
        "parameters": {
            "lat": "Latitude (required)",
            "lon": "Longitude (required)", 
            "hours": "Forecast hours (optional, default: 3)"
        },
        "example": "http://localhost:8001/weather?lat=1.2915&lon=103.8585&hours=3"
    })

# Health check route
@app.route('/health')
def health():
    return jsonify({"status": "healthy", "message": "API is running with 4 weather sources"})

# Weather endpoint
@app.route('/weather')
def get_weather():
    # Get parameters from URL
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    hours = request.args.get('hours', '3')
    
    # Basic validation
    if not lat or not lon:
        return jsonify({
            "error": "Missing required parameters",
            "message": "Please provide 'lat' and 'lon' parameters"
        }), 400
    
    try:
        # Convert to float
        lat = float(lat)
        lon = float(lon)
        hours = int(hours)
        
        # Get human-readable location name
        location_name = get_location_name(lat, lon)
        
        # Get weather data from all APIs
        all_data = fetch_all_weather_data(lat, lon, hours)
        
        return jsonify({
            "status": "success",
            "location": location_name,
            "forecast_hours": hours,
            "sources": len(all_data),
            "data": all_data
        })
        
    except ValueError:
        return jsonify({
            "error": "Invalid parameters",
            "message": "lat and lon must be numbers, hours must be an integer"
        }), 400

# Run the app
if __name__ == '__main__':
    print("🌤️  Starting Singapore Weather API with 4 Sources...")
    print("📍 Available endpoints:")
    print("   GET /weather?lat=1.2915&lon=103.8585&hours=3")
    print("   GET /health")
    print("   GET /")
    app.run(debug=True, host='0.0.0.0', port=8001)
