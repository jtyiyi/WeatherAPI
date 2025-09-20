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
    fetch_visual_crossing_data
)

# Import database functions
from database import get_weather_db

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

# API Keys
WEATHERAPI_KEY = os.getenv('WEATHERAPI_KEY')
GOOGLE_MAPS_KEY = os.getenv('GOOGLE_MAPS_KEY')



def get_location_name(lat, lon):
    """Convert latitude and longitude to human-readable address using Google Geocoding API with OpenStreetMap fallback."""
    try:
        import requests
        
        # Try Google Geocoding API first
        if GOOGLE_MAPS_KEY:
            try:
                url = "https://maps.googleapis.com/maps/api/geocode/json"
                params = {
                    'latlng': f"{lat},{lon}",
                    'key': GOOGLE_MAPS_KEY,
                    'language': 'en-US',  # US English language
                    'region': 'sg'        # Bias results towards Singapore
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if data.get('status') == 'OK' and data.get('results'):
                    result = data['results'][0]
                    address = result.get('formatted_address', f"Location at {lat}, {lon}")
                    return address
                else:
                    print(f"Google Geocoding API error: {data.get('error_message', 'Unknown error')}")
                    # Fall through to OpenStreetMap
            except Exception as e:
                print(f"Google Geocoding API failed: {e}")
                # Fall through to OpenStreetMap
        
        # Fallback to OpenStreetMap Nominatim API
        print("Using OpenStreetMap as fallback...")
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'addressdetails': 1,
            'zoom': 18,
            'accept-language': 'en-US'  # US English for OpenStreetMap
        }
        
        headers = {
            'User-Agent': 'Singapore Weather API/1.0',
            'Accept-Language': 'en-US'  # US English header
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
                address = ', '.join(location_parts)
                return address
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
        return {
            "api": api_name,
            "error": data["error"]
        }
    
    # Parse the captured output to extract forecast data
    lines = data.get("output", "").strip().split('\n')
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
        
        # Format timezone properly
        if len(current_time) >= 5:
            current_time = f"{current_time[:-5]}{current_time[-5:-2]}:{current_time[-2:]}"
        
        return {
            "api": api_name,
            "current_time": current_time,
            "forecast": forecast_data
        }
    else:
        return {
            "api": api_name,
            "error": "No forecast data available"
        }

def fetch_all_weather_data(lat, lon, hours=3):
    """Fetch weather data from all available APIs."""
    results = []
    
    # List of APIs to fetch from
    apis = [
        ("WeatherAPI", lambda: fetch_weatherapi_data(WEATHERAPI_KEY, lat, lon)),
        ("Singapore NEA", lambda: fetch_sg_api_data(lat, lon)),
        ("Visual Crossing", lambda: fetch_visual_crossing_data(lat, lon))
    ]
    
    for api_name, fetch_func in apis:
        try:
            # Capture the output from the weather functions
            f = io.StringIO()
            with redirect_stdout(f):
                result = fetch_func()
            
            output = f.getvalue()
            
            # Check for specific error messages
            if "API key not found" in output or "API key not configured" in output:
                results.append({
                    "api": api_name,
                    "error": "API key not configured or invalid"
                })
            elif "Rate limit exceeded" in output or "401" in output:
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
                    
                    # Format timezone properly
                    if len(current_time) >= 5:
                        current_time = f"{current_time[:-5]}{current_time[-5:-2]}:{current_time[-2:]}"
                    
                    results.append({
                        "api": api_name,
                        "current_time": current_time,
                        "forecast": forecast_data[:hours]
                    })
                else:
                    results.append({
                        "api": api_name,
                        "error": "No forecast data available"
                    })
                    
        except Exception as e:
            results.append({
                "api": api_name,
                "error": f"Failed to fetch data: {str(e)}"
            })
    
    return results

@app.route('/')
def hello():
    return jsonify({
        "message": "🌤️ Singapore Weather API - Clean Format with Geocoding & Database Storage",
        "version": "5.0",
        "description": "Returns weather data in clean, readable format with human-readable location names and automatic database storage",
        "features": [
            "3 Weather APIs (WeatherAPI, Singapore NEA, Visual Crossing)",
            "Geocoding: Converts lat/lon to human-readable addresses using Google Maps API with OpenStreetMap fallback",
            "Clean output format matching weather.py",
            "Optional parameters: Uses default coordinates and hours from .env if not provided",
            "Automatic Supabase database storage of all weather data",
            "Database query endpoints for retrieving stored weather data"
        ],
        "endpoints": {
            "weather": "/weather?lat=1.2915&lon=103.8585&hours=3",
            "weather_default": "/weather (uses default coordinates and hours from .env)",
            "database_stats": "/database/stats",
            "database_weather": "/database/weather?lat=1.2915&lon=103.8585&limit=10&api=WeatherAPI",
            "health": "/health"
        },
        "apis": ["WeatherAPI", "Singapore NEA", "Visual Crossing", "Google Maps (Geocoding)", "OpenStreetMap (Fallback)"],
        "parameters": {
            "lat": "Latitude (optional, uses DEFAULT_LAT from .env if not provided)",
            "lon": "Longitude (optional, uses DEFAULT_LON from .env if not provided)",
            "hours": "Forecast hours (optional, uses DEFAULT_HOURS from .env if not provided)"
        },
        "examples": [
            "http://localhost:8001/weather (uses all defaults from .env)",
            "http://localhost:8001/weather?hours=6 (uses default coordinates, custom hours)",
            "http://localhost:8001/weather?lat=1.2915&lon=103.8585&hours=3 (custom coordinates and hours)",
            "http://localhost:8001/database/stats (get database statistics)",
            "http://localhost:8001/database/weather (get stored weather data)",
            "http://localhost:8001/database/weather?api=WeatherAPI&limit=5 (get last 5 WeatherAPI records)"
        ]
    })

@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "message": "API is running with 3 weather sources"
    })

@app.route('/weather')
def get_weather():
    """Get weather data for a specific location."""
    # Get parameters from request or use defaults from .env
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    hours = request.args.get('hours')
    
    # Use default coordinates from .env if not provided
    if not lat:
        lat = os.getenv('DEFAULT_LAT', '1.2915')
    if not lon:
        lon = os.getenv('DEFAULT_LON', '103.8585')
    if not hours:
        hours = os.getenv('DEFAULT_HOURS', '3')  # Fixed: Now uses .env default
    
    try:
        lat = float(lat)
        lon = float(lon)
        hours = int(hours)
        
        # Get human-readable location name
        location_name = get_location_name(lat, lon)
        
        # Get weather data from all APIs
        all_data = fetch_all_weather_data(lat, lon, hours)
        
        # Prepare API response
        api_response = {
            "status": "success",
            "location": location_name,
            "coordinates": f"{lat}, {lon}",
            "forecast_hours": hours,
            "sources": len(all_data),
            "data": all_data
        }
        
        # Save to database (if enabled)
        if os.getenv("ENABLE_DATABASE", "true").lower() == "true":
            try:
                db = get_weather_db()
                if db.test_connection():
                    db.insert_weather_data(api_response)
                    api_response["database_status"] = "saved"
                else:
                    api_response["database_status"] = "connection_failed"
            except Exception as e:
                print(f"Database error: {e}")
                api_response["database_status"] = f"error: {str(e)}"
        else:
            api_response["database_status"] = "disabled"
        
        return jsonify(api_response)
        
    except ValueError:
        return jsonify({
            "error": "Invalid parameters",
            "message": "lat and lon must be numbers, hours must be an integer"
        }), 400

@app.route('/database/stats')
def get_database_stats():
    """Get database statistics."""
    try:
        db = get_weather_db()
        if not db.test_connection():
            return jsonify({
                "error": "Database connection failed"
            }), 500
            
        stats = db.get_database_stats()
        return jsonify({
            "status": "success",
            "database_stats": stats
        })
        
    except Exception as e:
        return jsonify({
            "error": "Failed to get database stats",
            "message": str(e)
        }), 500

@app.route('/database/weather')
def get_stored_weather():
    """Get stored weather data from database."""
    # Get parameters
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    limit = request.args.get('limit', '10')
    api_source = request.args.get('api')
    
    # Use default coordinates from .env if not provided
    if not lat:
        lat = os.getenv('DEFAULT_LAT', '1.2915')
    if not lon:
        lon = os.getenv('DEFAULT_LON', '103.8585')
    
    try:
        lat = float(lat)
        lon = float(lon)
        limit = int(limit)
        
        db = get_weather_db()
        if not db.test_connection():
            return jsonify({
                "error": "Database connection failed"
            }), 500
        
        # Get weather data based on parameters
        if api_source:
            weather_data = db.get_weather_by_api(lat, lon, api_source, limit)
        else:
            weather_data = db.get_latest_weather(lat, lon, limit)
        
        return jsonify({
            "status": "success",
            "location": f"Location at {lat}, {lon}",
            "coordinates": f"{lat}, {lon}",
            "limit": limit,
            "api_filter": api_source if api_source else "all",
            "count": len(weather_data),
            "data": weather_data
        })
        
    except ValueError:
        return jsonify({
            "error": "Invalid parameters",
            "message": "lat, lon, and limit must be numbers"
        }), 400
    except Exception as e:
        return jsonify({
            "error": "Failed to get stored weather data",
            "message": str(e)
        }), 500

# Run the app
if __name__ == '__main__':
    # Load coordinates and hours from environment variables with fallback defaults
    default_lat = os.getenv('DEFAULT_LAT', "1.2915")  # Singapore latitude
    default_lon = os.getenv('DEFAULT_LON', "103.8585")  # Singapore longitude
    default_hours = os.getenv('DEFAULT_HOURS', "3")  # Default forecast hours
    
    print("🌤️  Starting Singapore Weather API with 3 Sources + Database...")
    print("📍 Available endpoints:")
    print(f"   GET /weather (uses default coordinates: {default_lat}, {default_lon}, hours: {default_hours})")
    print(f"   GET /weather?lat={default_lat}&lon={default_lon}&hours={default_hours}")
    print("   GET /database/stats")
    print("   GET /database/weather")
    print("   GET /health")
    print("   GET /")
    app.run(debug=True, host='0.0.0.0', port=8001)