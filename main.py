from flask import Flask, request, jsonify
import os
import sys
import io
import re
from contextlib import redirect_stdout
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo

# Import functions from separate API modules
from weather_api import WeatherAPI
from singapore_nea import SingaporeNEA
from visual_crossing import VisualCrossing

# Import database functions
from database import get_weather_db

# Load environment variables
load_dotenv()

# Create Flask app
app = Flask(__name__)

# API Keys
WEATHERAPI_KEY = os.getenv('WEATHERAPI_KEY')



def get_location_name(lat, lon):
    """Get Singapore NEA area name for coordinates."""
    try:
        # Use Singapore NEA to find the closest area
        sg_nea = SingaporeNEA()
        
        # Get all areas and find the closest one
        areas = sg_nea.get_all_areas()
        if areas:
            closest_area = None
            min_distance = float('inf')
            
            for area in areas:
                area_lat = area['label_location']['latitude']
                area_lon = area['label_location']['longitude']
                
                # Calculate simple distance
                distance = ((lat - area_lat) ** 2 + (lon - area_lon) ** 2) ** 0.5
                
                if distance < min_distance:
                    min_distance = distance
                    closest_area = area['name']
            
            if closest_area:
                return f"{closest_area}, Singapore"
        
        # Fallback to coordinates
        return f"Singapore Area ({lat:.4f}, {lon:.4f})"
        
    except Exception as e:
        print(f"Area lookup error: {e}")
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
    """Fetch weather data from all available APIs using the new class-based approach."""
    results = []
    
    # Initialize API instances
    weather_api = WeatherAPI()
    sg_nea = SingaporeNEA()
    visual_crossing = VisualCrossing()
    
    # List of APIs to fetch from
    apis = [
        ("WeatherAPI", weather_api, lambda: weather_api.fetch_forecast_data(lat, lon, hours)),
        ("Singapore NEA", sg_nea, lambda: sg_nea.fetch_forecast_data(lat, lon, min(hours, 2))),  # NEA max 2 hours
        ("Visual Crossing", visual_crossing, lambda: visual_crossing.fetch_forecast_data(lat, lon, hours))
    ]
    
    for api_name, api_instance, fetch_func in apis:
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
            elif result is not None and 'forecast' in result:
                # Use the structured data from the new API classes
                forecast_data = result['forecast'][:hours]
                
                if forecast_data:
                    results.append({
                        "api": api_name,
                        "current_time": result.get('current_time', ''),
                        "forecast": forecast_data,
                        "location": result.get('location', {}),
                        "area_metadata": result.get('area_metadata', {})  # For Singapore NEA
                    })
                else:
                    results.append({
                        "api": api_name,
                        "error": "No forecast data available"
                    })
            else:
                # Fallback to parsing output if structured data not available
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
        "message": "🌤️ Singapore Weather API - Clean Format with Singapore NEA Areas & Database Storage",
        "version": "5.0",
        "description": "Returns weather data in clean, readable format with Singapore NEA area names and automatic database storage",
        "features": [
            "3 Weather APIs (WeatherAPI, Singapore NEA, Visual Crossing)",
            "Singapore NEA area names for all locations",
            "Clean output format",
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
        "apis": ["WeatherAPI", "Singapore NEA", "Visual Crossing"],
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
        
        # Database upload disabled - manual upload only
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
