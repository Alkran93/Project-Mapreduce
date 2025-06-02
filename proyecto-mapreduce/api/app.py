#!/usr/bin/env python3
"""
Flask API to serve results of MapReduce analysis of climate data
EAFIT University - ST0263 - MapReduce Hadoop Project
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import os
import json
from datetime import date, datetime
import io

app = Flask(__name__)
CORS(app)  # Enable CORS for development

# Configuration
PROCESSED_DATA_DIR = "../data/processed"
RAW_DATA_DIR = "../data/raw"

# Verify files exist
TEMPERATURE_FILE = os.path.join(PROCESSED_DATA_DIR, "temperature_results.csv")
PRECIPITATION_FILE = os.path.join(PROCESSED_DATA_DIR, "precipitation_results.csv")
RAW_DATA_FILE = os.path.join(RAW_DATA_DIR, "weather_data.csv")

def load_data():
    """Load processed data with path logs and existence checks"""
    data = {}
    
    try:
        if os.path.exists(TEMPERATURE_FILE):
            df_temp = pd.read_csv(TEMPERATURE_FILE, encoding='utf-16', header=None)
            if df_temp.shape[1] == 1:
                df_temp = df_temp[0].str.split(",", expand=True)
            df_temp.columns = [
                "city", "year", "number_month", "month",
                "avg_max_temp", "avg_min_temp", "avg_mean_temp",
                "max_temp", "min_temp", "days_recorded"
            ]
            # Convert columns to numeric
            numeric_cols_temp = [
                "avg_max_temp", "avg_min_temp", "avg_mean_temp",
                "max_temp", "min_temp"
            ]
            for col in numeric_cols_temp:
                df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce')
 
            data['temperature'] = df_temp
        else:
            data['temperature'] = pd.DataFrame()
    except Exception as e:
        print(f"Error loading temperature data: {e}")
        data['temperature'] = pd.DataFrame()
 
    try:
        if os.path.exists(PRECIPITATION_FILE):
            df_precip = pd.read_csv(PRECIPITATION_FILE, encoding='utf-16', header=None)
            if df_precip.shape[1] == 1:
                df_precip = df_precip[0].str.split(",", expand=True)
            df_precip.columns = [
                "city", "year", "season",
                "total_precipitation", "avg_monthly_precipitation",
                "max_monthly_precipitation", "total_rainy_days",
                "months_in_season"
            ]
            numeric_cols_precip = [
                "total_precipitation", "avg_monthly_precipitation",
                "max_monthly_precipitation", "total_rainy_days"
            ]
            for col in numeric_cols_precip:
                df_precip[col] = pd.to_numeric(df_precip[col], errors='coerce')
 
            data['precipitation'] = df_precip
        else:
            data['precipitation'] = pd.DataFrame()
    except Exception as e:
        print(f"Error loading precipitation data: {e}")
        data['precipitation'] = pd.DataFrame()
 
    try:
        if os.path.exists(RAW_DATA_FILE):
            data['raw'] = pd.read_csv(RAW_DATA_FILE)
        else:
            data['raw'] = pd.DataFrame()
    except Exception as e:
        print(f"Error loading raw data: {e}")
        data['raw'] = pd.DataFrame()
    
    return data
 
@app.route('/')
def home():
    """Main endpoint with API info"""
    return jsonify({
        "message": "MapReduce Climate Analysis API",
        "author": "Sofia Zapata",
        "description": "API to query MapReduce analysis results of climate data",
        "endpoints": {
                        "1.""/": "API information", 
                        "2.""/health": "API and files status",
                        "3.""/data/temperature": "Temperature analysis",
                        "4.""/data/precipitation": "Precipitation analysis",
                        "5.""/data/summary": "Summary of all analyses",
                        "6.""/data/cities": "List of analyzed cities",
                        "7.""/download/temperature": "Download temperature CSV",
                        "8.""/download/precipitation": "Download precipitation CSV",
                        "9.""/stats": "General statistics"
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/health')
def health():
    """Check API and file status"""
    data = load_data()

    status = {
        "api_status": "online",
        "timestamp": datetime.now().isoformat(),
        "files": {
            "temperature_results": {
                "exists": not data['temperature'].empty,
                "records": len(data['temperature']) if not data['temperature'].empty else 0,
                "path": TEMPERATURE_FILE
            },
            "precipitation_results": {
                "exists": not data['precipitation'].empty,
                "records": len(data['precipitation']) if not data['precipitation'].empty else 0,
                "path": PRECIPITATION_FILE
            },
            "raw_data": {
                "exists": not data['raw'].empty,
                "records": len(data['raw']) if not data['raw'].empty else 0,
                "path": RAW_DATA_FILE
            }
        }
    }

    return jsonify(status)

@app.route('/data/temperature')
def get_temperature_data():
    data = load_data()
    if data['temperature'].empty:
        return jsonify({"error": "No temperature data available"}), 404
 
    city = request.args.get('city')
    year = request.args.get('year')
    limit = request.args.get('limit', type=int)
 
    if year:
        try:
            year = int(year)
        except ValueError:
            return jsonify({"error": "Invalid year parameter"}), 400
 
    if limit is not None and limit <= 0:
        return jsonify({"error": "Limit parameter must be a positive integer"}), 400
 
    df = data['temperature'].copy()
 
    if city:
        df = df[df['city'].str.contains(city, case=False, na=False)]
    if year:
        df = df[df['year'] == year]
    if limit:
        df = df.head(limit)
 
    result = {
        "data": df.to_dict('records'),
        "metadata": {
            "total_records": len(df),
            "cities": df['city'].unique().tolist() if not df.empty else [],
            "years": sorted(df['year'].unique().tolist()) if not df.empty else [],
            "filters_applied": {
                "city": city,
                "year": year,
                "limit": limit
            }
        }
    }
    return jsonify(result)

@app.route('/data/precipitation')
def get_precipitation_data():
    data = load_data()
    if data['precipitation'].empty:
        return jsonify({"error": "No precipitation data available"}), 404
 
    city = request.args.get('city')
    year = request.args.get('year')
    season = request.args.get('season')
    limit = request.args.get('limit', type=int)
 
    if year:
        try:
            year = int(year)
        except ValueError:
            return jsonify({"error": "Invalid year parameter"}), 400
 
    if limit is not None and limit <= 0:
        return jsonify({"error": "Limit parameter must be a positive integer"}), 400
 
    df = data['precipitation'].copy()
 
    if city:
        df = df[df['city'].str.contains(city, case=False, na=False)]
    if year:
        df = df[df['year'] == year]
    if season:
        df = df[df['season'].str.contains(season, case=False, na=False)]
    if limit:
        df = df.head(limit)
 
    result = {
        "data": df.to_dict('records'),
        "metadata": {
            "total_records": len(df),
            "cities": df['city'].unique().tolist() if not df.empty else [],
            "years": sorted(df['year'].unique().tolist()) if not df.empty else [],
            "seasons": df['season'].unique().tolist() if not df.empty else [],
            "filters_applied": {
                "city": city,
                "year": year,
                "season": season,
                "limit": limit
            }
        }
    }
    return jsonify(result)

@app.route('/data/summary')
def get_summary():

    data = load_data()

    summary = {

        "timestamp": datetime.now().isoformat(),

        "temperature_analysis": {},

        "precipitation_analysis": {},

        "raw_data_info": {}

    }
 
    if not data['temperature'].empty:

        temp_df = data['temperature']

        try:

            avg_max = pd.to_numeric(temp_df['avg_max_temp'], errors='coerce').mean()

            avg_min = pd.to_numeric(temp_df['avg_min_temp'], errors='coerce').mean()

            highest = pd.to_numeric(temp_df['max_temp'], errors='coerce').max()

            lowest = pd.to_numeric(temp_df['min_temp'], errors='coerce').min()

        except Exception:

            avg_max = avg_min = highest = lowest = None
 
        summary["temperature_analysis"] = {

            "total_records": len(temp_df),

            "cities": temp_df['city'].unique().tolist(),

            "years": sorted(temp_df['year'].unique().tolist()),

            "temperature_stats": {

                "avg_max_temp": round(avg_max, 2) if avg_max is not None else None,

                "avg_min_temp": round(avg_min, 2) if avg_min is not None else None,

                "highest_temp_recorded": round(highest, 2) if highest is not None else None,

                "lowest_temp_recorded": round(lowest, 2) if lowest is not None else None

            }

        }
 
    if not data['precipitation'].empty:

        precip_df = data['precipitation']

        try:

            avg_precip = pd.to_numeric(precip_df['total_precipitation'], errors='coerce').mean()

            max_precip = pd.to_numeric(precip_df['total_precipitation'], errors='coerce').max()

            avg_rainy = pd.to_numeric(precip_df['total_rainy_days'], errors='coerce').mean()

        except Exception:

            avg_precip = max_precip = avg_rainy = None
 
        summary["precipitation_analysis"] = {

            "total_records": len(precip_df),

            "cities": precip_df['city'].unique().tolist(),

            "years": sorted(precip_df['year'].unique().tolist()),

            "seasons": precip_df['season'].unique().tolist(),

            "precipitation_stats": {

                "avg_seasonal_precipitation": round(avg_precip, 2) if avg_precip is not None else None,

                "max_seasonal_precipitation": round(max_precip, 2) if max_precip is not None else None,

                "avg_rainy_days": round(avg_rainy, 2) if avg_rainy is not None else None

            }

        }
 
    if not data['raw'].empty:

        raw_df = data['raw']

        summary["raw_data_info"] = {

            "total_records": len(raw_df),

            "date_range": {

                "start": raw_df['date'].min() if 'date' in raw_df.columns else "N/A",

                "end": raw_df['date'].max() if 'date' in raw_df.columns else "N/A"

            },

            "cities": raw_df['city'].unique().tolist() if 'city' in raw_df.columns else []

        }
 
    return jsonify(summary)
 

@app.route('/data/cities')
def get_cities():
    data = load_data()
    cities_info = {}
 
    if not data['temperature'].empty:
        temp_df = data['temperature']
        city_col = 'city'
        for city in temp_df[city_col].unique():
            city_data = temp_df[temp_df[city_col] == city]
            try:
                avg_temp = pd.to_numeric(city_data['avg_mean_temp'], errors='coerce').mean()
                max_temp = pd.to_numeric(city_data['max_temp'], errors='coerce').max()
                min_temp = pd.to_numeric(city_data['min_temp'], errors='coerce').min()
            except Exception:
                avg_temp = max_temp = min_temp = None
 
            cities_info[city] = {
                "temperature_records": len(city_data),
                "years_analyzed": sorted(city_data['year'].unique().tolist()),
                "avg_temperature": round(avg_temp, 2) if avg_temp is not None else None,
                "max_temp_recorded": round(max_temp, 2) if max_temp is not None else None,
                "min_temp_recorded": round(min_temp, 2) if min_temp is not None else None
            }
 
    if not data['precipitation'].empty:
        precip_df = data['precipitation']
        for city in precip_df['city'].unique():
            city_data = precip_df[precip_df['city'] == city]
 
            try:
                avg_precip = pd.to_numeric(city_data['total_precipitation'], errors='coerce').mean()
                max_precip = pd.to_numeric(city_data['total_precipitation'], errors='coerce').max()
                avg_rainy_days = pd.to_numeric(city_data['total_rainy_days'], errors='coerce').mean()
            except Exception:
                avg_precip = max_precip = avg_rainy_days = None
 
            if city not in cities_info:
                cities_info[city] = {}
 
            cities_info[city].update({
                "precipitation_records": len(city_data),
                "seasons_analyzed": city_data['season'].unique().tolist(),
                "avg_seasonal_precipitation": round(avg_precip, 2) if avg_precip is not None else None,
                "max_seasonal_precipitation": round(max_precip, 2) if max_precip is not None else None,
                "avg_rainy_days_per_season": round(avg_rainy_days, 2) if avg_rainy_days is not None else None
            })
 
    return jsonify({
        "cities": cities_info,
        "total_cities": len(cities_info),
        "available_cities": list(cities_info.keys())
    })
  
@app.route('/download/temperature')
def download_temperature():
    """Download temperature analysis CSV"""
    if not os.path.exists(TEMPERATURE_FILE):
        return jsonify({"error": "Temperature file not found"}), 404

    return send_file(
        TEMPERATURE_FILE,
        as_attachment=True,
        download_name=f"temperature_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        mimetype='text/csv'
    )

@app.route('/download/precipitation')
def download_precipitation():
    """Download precipitation analysis CSV"""
    if not os.path.exists(PRECIPITATION_FILE):
        return jsonify({"error": "Precipitation file not found"}), 404

    return send_file(
        PRECIPITATION_FILE,
        as_attachment=True,
        download_name=f"precipitation_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        mimetype='text/csv'
    )

@app.route('/stats')
def get_stats():
    """General project statistics"""
    data = load_data()

    stats = {
        "project_info": {
            "name": "MapReduce Climate Analysis",
            "course": "ST0263 - Special Topics in Telematics",
            "university": "EAFIT University",
            "technology": "Hadoop MapReduce + Python MRJob"
        },
        "data_processing": {
            "mapreduce_jobs": 2,
            "jobs_executed": [
                "Temperature Analysis",
                "Precipitation Analysis"
            ]
        },
        "file_stats": {},
        "api_stats": {
            "endpoints": 10,
            "status": "active",
            "last_updated": datetime.now().isoformat()
        }
    }

    # File statistics
    for name, df in data.items():
        if not df.empty:
            stats["file_stats"][name] = {
                "records": len(df),
                "columns": len(df.columns),
                "size_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            }

    return jsonify(stats)

@app.route('/data/temperature/by-city/<city>')
def get_temperature_by_city(city):
    """Get temperature data for a specific city"""
    data = load_data()

    if data['temperature'].empty:
        return jsonify({"error": "No temperature data available"}), 404

    city_data = data['temperature'][
        data['temperature']['city'].str.contains(city, case=False, na=False)
    ]

    if city_data.empty:
        return jsonify({"error": f"No data found for city: {city}"}), 404

    # Organize data by year and month
    result = {
        "city": city,
        "data": city_data.to_dict('records'),
        "summary": {
            "total_months": len(city_data),
            "years_covered": sorted(city_data['year'].unique().tolist()),
            "temperature_range": {
                "min": round(city_data['min_temp_recorded'].min(), 2),
                "max": round(city_data['max_temp_recorded'].max(), 2),
                "avg": round(city_data['avg_temp_mean'].mean(), 2)
            }
        }
    }

    return jsonify(result)

@app.route('/data/precipitation/by-city/<city>')
def get_precipitation_by_city(city):
    """Get precipitation data for a specific city"""
    data = load_data()

    if data['precipitation'].empty:
        return jsonify({"error": "No precipitation data available"}), 404

    city_data = data['precipitation'][
        data['precipitation']['city'].str.contains(city, case=False, na=False)
    ]

    if city_data.empty:
        return jsonify({"error": f"No data found for city: {city}"}), 404

    result = {
        "city": city,
        "data": city_data.to_dict('records'),
        "summary": {
            "total_seasons": len(city_data),
            "years_covered": sorted(city_data['year'].unique().tolist()),
            "precipitation_summary": {
                "total_annual_avg": round(city_data['total_seasonal_precipitation'].sum() / len(city_data['year'].unique()), 2),
                "wettest_season": city_data.loc[city_data['total_seasonal_precipitation'].idxmax(), 'season'],
                "avg_rainy_days": round(city_data['total_rainy_days'].mean(), 2)
            }
        }
    }

    return jsonify(result)

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Endpoint not found",
        "message": "Query / to see available endpoints"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Internal server error",
        "message": "Contact administrator"
    }), 500

if __name__ == '__main__':
    os.makedirs(os.path.dirname(TEMPERATURE_FILE), exist_ok=True)

    print("Starting MapReduce Climate Analysis API")
    print("=" * 50)
    print(f"Processed data directory: {PROCESSED_DATA_DIR}")
    print(f"Temperature file: {TEMPERATURE_FILE}")
    print(f"Precipitation file: {PRECIPITATION_FILE}")
    print("")
    print("API running at: http://localhost:5000")
    print("Main endpoints:")
    print("  • /                     - API information")
    print("  • /health               - API status")
    print("  • /data/temperature     - Temperature analysis")
    print("  • /data/precipitation   - Precipitation analysis")
    print("  • /data/summary         - Full summary")
    print("  • /data/cities          - Cities information")
    print("=" * 50)
    

    app.run(debug=True, host='0.0.0.0', port=5000)
