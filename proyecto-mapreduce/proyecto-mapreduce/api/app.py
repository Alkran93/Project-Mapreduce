#!/usr/bin/env python3
"""
API Flask para servir resultados del análisis MapReduce de datos climáticos
Universidad EAFIT - ST0263 - Proyecto MapReduce Hadoop
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import os
import json
from datetime import datetime
import io

app = Flask(__name__)
CORS(app)  # Permitir CORS para desarrollo

# Configuración
PROCESSED_DATA_DIR = "../data/processed"
RAW_DATA_DIR = "../data/raw"

# Verificar que los archivos existen
TEMPERATURE_FILE = os.path.join(PROCESSED_DATA_DIR, "temperature_results.csv")
PRECIPITATION_FILE = os.path.join(PROCESSED_DATA_DIR, "precipitation_results.csv")
RAW_DATA_FILE = os.path.join(RAW_DATA_DIR, "weather_data.csv")

def load_data():
    """Cargar datos procesados"""
    data = {}
    
    try:
        if os.path.exists(TEMPERATURE_FILE):
            data['temperature'] = pd.read_csv(TEMPERATURE_FILE)
        else:
            data['temperature'] = pd.DataFrame()
            
        if os.path.exists(PRECIPITATION_FILE):
            data['precipitation'] = pd.read_csv(PRECIPITATION_FILE)
        else:
            data['precipitation'] = pd.DataFrame()
            
        if os.path.exists(RAW_DATA_FILE):
            data['raw'] = pd.read_csv(RAW_DATA_FILE)
        else:
            data['raw'] = pd.DataFrame()
            
    except Exception as e:
        print(f"Error cargando datos: {e}")
        
    return data

@app.route('/')
def home():
    """Endpoint principal con información de la API"""
    return jsonify({
        "message": "API de Análisis Climático MapReduce",
        "project": "ST0263 - Universidad EAFIT",
        "description": "API para consultar resultados de análisis MapReduce de datos climáticos",
        "endpoints": {
            "/": "Información de la API",
            "/health": "Estado de la API y archivos",
            "/data/temperature": "Análisis de temperaturas",
            "/data/precipitation": "Análisis de precipitaciones",
            "/data/summary": "Resumen de todos los análisis",
            "/data/cities": "Lista de ciudades analizadas",
            "/download/temperature": "Descargar CSV de temperaturas",
            "/download/precipitation": "Descargar CSV de precipitaciones",
            "/stats": "Estadísticas generales"
        },
        "timestamp": datetime.now().isoformat()
    })

@app.route('/health')
def health():
    """Verificar estado de la API y archivos"""
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
    """Obtener análisis de temperaturas"""
    data = load_data()
    
    if data['temperature'].empty:
        return jsonify({"error": "No hay datos de temperatura disponibles"}), 404
    
    # Parámetros opcionales
    city = request.args.get('city')
    year = request.args.get('year')
    limit = request.args.get('limit', type=int)
    
    df = data['temperature'].copy()
    
    # Filtros
    if city:
        df = df[df['city'].str.contains(city, case=False, na=False)]
    if year:
        df = df[df['year'] == int(year)]
    if limit:
        df = df.head(limit)
    
    # Convertir a formato JSON
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
    """Obtener análisis de precipitaciones"""
    data = load_data()
    
    if data['precipitation'].empty:
        return jsonify({"error": "No hay datos de precipitación disponibles"}), 404
    
    # Parámetros opcionales
    city = request.args.get('city')
    year = request.args.get('year')
    season = request.args.get('season')
    limit = request.args.get('limit', type=int)
    
    df = data['precipitation'].copy()
    
    # Filtros
    if city:
        df = df[df['city'].str.contains(city, case=False, na=False)]
    if year:
        df = df[df['year'] == int(year)]
    if season:
        df = df[df['season'].str.contains(season, case=False, na=False)]
    if limit:
        df = df.head(limit)
    
    # Convertir a formato JSON
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
    """Resumen de todos los análisis"""
    data = load_data()
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "temperature_analysis": {},
        "precipitation_analysis": {},
        "raw_data_info": {}
    }
    
    # Resumen de temperaturas
    if not data['temperature'].empty:
        temp_df = data['temperature']
        summary["temperature_analysis"] = {
            "total_records": len(temp_df),
            "cities": temp_df['city'].unique().tolist(),
            "years": sorted(temp_df['year'].unique().tolist()),
            "temperature_stats": {
                "avg_max_temp": round(temp_df['avg_temp_max'].mean(), 2),
                "avg_min_temp": round(temp_df['avg_temp_min'].mean(), 2),
                "highest_temp_recorded": round(temp_df['max_temp_recorded'].max(), 2),
                "lowest_temp_recorded": round(temp_df['min_temp_recorded'].min(), 2)
            }
        }
    
    # Resumen de precipitaciones
    if not data['precipitation'].empty:
        precip_df = data['precipitation']
        summary["precipitation_analysis"] = {
            "total_records": len(precip_df),
            "cities": precip_df['city'].unique().tolist(),
            "years": sorted(precip_df['year'].unique().tolist()),
            "seasons": precip_df['season'].unique().tolist(),
            "precipitation_stats": {
                "avg_seasonal_precipitation": round(precip_df['total_seasonal_precipitation'].mean(), 2),
                "max_seasonal_precipitation": round(precip_df['total_seasonal_precipitation'].max(), 2),
                "avg_rainy_days": round(precip_df['total_rainy_days'].mean(), 2)
            }
        }
    
    # Información de datos crudos
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
    """Obtener lista de ciudades con estadísticas"""
    data = load_data()
    
    cities_info = {}
    
    # Información de ciudades desde datos de temperatura
    if not data['temperature'].empty:
        temp_df = data['temperature']
        for city in temp_df['city'].unique():
            city_data = temp_df[temp_df['city'] == city]
            cities_info[city] = {
                "temperature_records": len(city_data),
                "years_analyzed": sorted(city_data['year'].unique().tolist()),
                "avg_temperature": round(city_data['avg_temp_mean'].mean(), 2),
                "max_temp_recorded": round(city_data['max_temp_recorded'].max(), 2),
                "min_temp_recorded": round(city_data['min_temp_recorded'].min(), 2)
            }
    
    # Agregar información de precipitaciones
    if not data['precipitation'].empty:
        precip_df = data['precipitation']
        for city in precip_df['city'].unique():
            city_data = precip_df[precip_df['city'] == city]
            if city not in cities_info:
                cities_info[city] = {}
            
            cities_info[city].update({
                "precipitation_records": len(city_data),
                "seasons_analyzed": city_data['season'].unique().tolist(),
                "avg_seasonal_precipitation": round(city_data['total_seasonal_precipitation'].mean(), 2),
                "max_seasonal_precipitation": round(city_data['total_seasonal_precipitation'].max(), 2),
                "avg_rainy_days_per_season": round(city_data['total_rainy_days'].mean(), 2)
            })
    
    return jsonify({
        "cities": cities_info,
        "total_cities": len(cities_info),
        "available_cities": list(cities_info.keys())
    })

@app.route('/download/temperature')
def download_temperature():
    """Descargar CSV de análisis de temperaturas"""
    if not os.path.exists(TEMPERATURE_FILE):
        return jsonify({"error": "Archivo de temperaturas no encontrado"}), 404
    
    return send_file(
        TEMPERATURE_FILE,
        as_attachment=True,
        download_name=f"temperature_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        mimetype='text/csv'
    )

@app.route('/download/precipitation')
def download_precipitation():
    """Descargar CSV de análisis de precipitaciones"""
    if not os.path.exists(PRECIPITATION_FILE):
        return jsonify({"error": "Archivo de precipitaciones no encontrado"}), 404
    
    return send_file(
        PRECIPITATION_FILE,
        as_attachment=True,
        download_name=f"precipitation_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        mimetype='text/csv'
    )

@app.route('/stats')
def get_stats():
    """Estadísticas generales del proyecto"""
    data = load_data()
    
    stats = {
        "project_info": {
            "name": "Análisis Climático MapReduce",
            "course": "ST0263 - Tópicos Especiales en Telemática",
            "university": "Universidad EAFIT",
            "technology": "Hadoop MapReduce + Python MRJob"
        },
        "data_processing": {
            "mapreduce_jobs": 2,
            "jobs_executed": [
                "Análisis de Temperaturas",
                "Análisis de Precipitaciones"
            ]
        },
        "file_stats": {},
        "api_stats": {
            "endpoints": 10,
            "status": "active",
            "last_updated": datetime.now().isoformat()
        }
    }
    
    # Estadísticas de archivos
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
    """Obtener datos de temperatura para una ciudad específica"""
    data = load_data()
    
    if data['temperature'].empty:
        return jsonify({"error": "No hay datos de temperatura disponibles"}), 404
    
    city_data = data['temperature'][
        data['temperature']['city'].str.contains(city, case=False, na=False)
    ]
    
    if city_data.empty:
        return jsonify({"error": f"No se encontraron datos para la ciudad: {city}"}), 404
    
    # Organizar datos por año y mes
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
    """Obtener datos de precipitación para una ciudad específica"""
    data = load_data()
    
    if data['precipitation'].empty:
        return jsonify({"error": "No hay datos de precipitación disponibles"}), 404
    
    city_data = data['precipitation'][
        data['precipitation']['city'].str.contains(city, case=False, na=False)
    ]
    
    if city_data.empty:
        return jsonify({"error": f"No se encontraron datos para la ciudad: {city}"}), 404
    
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
        "error": "Endpoint no encontrado",
        "message": "Consulta / para ver los endpoints disponibles"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "error": "Error interno del servidor",
        "message": "Contacta al administrador"
    }), 500

if __name__ == '__main__':
    # Verificar que los directorios existen
    os.makedirs(os.path.dirname(TEMPERATURE_FILE), exist_ok=True)
    
    print("🌟 Iniciando API de Análisis Climático MapReduce")
    print("=" * 50)
    print(f"📁 Directorio de datos procesados: {PROCESSED_DATA_DIR}")
    print(f"📄 Archivo de temperaturas: {TEMPERATURE_FILE}")
    print(f"📄 Archivo de precipitaciones: {PRECIPITATION_FILE}")
    print("")
    print("🌐 API corriendo en: http://localhost:5000")
    print("📋 Endpoints principales:")
    print("  • /                     - Información de la API")  
    print("  • /health               - Estado de la API")
    print("  • /data/temperature     - Análisis de temperaturas")
    print("  • /data/precipitation   - Análisis de precipitaciones")
    print("  • /data/summary         - Resumen completo")
    print("  • /data/cities          - Información de ciudades")
    print("=" * 50)
    
    app.run(debug=True, host='0.0.0.0', port=5000)