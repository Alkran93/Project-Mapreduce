#!/usr/bin/env python3
"""
Script para descargar datos climáticos de Open-Meteo
Descarga datos históricos de múltiples ciudades colombianas
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import os

# Ciudades colombianas principales
CITIES = {
    'Medellín': {'lat': 6.2518, 'lon': -75.5636},
    'Bogotá': {'lat': 4.7110, 'lon': -74.0721},
    'Cali': {'lat': 3.4516, 'lon': -76.5320},
    'Barranquilla': {'lat': 10.9639, 'lon': -74.7964},
    'Cartagena': {'lat': 10.3910, 'lon': -75.4794}
}

def download_weather_data(city_name, lat, lon, start_date='2023-01-01', end_date='2024-12-31'):
    """Descarga datos meteorológicos para una ciudad específica"""
    
    url = 'https://archive-api.open-meteo.com/v1/archive'
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date,
        'end_date': end_date,
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'windspeed_10m_max',
            'sunshine_duration'
        ],
        'timezone': 'America/Bogota'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convertir a DataFrame
        df = pd.DataFrame(data['daily'])
        df['city'] = city_name
        df['latitude'] = lat
        df['longitude'] = lon
        
        return df
    
    except Exception as e:
        print(f"Error descargando datos para {city_name}: {e}")
        return None

def main():
    """Función principal para descargar todos los datos"""
    
    # Crear directorio si no existe
    os.makedirs('data/raw', exist_ok=True)
    
    all_data = []
    
    print("Iniciando descarga de datos climáticos...")
    
    for city_name, coords in CITIES.items():
        print(f"Descargando datos para {city_name}...")
        
        city_data = download_weather_data(
            city_name, 
            coords['lat'], 
            coords['lon']
        )
        
        if city_data is not None:
            all_data.append(city_data)
            print(f"✓ {city_name}: {len(city_data)} registros")
        else:
            print(f"✗ Error en {city_name}")
    
    if all_data:
        # Combinar todos los datos
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Guardar como CSV
        output_file = 'data/raw/weather_data.csv'
        combined_df.to_csv(output_file, index=False)
        
        print(f"\n✓ Datos guardados en {output_file}")
        print(f"Total de registros: {len(combined_df)}")
        print(f"Período: {combined_df['time'].min()} a {combined_df['time'].max()}")
        print(f"Ciudades: {', '.join(combined_df['city'].unique())}")
        
        # Guardar resumen
        summary = {
            'total_records': len(combined_df),
            'cities': list(combined_df['city'].unique()),
            'date_range': {
                'start': combined_df['time'].min(),
                'end': combined_df['time'].max()
            },
            'variables': [col for col in combined_df.columns if col not in ['city', 'latitude', 'longitude', 'time']]
        }
        
        with open('data/raw/data_summary.json', 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print("✓ Resumen guardado en data/raw/data_summary.json")
    
    else:
        print("✗ No se pudieron descargar datos")

if __name__ == "__main__":
    # Instalar dependencias si es necesario
    try:
        import pandas
        import requests
    except ImportError:
        print("Instalando dependencias...")
        os.system("pip install pandas requests")
        import pandas
        import requests
    
    main()