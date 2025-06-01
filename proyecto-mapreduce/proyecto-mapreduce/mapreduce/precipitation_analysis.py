#!/usr/bin/env python3
"""
MapReduce Job para análisis de precipitaciones
Calcula totales mensuales y patrones estacionales por ciudad
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime
import json

class PrecipitationAnalysis(MRJob):
    """
    Analiza precipitaciones por ciudad, mes y estación
    Calcula totales mensuales y patrones estacionales
    """
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_parse_precipitation,
                reducer=self.reducer_monthly_totals
            ),
            MRStep(
                mapper=self.mapper_add_season,
                reducer=self.reducer_seasonal_analysis
            )
        ]
    
    def get_season(self, month):
        """Determina la estación del año basada en el mes (Colombia)"""
        if month in [12, 1, 2]:
            return "Verano"  # Época seca
        elif month in [3, 4, 5]:
            return "Transición"  # Transición a lluvias
        elif month in [6, 7, 8]:
            return "Invierno"  # Época lluviosa
        else:  # 9, 10, 11
            return "Lluvias_Tardías"  # Lluvias tardías
    
    def mapper_parse_precipitation(self, _, line):
        """
        Parsea datos de precipitación
        Key: (ciudad, año, mes)
        Value: precipitación diaria
        """
        try:
            if line.startswith('time,temperature'):
                return
            
            reader = csv.reader([line])
            row = next(reader)
            
            if len(row) < 8:
                return
            
            date_str = row[0]  # time
            precipitation = float(row[4]) if row[4] else 0.0  # precipitation_sum
            city = row[7]  # city
            
            # Parsear fecha
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            year = date_obj.year
            month = date_obj.month
            
            key = f"{city}_{year}_{month:02d}"
            yield key, precipitation
            
        except Exception as e:
            yield "ERROR", f"Parse error: {str(e)}"
    
    def reducer_monthly_totals(self, key, values):
        """
        Calcula totales mensuales de precipitación
        """
        if key == "ERROR":
            return
        
        daily_values = list(values)
        monthly_total = sum(daily_values)
        days_with_rain = sum(1 for v in daily_values if v > 0)
        max_daily = max(daily_values) if daily_values else 0
        avg_daily = monthly_total / len(daily_values) if daily_values else 0
        
        result = {
            'monthly_total': round(monthly_total, 2),
            'days_with_rain': days_with_rain,
            'max_daily_precipitation': round(max_daily, 2),
            'avg_daily_precipitation': round(avg_daily, 2),
            'total_days': len(daily_values)
        }
        
        yield key, json.dumps(result)
    
    def mapper_add_season(self, key, value):
        """
        Añade información de estación y prepara para análisis estacional
        """
        if key == "ERROR":
            return
        
        try:
            data = json.loads(value)
            city, year, month = key.split('_')
            month_num = int(month)
            season = self.get_season(month_num)
            
            # Key para agrupar por ciudad y estación
            seasonal_key = f"{city}_{season}_{year}"
            
            enhanced_data = {
                'city': city,
                'year': int(year),
                'month': month_num,
                'season': season,
                **data
            }
            
            yield seasonal_key, json.dumps(enhanced_data)
            
        except Exception as e:
            yield "ERROR", f"Season mapping error: {str(e)}"
    
    def reducer_seasonal_analysis(self, key, values):
        """
        Analiza patrones estacionales de precipitación
        """
        if key == "ERROR":
            return
        
        seasonal_data = []
        for value_str in values:
            try:
                data = json.loads(value_str)
                seasonal_data.append(data)
            except:
                continue
        
        if not seasonal_data:
            return
        
        # Calcular estadísticas estacionales
        total_precipitation = sum(d['monthly_total'] for d in seasonal_data)
        total_rainy_days = sum(d['days_with_rain'] for d in seasonal_data)
        max_monthly = max(d['monthly_total'] for d in seasonal_data)
        avg_monthly = total_precipitation / len(seasonal_data)
        
        # Información de la estación
        city = seasonal_data[0]['city']
        year = seasonal_data[0]['year']
        season = seasonal_data[0]['season']
        
        # Crear salida final
        result = {
            'city': city,
            'year': year,
            'season': season,
            'total_seasonal_precipitation': round(total_precipitation, 2),
            'avg_monthly_precipitation': round(avg_monthly, 2),
            'max_monthly_precipitation': round(max_monthly, 2),
            'total_rainy_days': total_rainy_days,
            'months_in_season': len(seasonal_data)
        }
        
        # Formato CSV
        csv_line = f"{result['city']},{result['year']},{result['season']}," \
                  f"{result['total_seasonal_precipitation']},{result['avg_monthly_precipitation']}," \
                  f"{result['max_monthly_precipitation']},{result['total_rainy_days']}," \
                  f"{result['months_in_season']}"
        
        yield None, csv_line

if __name__ == '__main__':
    PrecipitationAnalysis.run()