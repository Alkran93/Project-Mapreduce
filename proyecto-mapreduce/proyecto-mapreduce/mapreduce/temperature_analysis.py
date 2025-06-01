#!/usr/bin/env python3
"""
MapReduce Job para análisis de temperaturas por ciudad y mes
Usando MRJob para compatibilidad con Hadoop
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from datetime import datetime
import json

class TemperatureAnalysis(MRJob):
    """
    Analiza temperaturas por ciudad y mes
    Calcula: promedio, máxima, mínima por ciudad-mes
    """
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_parse_data,
                reducer=self.reducer_aggregate_temps
            ),
            MRStep(
                mapper=self.mapper_format_output,
                reducer=self.reducer_final_output
            )
        ]
    
    def mapper_parse_data(self, _, line):
        """
        Parsea cada línea del CSV y emite datos de temperatura
        Key: (ciudad, año, mes)
        Value: (temp_max, temp_min, temp_mean)
        """
        try:
            # Saltar header
            if line.startswith('time,temperature'):
                return
            
            # Parsear CSV
            reader = csv.reader([line])
            row = next(reader)
            
            if len(row) < 8:  # Verificar que tenga todas las columnas
                return
            
            # Extraer campos
            date_str = row[0]  # time
            temp_max = float(row[1]) if row[1] else None  # temperature_2m_max
            temp_min = float(row[2]) if row[2] else None  # temperature_2m_min
            temp_mean = float(row[3]) if row[3] else None  # temperature_2m_mean
            city = row[7]  # city
            
            # Validar datos
            if not all([temp_max is not None, temp_min is not None, temp_mean is not None]):
                return
            
            # Parsear fecha
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            year = date_obj.year
            month = date_obj.month
            
            # Emitir key-value
            key = f"{city}_{year}_{month:02d}"
            value = {
                'temp_max': temp_max,
                'temp_min': temp_min,
                'temp_mean': temp_mean,
                'count': 1
            }
            
            yield key, json.dumps(value)
            
        except Exception as e:
            # Log error pero continuar procesando
            yield "ERROR", f"Error processing line: {str(e)}"
    
    def reducer_aggregate_temps(self, key, values):
        """
        Agrega temperaturas por ciudad-mes
        Calcula estadísticas agregadas
        """
        if key == "ERROR":
            return  # Ignorar errores
        
        temp_max_list = []
        temp_min_list = []
        temp_mean_list = []
        total_days = 0
        
        for value_str in values:
            try:
                value = json.loads(value_str)
                temp_max_list.append(value['temp_max'])
                temp_min_list.append(value['temp_min'])
                temp_mean_list.append(value['temp_mean'])
                total_days += value['count']
            except:
                continue
        
        if not temp_max_list:
            return
        
        # Calcular estadísticas
        stats = {
            'city_year_month': key,
            'avg_temp_max': round(sum(temp_max_list) / len(temp_max_list), 2),
            'avg_temp_min': round(sum(temp_min_list) / len(temp_min_list), 2),
            'avg_temp_mean': round(sum(temp_mean_list) / len(temp_mean_list), 2),
            'max_temp_recorded': round(max(temp_max_list), 2),
            'min_temp_recorded': round(min(temp_min_list), 2),
            'total_days': total_days
        }
        
        yield key, json.dumps(stats)
    
    def mapper_format_output(self, key, value):
        """
        Formatea salida para presentación final
        """
        if key == "ERROR":
            return
        
        try:
            stats = json.loads(value)
            city, year, month = key.split('_')
            
            # Crear salida formateada
            output = {
                'city': city,
                'year': int(year),
                'month': int(month),
                'month_name': datetime(int(year), int(month), 1).strftime('%B'),
                **stats
            }
            
            yield f"{city}_{year}", json.dumps(output)
            
        except Exception as e:
            yield "ERROR", f"Format error: {str(e)}"
    
    def reducer_final_output(self, key, values):
        """
        Produce la salida final organizada por ciudad y año
        """
        if key == "ERROR":
            return
        
        city_data = []
        for value_str in values:
            try:
                data = json.loads(value_str)
                city_data.append(data)
            except:
                continue
        
        # Ordenar por mes
        city_data.sort(key=lambda x: x['month'])
        
        # Emitir cada registro
        for data in city_data:
            # Formato CSV para salida
            csv_line = f"{data['city']},{data['year']},{data['month']:02d},{data['month_name']}," \
                      f"{data['avg_temp_max']},{data['avg_temp_min']},{data['avg_temp_mean']}," \
                      f"{data['max_temp_recorded']},{data['min_temp_recorded']},{data['total_days']}"
            
            yield None, csv_line

if __name__ == '__main__':
    TemperatureAnalysis.run()