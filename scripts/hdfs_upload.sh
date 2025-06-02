#!/bin/bash
# Script para cargar datos a HDFS y ejecutar jobs MapReduce
# Requiere Hadoop configurado y corriendo

set -e  # Salir si hay errores

echo "=== HADOOP MAPREDUCE - PROCESAMIENTO CLIMÁTICO ==="
echo ""

# Variables de configuración
INPUT_DIR="/user/$(whoami)/weather_input"
OUTPUT_DIR_TEMP="/user/$(whoami)/weather_temp_output"
OUTPUT_DIR_PRECIP="/user/$(whoami)/weather_precip_output"
LOCAL_DATA="data/raw/weather_data.csv"

# Función para verificar si Hadoop está corriendo
check_hadoop() {
    echo "Verificando estado de Hadoop..."
    if ! jps | grep -q "NameNode\|DataNode"; then
        echo "❌ Hadoop no está corriendo. Iniciando servicios..."
        start-dfs.sh
        start-yarn.sh
        sleep 10
    fi
    echo "✅ Hadoop está corriendo"
    echo ""
}

# Función para crear directorios HDFS
setup_hdfs_dirs() {
    echo "Configurando directorios HDFS..."
    
    # Limpiar directorios existentes
    hadoop fs -rm -r -f $INPUT_DIR 2>/dev/null || true
    hadoop fs -rm -r -f $OUTPUT_DIR_TEMP 2>/dev/null || true
    hadoop fs -rm -r -f $OUTPUT_DIR_PRECIP 2>/dev/null || true
    
    # Crear directorios
    hadoop fs -mkdir -p $INPUT_DIR
    
    echo "✅ Directorios HDFS configurados"
    echo ""
}

# Función para cargar datos
upload_data() {
    echo "Cargando datos a HDFS..."
    
    if [ ! -f "$LOCAL_DATA" ]; then
        echo "❌ Error: No se encuentra el archivo $LOCAL_DATA"
        echo "Ejecuta primero: python scripts/data_download.py"
        exit 1
    fi
    
    # Subir archivo principal
    hadoop fs -put $LOCAL_DATA $INPUT_DIR/
    
    # Verificar carga
    echo "Verificando datos cargados:"
    hadoop fs -ls $INPUT_DIR
    echo ""
    
    # Mostrar primeras líneas
    echo "Muestra de datos cargados:"
    hadoop fs -head $INPUT_DIR/weather_data.csv
    echo ""
    
    echo "✅ Datos cargados exitosamente"
    echo ""
}

# Función para ejecutar job de temperaturas
run_temperature_job() {
    echo "=== EJECUTANDO JOB MAPREDUCE: ANÁLISIS DE TEMPERATURAS ==="
    echo ""
    
    # Instalar mrjob si no está disponible
    pip install mrjob > /dev/null 2>&1 || true
    
    echo "Iniciando procesamiento de temperaturas..."
    python mapreduce/temperature_analysis.py \
        -r hadoop \
        --hadoop-streaming-jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        --output-dir $OUTPUT_DIR_TEMP \
        $INPUT_DIR/weather_data.csv
    
    echo "✅ Job de temperaturas completado"
    echo ""
    
    # Mostrar resultados
    echo "Resultados del análisis de temperaturas:"
    hadoop fs -head $OUTPUT_DIR_TEMP/part-00000
    echo ""
}

# Función para ejecutar job de precipitaciones
run_precipitation_job() {
    echo "=== EJECUTANDO JOB MAPREDUCE: ANÁLISIS DE PRECIPITACIONES ==="
    echo ""
    
    echo "Iniciando procesamiento de precipitaciones..."
    python mapreduce/precipitation_analysis.py \
        -r hadoop \
        --hadoop-streaming-jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        --output-dir $OUTPUT_DIR_PRECIP \
        $INPUT_DIR/weather_data.csv
    
    echo "✅ Job de precipitaciones completado"
    echo ""
    
    # Mostrar resultados
    echo "Resultados del análisis de precipitaciones:"
    hadoop fs -head $OUTPUT_DIR_PRECIP/part-00000
    echo ""
}

# Función para descargar resultados
download_results() {
    echo "=== DESCARGANDO RESULTADOS ==="
    echo ""
    
    # Crear directorio local
    mkdir -p data/processed
    
    # Descargar resultados de temperaturas
    echo "Descargando resultados de temperaturas..."
    hadoop fs -get $OUTPUT_DIR_TEMP/part-00000 data/processed/temperature_analysis.csv
    
    # Añadir header al CSV de temperaturas
    echo "city,year,month,month_name,avg_temp_max,avg_temp_min,avg_temp_mean,max_temp_recorded,min_temp_recorded,total_days" > data/processed/temperature_results.csv
    cat data/processed/temperature_analysis.csv >> data/processed/temperature_results.csv
    
    # Descargar resultados de precipitaciones
    echo "Descargando resultados de precipitaciones..."
    hadoop fs -get $OUTPUT_DIR_PRECIP/part-00000 data/processed/precipitation_analysis.csv
    
    # Añadir header al CSV de precipitaciones
    echo "city,year,season,total_seasonal_precipitation,avg_monthly_precipitation,max_monthly_precipitation,total_rainy_days,months_in_season" > data/processed/precipitation_results.csv
    cat data/processed/precipitation_analysis.csv >> data/processed/precipitation_results.csv
    
    # Limpiar archivos temporales
    rm -f data/processed/temperature_analysis.csv data/processed/precipitation_analysis.csv
    
    echo "✅ Resultados descargados:"
    echo "  - data/processed/temperature_results.csv"
    echo "  - data/processed/precipitation_results.csv"
    echo ""
    
    # Mostrar estadísticas
    echo "Estadísticas de resultados:"
    echo "Registros de temperaturas: $(wc -l < data/processed/temperature_results.csv)"
    echo "Registros de precipitaciones: $(wc -l < data/processed/precipitation_results.csv)"
    echo ""
}

# Función para mostrar resumen final
show_summary() {
    echo "=== RESUMEN DE PROCESAMIENTO ==="
    echo ""
    
    echo "📊 Archivos generados:"
    ls -la data/processed/
    echo ""
    
    echo "🏙️ Muestra de análisis de temperaturas:"
    head -5 data/processed/temperature_results.csv
    echo ""
    
    echo "🌧️ Muestra de análisis de precipitaciones:"
    head -5 data/processed/precipitation_results.csv
    echo ""
    
    echo "✅ Procesamiento MapReduce completado exitosamente!"
    echo "📁 Los resultados están listos en data/processed/"
    echo ""
}

# Ejecución principal
main() {
    echo "Iniciando pipeline completo de procesamiento..."
    echo ""
    
    check_hadoop
    setup_hdfs_dirs
    upload_data
    run_temperature_job
    run_precipitation_job
    download_results
    show_summary
    
    echo "🎉 PROCESO COMPLETADO - Datos listos para la API"
}

# Ejecutar si se llama directamente
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi