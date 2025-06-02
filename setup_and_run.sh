#!/bin/bash
# Script principal para configurar y ejecutar todo el pipeline MapReduce con Docker
# VERSION CORREGIDA PARA WINDOWS

set -e  # Salir si hay errores

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Variables de configuración
PROJECT_DIR=$(pwd)
NAMENODE_CONTAINER="namenode"

# Función para mostrar mensajes con color
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Función para verificar prerrequisitos
check_prerequisites() {
    log_info "Verificando prerrequisitos..."
    
    # Verificar Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado. Instala Docker y vuelve a intentar."
        exit 1
    fi
    
    # Verificar Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose no está instalado."
        exit 1
    fi
    
    # Verificar Python
    if ! command -v python &> /dev/null && ! command -v python &> /dev/null; then
        log_error "Python no está instalado."
        exit 1
    fi
    
    log_success "Todos los prerrequisitos están satisfechos"
}

# Función para verificar estructura del proyecto
check_project_structure() {
    log_info "Verificando estructura del proyecto..."
    
    # Crear directorios si no existen
    mkdir -p data/raw data/processed mapreduce scripts api
    
    # Verificar archivos clave
    if [ ! -f "data/raw/weather_data.csv" ]; then
        log_warning "No se encuentra weather_data.csv. Ejecutando descarga de datos..."
        if [ -f "scripts/data_download.py" ]; then
            python scripts/data_download.py
        else
            log_error "Script de descarga no encontrado. Descarga los datos manualmente."
            exit 1
        fi
    fi
    
    if [ ! -f "mapreduce/temperature_analysis.py" ] || [ ! -f "mapreduce/precipitation_analysis.py" ]; then
        log_error "Jobs MapReduce no encontrados en mapreduce/"
        exit 1
    fi
    
    log_success "Estructura del proyecto verificada"
}

# Función para iniciar cluster Hadoop
start_hadoop_cluster() {
    log_info "Iniciando cluster Hadoop con Docker..."
    
    # Detener contenedores existentes
    docker-compose down 2>/dev/null || true
    
    # Limpiar volúmenes si es necesario
    # docker volume prune -f
    
    # Iniciar servicios
    docker-compose up -d
    
    log_info "Esperando que los servicios estén listos..."
    sleep 30
    
    # Verificar que NameNode esté corriendo
    if ! docker ps | grep -q namenode; then
        log_error "NameNode no está corriendo"
        docker-compose logs namenode
        exit 1
    fi
    
    log_success "Cluster Hadoop iniciado correctamente"
    
    # Mostrar URLs importantes
    log_info "URLs del cluster:"
    echo "  - NameNode Web UI: http://localhost:9870"
    echo "  - ResourceManager: http://localhost:8088"
    echo "  - History Server: http://localhost:8188"
}

# Función para esperar que HDFS esté listo (CORREGIDA)
wait_for_hdfs() {
    log_info "Esperando que HDFS esté completamente iniciado..."
    
    local max_attempts=15
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        log_info "Intento $attempt/$max_attempts - Verificando estado de HDFS..."
        
        # Verificar si el contenedor está corriendo
        if ! docker ps | grep -q namenode; then
            log_error "El contenedor namenode no está corriendo"
            return 1
        fi
        
        # Verificar si HDFS responde con un comando simple
        if docker exec $NAMENODE_CONTAINER hdfs dfsadmin -report &>/dev/null; then
            log_info "HDFS responde a comandos administrativos"
            
            # Verificar si podemos hacer operaciones de archivos
            if docker exec $NAMENODE_CONTAINER hdfs dfs -ls / &>/dev/null; then
                log_success "HDFS está listo y operacional"
                return 0
            else
                log_info "HDFS responde pero aún no está listo para operaciones de archivos"
            fi
        else
            log_info "HDFS aún no responde a comandos administrativos"
        fi
        
        # Verificar safe mode solo si HDFS responde
        local safe_mode_status=$(docker exec $NAMENODE_CONTAINER hdfs dfsadmin -safemode get 2>/dev/null || echo "UNKNOWN")
        log_info "Estado del safe mode: $safe_mode_status"
        
        # Si está en safe mode, intentar salir
        if echo "$safe_mode_status" | grep -q "ON"; then
            log_info "HDFS está en modo seguro, intentando salir..."
            docker exec $NAMENODE_CONTAINER hdfs dfsadmin -safemode leave 2>/dev/null || true
        fi
        
        sleep 10
        ((attempt++))
    done
    
    log_error "HDFS no pudo iniciarse correctamente después de $max_attempts intentos"
    log_info "Mostrando logs del NameNode para diagnóstico..."
    docker logs --tail 30 $NAMENODE_CONTAINER
    return 1
}

# Función para configurar HDFS (SIMPLIFICADA)
setup_hdfs() {
    log_info "Configurando directorios HDFS..."
    
    # Esperar que HDFS esté completamente listo
    if ! wait_for_hdfs; then
        log_error "HDFS no está disponible"
        return 1
    fi
    
    # Crear directorio de usuario con reintentos
    log_info "Creando directorios de usuario..."
    local create_attempts=3
    local create_attempt=1
    
    while [ $create_attempt -le $create_attempts ]; do
        if docker exec $NAMENODE_CONTAINER hdfs dfs -mkdir -p /user/root 2>/dev/null; then
            log_success "Directorio /user/root creado exitosamente"
            break
        else
            log_warning "Intento $create_attempt/$create_attempts - Error creando /user/root"
            if [ $create_attempt -eq $create_attempts ]; then
                log_error "No se pudo crear el directorio /user/root"
                return 1
            fi
            sleep 5
            ((create_attempt++))
        fi
    done
    
    # Establecer permisos
    docker exec $NAMENODE_CONTAINER hdfs dfs -chmod 755 /user/root 2>/dev/null || true
    
    # Crear directorios para el proyecto
    log_info "Creando directorios del proyecto..."
    docker exec $NAMENODE_CONTAINER hdfs dfs -mkdir -p /user/root/weather_input 2>/dev/null || true
    docker exec $NAMENODE_CONTAINER hdfs dfs -mkdir -p /user/root/weather_output 2>/dev/null || true
    
    # Verificar que los directorios se crearon
    log_info "Verificando directorios creados:"
    if docker exec $NAMENODE_CONTAINER hdfs dfs -ls /user/root/ 2>/dev/null; then
        log_success "Directorios creados exitosamente"
    else
        log_warning "No se pudieron listar los directorios, pero continuando..."
    fi
    
    log_success "Directorios HDFS configurados"
}

# Función para instalar dependencias Python en el contenedor
install_python_deps() {
    log_info "Instalando dependencias Python en NameNode..."
    
    # Verificar si ya están instaladas
    if docker exec $NAMENODE_CONTAINER python -c "import mrjob, pandas" &>/dev/null; then
        log_success "Dependencias ya están instaladas"
        return 0
    fi
    
    # Actualizar repositorios e instalar dependencias
    log_info "Instalando paquetes del sistema..."
    docker exec $NAMENODE_CONTAINER sh -c "
        export DEBIAN_FRONTEND=noninteractive && \
        apt-get update -qq && \
        apt-get install -y python-pip python-dev build-essential
    " &>/dev/null || {
        log_warning "Instalación de paquetes del sistema falló, continuando..."
    }
    
    # Instalar dependencias Python
    log_info "Instalando dependencias Python..."
    docker exec $NAMENODE_CONTAINER pip3 install --no-cache-dir --upgrade pip 2>/dev/null || true
    docker exec $NAMENODE_CONTAINER pip3 install --no-cache-dir mrjob pandas numpy 2>/dev/null || {
        log_warning "Instalación con pip falló, pero continuando..."
    }
    
    # Verificar instalación
    if docker exec $NAMENODE_CONTAINER python -c "import mrjob, pandas; print('Dependencias instaladas correctamente')" 2>/dev/null; then
        log_success "Dependencias Python instaladas y verificadas"
    else
        log_warning "Verificación de dependencias falló, pero continuando..."
    fi
}

# Función para cargar datos a HDFS (MEJORADA)
upload_data_to_hdfs() {
    log_info "Cargando datos a HDFS..."
    
    # Verificar que el archivo existe localmente
    if [ ! -f "data/raw/weather_data.csv" ]; then
        log_error "Archivo weather_data.csv no encontrado en data/raw/"
        return 1
    fi
    
    # Limpiar datos existentes en HDFS
    log_info "Limpiando datos existentes..."
    docker exec $NAMENODE_CONTAINER hdfs dfs -rm -r -f /user/root/weather_input/* 2>/dev/null || true
    
    # Copiar archivo al contenedor primero
    log_info "Copiando archivo al contenedor..."
    if docker cp data/raw/weather_data.csv $NAMENODE_CONTAINER:/tmp/weather_data.csv; then
        log_success "Archivo copiado al contenedor exitosamente"
    else
        log_error "Error copiando archivo al contenedor"
        return 1
    fi
    
    # Verificar que se copió correctamente
    if docker exec $NAMENODE_CONTAINER ls -la /tmp/weather_data.csv &>/dev/null; then
        local file_size=$(docker exec $NAMENODE_CONTAINER du -h /tmp/weather_data.csv 2>/dev/null | cut -f1)
        log_info "Archivo en contenedor: $file_size"
    else
        log_error "Archivo no encontrado en el contenedor"
        return 1
    fi
    
    # Subir a HDFS con reintentos
    log_info "Subiendo archivo a HDFS..."
    local upload_attempts=3
    local upload_attempt=1
    
    while [ $upload_attempt -le $upload_attempts ]; do
        if docker exec $NAMENODE_CONTAINER hdfs dfs -put -f /tmp/weather_data.csv /user/root/weather_input/ 2>/dev/null; then
            log_success "Archivo subido a HDFS exitosamente"
            break
        else
            log_warning "Intento $upload_attempt/$upload_attempts - Error subiendo archivo a HDFS"
            if [ $upload_attempt -eq $upload_attempts ]; then
                log_error "No se pudo subir el archivo a HDFS después de $upload_attempts intentos"
                return 1
            fi
            sleep 5
            ((upload_attempt++))
        fi
    done
    
    # Verificar carga
    log_info "Verificando datos cargados:"
    if docker exec $NAMENODE_CONTAINER hdfs dfs -ls /user/root/weather_input/ 2>/dev/null; then
        # Mostrar estadísticas del archivo
        local hdfs_file_size=$(docker exec $NAMENODE_CONTAINER hdfs dfs -du -h /user/root/weather_input/weather_data.csv 2>/dev/null | awk '{print $1}')
        log_info "Tamaño del archivo en HDFS: $hdfs_file_size"
        
        # Mostrar muestra de datos (primeras líneas)
        log_info "Muestra de datos (primeras 3 líneas):"
        docker exec $NAMENODE_CONTAINER hdfs dfs -cat /user/root/weather_input/weather_data.csv 2>/dev/null | head -3 || {
            log_warning "No se puede mostrar muestra de datos"
        }
        
        log_success "Datos cargados exitosamente a HDFS"
    else
        log_error "No se pueden verificar los datos en HDFS"
        return 1
    fi
}

# Función para ejecutar job de temperaturas (MEJORADA)
run_temperature_analysis() {
    log_info "=== EJECUTANDO ANÁLISIS DE TEMPERATURAS ==="
    
    # Limpiar output anterior
    docker exec $NAMENODE_CONTAINER hdfs dfs -rm -r -f /user/root/weather_temp_output 2>/dev/null || true
    
    # Verificar que el script existe
    if [ ! -f "mapreduce/temperature_analysis.py" ]; then
        log_error "Script temperature_analysis.py no encontrado"
        return 1
    fi
    
    # Copiar script al contenedor
    log_info "Copiando script de análisis de temperaturas..."
    docker cp mapreduce/temperature_analysis.py $NAMENODE_CONTAINER:/opt/temperature_analysis.py
    
    # Verificar permisos y hacer ejecutable
    docker exec $NAMENODE_CONTAINER chmod +x /opt/temperature_analysis.py
    
    # Ejecutar job MapReduce con mejor manejo de errores
    log_info "Ejecutando job MapReduce de temperaturas..."
    log_info "Esto puede tomar varios minutos..."
    
    if docker exec $NAMENODE_CONTAINER timeout 600 python /opt/temperature_analysis.py \
        -r hadoop \
        --hadoop-streaming-jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
        --output-dir hdfs://namenode:9000/user/root/weather_temp_output \
        hdfs://namenode:9000/user/root/weather_input/weather_data.csv 2>&1; then
        
        log_success "Análisis de temperaturas completado"
        
        # Verificar que se generó output
        log_info "Verificando resultados generados:"
        if docker exec $NAMENODE_CONTAINER hdfs dfs -ls /user/root/weather_temp_output/ 2>/dev/null; then
            # Mostrar muestra de resultados
            log_info "Muestra de resultados de temperaturas:"
            docker exec $NAMENODE_CONTAINER hdfs dfs -cat /user/root/weather_temp_output/part-00000 2>/dev/null | head -3 || {
                log_warning "No se pueden mostrar los resultados"
            }
        else
            log_warning "No se encontraron archivos de resultado"
        fi
        
    else
        log_error "Error ejecutando análisis de temperaturas"
        log_info "Verificando logs de aplicación..."
        docker exec $NAMENODE_CONTAINER find /opt/hadoop-3.2.1/logs -name "*application*" -type f -exec tail -10 {} \; 2>/dev/null || {
            log_warning "No se pueden mostrar los logs de la aplicación"
        }
        return 1
    fi
}

# Función para ejecutar job de precipitaciones (MEJORADA)
run_precipitation_analysis() {
    log_info "=== EJECUTANDO ANÁLISIS DE PRECIPITACIONES ==="
    
    # Limpiar output anterior
    docker exec $NAMENODE_CONTAINER hdfs dfs -rm -r -f /user/root/weather_precip_output 2>/dev/null || true
    
    # Verificar que el script existe
    if [ ! -f "mapreduce/precipitation_analysis.py" ]; then
        log_error "Script precipitation_analysis.py no encontrado"
        return 1
    fi
    
    # Copiar script al contenedor
    log_info "Copiando script de análisis de precipitaciones..."
    docker cp mapreduce/precipitation_analysis.py $NAMENODE_CONTAINER:/opt/precipitation_analysis.py
    
    # Hacer ejecutable
    docker exec $NAMENODE_CONTAINER chmod +x /opt/precipitation_analysis.py
    
    # Ejecutar job MapReduce
    log_info "Ejecutando job MapReduce de precipitaciones..."
    log_info "Esto puede tomar varios minutos..."
    
    if docker exec $NAMENODE_CONTAINER timeout 600 python /opt/precipitation_analysis.py \
        -r hadoop \
        --hadoop-streaming-jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
        --output-dir hdfs://namenode:9000/user/root/weather_precip_output \
        hdfs://namenode:9000/user/root/weather_input/weather_data.csv 2>&1; then
        
        log_success "Análisis de precipitaciones completado"
        
        # Verificar que se generó output
        log_info "Verificando resultados generados:"
        if docker exec $NAMENODE_CONTAINER hdfs dfs -ls /user/root/weather_precip_output/ 2>/dev/null; then
            # Mostrar muestra de resultados
            log_info "Muestra de resultados de precipitaciones:"
            docker exec $NAMENODE_CONTAINER hdfs dfs -cat /user/root/weather_precip_output/part-00000 2>/dev/null | head -3 || {
                log_warning "No se pueden mostrar los resultados"
            }
        else
            log_warning "No se encontraron archivos de resultado"
        fi
        
    else
        log_error "Error ejecutando análisis de precipitaciones"
        return 1
    fi
}

# Función para descargar resultados (MEJORADA)
download_results() {
    log_info "=== DESCARGANDO RESULTADOS ==="
    
    # Crear directorio local
    mkdir -p data/processed
    
    # Descargar resultados de temperaturas
    if docker exec $NAMENODE_CONTAINER hdfs dfs -test -e /user/root/weather_temp_output/part-00000 2>/dev/null; then
        log_info "Descargando análisis de temperaturas..."
        docker exec $NAMENODE_CONTAINER hdfs dfs -get /user/root/weather_temp_output/part-00000 /tmp/temperature_results.csv 2>/dev/null
        docker cp $NAMENODE_CONTAINER:/tmp/temperature_results.csv data/processed/ 2>/dev/null
        
        if [ -f "data/processed/temperature_results.csv" ]; then
            # Añadir header
            echo "city,year,month,month_name,avg_temp_max,avg_temp_min,avg_temp_mean,max_temp_recorded,min_temp_recorded,total_days" > data/processed/temperature_analysis.csv
            cat data/processed/temperature_results.csv >> data/processed/temperature_analysis.csv 2>/dev/null
            mv data/processed/temperature_analysis.csv data/processed/temperature_results.csv
            log_success "Resultados de temperaturas descargados"
        else
            log_warning "No se pudieron descargar los resultados de temperaturas"
        fi
    else
        log_warning "No se encontraron resultados de temperaturas en HDFS"
    fi
    
    # Descargar resultados de precipitaciones
    if docker exec $NAMENODE_CONTAINER hdfs dfs -test -e /user/root/weather_precip_output/part-00000 2>/dev/null; then
        log_info "Descargando análisis de precipitaciones..."
        docker exec $NAMENODE_CONTAINER hdfs dfs -get /user/root/weather_precip_output/part-00000 /tmp/precipitation_results.csv 2>/dev/null
        docker cp $NAMENODE_CONTAINER:/tmp/precipitation_results.csv data/processed/ 2>/dev/null
        
        if [ -f "data/processed/precipitation_results.csv" ]; then
            # Añadir header
            echo "city,year,season,total_seasonal_precipitation,avg_monthly_precipitation,max_monthly_precipitation,total_rainy_days,months_in_season" > data/processed/precipitation_analysis.csv
            cat data/processed/precipitation_results.csv >> data/processed/precipitation_analysis.csv 2>/dev/null
            mv data/processed/precipitation_analysis.csv data/processed/precipitation_results.csv
            log_success "Resultados de precipitaciones descargados"
        else
            log_warning "No se pudieron descargar los resultados de precipitaciones"
        fi
    else
        log_warning "No se encontraron resultados de precipitaciones en HDFS"
    fi
    
    # Mostrar archivos generados
    if [ -f "data/processed/temperature_results.csv" ] || [ -f "data/processed/precipitation_results.csv" ]; then
        log_success "Resultados descargados:"
        ls -la data/processed/*.csv 2>/dev/null || true
        
        # Mostrar estadísticas
        if [ -f "data/processed/temperature_results.csv" ]; then
            temp_lines=$(wc -l < data/processed/temperature_results.csv 2>/dev/null || echo "0")
            echo "  🌡️  Registros de temperaturas: $temp_lines"
        fi
        
        if [ -f "data/processed/precipitation_results.csv" ]; then
            precip_lines=$(wc -l < data/processed/precipitation_results.csv 2>/dev/null || echo "0")
            echo "  🌧️  Registros de precipitaciones: $precip_lines"
        fi
    else
        log_warning "No se pudieron descargar los resultados"
    fi
}

# Función para mostrar resumen
show_summary() {
    log_success "=== PROCESAMIENTO MAPREDUCE COMPLETADO ==="
    echo ""
    echo "📊 Archivos generados:"
    ls -la data/processed/ 2>/dev/null | grep -E "\.csv$" || echo "  (No se encontraron archivos CSV)"
    echo ""
    
    if [ -f "data/processed/temperature_results.csv" ]; then
        log_info "🌡️ Muestra de análisis de temperaturas:"
        head -3 data/processed/temperature_results.csv 2>/dev/null || echo "  Error leyendo archivo"
        echo ""
    fi
    
    if [ -f "data/processed/precipitation_results.csv" ]; then
        log_info "🌧️ Muestra de análisis de precipitaciones:"
        head -3 data/processed/precipitation_results.csv 2>/dev/null || echo "  Error leyendo archivo"
        echo ""
    fi
    
    log_success "✨ Procesamiento completado!"
    echo ""
    echo "🌐 Accede a las interfaces web:"
    echo "  • NameNode: http://localhost:9870"
    echo "  • ResourceManager: http://localhost:8088"
    echo ""
    
    if [ -f "data/processed/temperature_results.csv" ] || [ -f "data/processed/precipitation_results.csv" ]; then
        log_success "Los datos están listos para la API Flask!"
    else
        log_warning "Algunos procesos pueden haber fallado. Revisa los logs anteriores."
    fi
}

# Función principal
main() {
    echo "🚀 INICIANDO PIPELINE MAPREDUCE CON DOCKER HADOOP"
    echo "================================================="
    echo ""
    
    check_prerequisites
    check_project_structure
    start_hadoop_cluster
    setup_hdfs
    install_python_deps
    upload_data_to_hdfs
    run_temperature_analysis
    run_precipitation_analysis
    download_results
    show_summary
    
    log_success "🎉 PIPELINE COMPLETADO"
}

# Función para limpiar entorno
cleanup() {
    log_info "Deteniendo cluster Hadoop..."
    docker-compose down
    log_success "Cluster detenido"
}

# Función de ayuda
show_help() {
    echo "Uso: $0 [opción]"
    echo ""
    echo "Opciones:"
    echo "  run       Ejecutar pipeline completo (por defecto)"
    echo "  start     Solo iniciar cluster Hadoop"
    echo "  stop      Detener cluster Hadoop"
    echo "  clean     Limpiar todo y reiniciar"
    echo "  status    Mostrar estado del cluster"
    echo "  debug     Información de debugging"
    echo "  help      Mostrar esta ayuda"
}

# Función de debugging
debug_info() {
    log_info "=== INFORMACIÓN DE DEBUG ==="
    echo "Sistema: $(uname -a)"
    echo "Docker versión: $(docker --version)"
    echo "Docker Compose versión: $(docker-compose --version)"
    echo ""
    
    log_info "Estado de contenedores:"
    docker ps -a | grep -E "(namenode|datanode|resourcemanager)" || echo "No hay contenedores Hadoop corriendo"
    echo ""
    
    if docker ps | grep -q namenode; then
        log_info "Logs de NameNode (últimas 20 líneas):"
        docker logs --tail 20 namenode
        echo ""
        
        log_info "Estado de HDFS:"
        docker exec namenode hdfs dfsadmin -report 2>/dev/null || echo "No se puede conectar a HDFS"
    fi
}

# Manejar argumentos
case "${1:-run}" in
    "run")
        main
        ;;
    "start")
        start_hadoop_cluster
        setup_hdfs
        ;;
    "stop")
        cleanup
        ;;
    "clean")
        cleanup
        log_info "Limpiando volúmenes..."
        docker system prune -f
        main
        ;;
    "status")
        docker-compose ps
        ;;
    "debug")
        debug_info
        ;;
    "help")
        show_help
        ;;
    *)
        log_error "Opción no reconocida: $1"
        show_help
        exit 1
        ;;
esac