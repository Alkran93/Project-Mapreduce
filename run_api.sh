#!/bin/bash
# Script para ejecutar la API Flask después del procesamiento MapReduce

set -e

# Colores
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

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

# Verificar que los datos procesados existen
check_processed_data() {
    log_info "Verificando datos procesados..."
    
    if [ ! -f "data/processed/temperature_results.csv" ]; then
        log_error "No se encontró temperature_results.csv"
        log_warning "Ejecuta primero: ./setup_and_run.sh"
        exit 1
    fi
    
    if [ ! -f "data/processed/precipitation_results.csv" ]; then
        log_error "No se encontró precipitation_results.csv"
        log_warning "Ejecuta primero: ./setup_and_run.sh"
        exit 1
    fi
    
    log_success "Datos procesados encontrados"
}

# Configurar entorno Python para API
setup_api_environment() {
    log_info "Configurando entorno para la API..."
    
    # Crear entorno virtual si no existe
    if [ ! -d "api/venv" ]; then
        log_info "Creando entorno virtual..."
        cd api
        python -m venv venv
        cd ..
    fi
    
    # Activar entorno virtual e instalar dependencias
    log_info "Instalando dependencias..."
    cd api
    source venv/Scripts/activate
    pip install -r requirements.txt --quiet
    cd ..
    
    log_success "Entorno de API configurado"
}

# Ejecutar API
start_api() {
    log_info "Iniciando API Flask..."
    echo ""
    log_success "🚀 API INICIADA - Accede a: http://localhost:5000"
    echo ""
    log_info "📋 Endpoints principales:"
    echo "  • http://localhost:5000/                     - Info de la API"
    echo "  • http://localhost:5000/health               - Estado"
    echo "  • http://localhost:5000/data/temperature     - Temperaturas"
    echo "  • http://localhost:5000/data/precipitation   - Precipitaciones"
    echo "  • http://localhost:5000/data/summary         - Resumen"
    echo "  • http://localhost:5000/data/cities          - Ciudades"
    echo ""
    log_warning "Presiona Ctrl+C para detener la API"
    echo ""
    
    cd api
    source venv/bin/activate
    python app.py
}

# Función principal
main() {
    echo "🌐 INICIANDO API FLASK PARA ANÁLISIS CLIMÁTICO"
    echo "=============================================="
    echo ""
    
    check_processed_data
    setup_api_environment
    start_api
}

# Ejecutar función principal
main "$@"