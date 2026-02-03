#!/bin/bash
# Script para ejecutar con cron - modo "once"
# Ajusta la ruta al directorio del proyecto
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
docker-compose run --rm -e RUN_MODE=once analisis-email
