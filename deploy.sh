#!/bin/bash
# Reconstruye todo y levanta con variables actualizadas (EC2/Linux)
# 1. Elimina contenedor (las vars de .env se cargan al crear, no al reiniciar)
# 2. Build sin cache
# 3. Crea contenedor nuevo con .env actual
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
docker-compose down
docker-compose build --no-cache
docker-compose up -d
