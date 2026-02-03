# Reconstruye todo y levanta con variables actualizadas
# 1. Elimina contenedor (las vars de .env se cargan al crear, no al reiniciar)
# 2. Build sin cache
# 3. Crea contenedor nuevo con .env actual
$env:DOCKER_BUILDKIT = "0"
docker-compose down
docker-compose build --no-cache
docker-compose up -d
