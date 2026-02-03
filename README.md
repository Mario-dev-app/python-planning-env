# Análisis de Datos Automatizado con Envío de Correos

Sistema en Python + Docker que ejecuta un análisis de datos a una hora programada y envía el reporte por correo electrónico.

## Estructura del proyecto

```
demo-auto-python/
├── src/
│   ├── main.py        # Punto de entrada con scheduler
│   ├── config.py      # Configuración desde variables de entorno
│   ├── db.py          # Conexión PostgreSQL y funciones reutilizables
│   ├── analyzer.py    # Lógica de análisis de datos
│   └── email_sender.py# Envío de correos SMTP
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Requisitos previos

- Docker y Docker Compose
- Cuenta de correo con SMTP (Gmail, Outlook, etc.)

## Configuración

1. Copia el archivo de ejemplo de variables de entorno:

```bash
cp .env.example .env
```

2. Edita `.env` con tus credenciales SMTP. Para Gmail necesitas una [Contraseña de aplicación](https://support.google.com/accounts/answer/185833):

```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=tu_email@gmail.com
SMTP_PASSWORD=tu_app_password
SMTP_TO=destinatario@ejemplo.com
```

3. Ajusta las tareas programadas: `cron:nombre_tarea` separadas por `;`:

| SCHEDULES | Significado |
|-----------|-------------|
| `0 9 * * *:analisis_email` | Análisis+email a las 9:00 |
| `0 6 * * *:origen_inventario` | Flujo Origen de Inventario + BD diario a las 6:00 |
| `0 9 * * *:analisis_email;0 14 * * *:reporte_resumen` | Dos tareas distintas |
| `0 6 * * *:origen_inventario;0 9 * * *:analisis_email` | Origen BD 6:00, análisis+email 9:00 |

Tareas disponibles: `analisis_email`, `reporte_resumen`, `solo_analisis`, `origen_inventario`. Añade más en `src/jobs/tasks.py`.

## Despliegue en Ubuntu

### Opción 1: Docker Compose (recomendado)

El contenedor corre 24/7 y el scheduler interno ejecuta el análisis a la hora configurada.

```bash
# Instalar Docker en Ubuntu (si no lo tienes)
sudo apt update
sudo apt install -y docker.io docker-compose
sudo usermod -aG docker $USER
# Cierra sesión y vuelve a entrar

# Clonar/copiar el proyecto
cd /ruta/del/proyecto

# Configurar .env
cp .env.example .env
nano .env   # Editar credenciales

# Construir y ejecutar
docker-compose up -d --build

# Ver logs
docker-compose logs -f
```

### Opción 2: Cron + Docker (ejecutar a una hora específica)

Útil si prefieres usar el cron del sistema en lugar del scheduler interno:

1. Crea un script `run_cron.sh` en el proyecto:

```bash
#!/bin/bash
cd /ruta/del/proyecto
docker-compose run --rm -e RUN_MODE=once analisis-email
```

2. Hazlo ejecutable y añádelo al crontab:

```bash
chmod +x run_cron.sh
crontab -e
```

Añade esta línea (ejecuta a las 9:00 AM todos los días):

```
0 9 * * * /ruta/del/proyecto/run_cron.sh >> /var/log/analisis-email.log 2>&1
```

### Opción 3: Solo Docker (sin Compose)

```bash
docker build -t analisis-email .
docker run -d --name analisis-email --env-file .env --restart unless-stopped analisis-email
```

## Modos de ejecución

| Variable | Valores | Descripción |
|----------|---------|-------------|
| `RUN_MODE` | `scheduler` | El contenedor corre 24/7 y ejecuta a la hora configurada |
| `RUN_MODE` | `once` | Ejecuta el análisis una vez y termina (ideal para cron) |

## Desarrollo local (sin Docker)

```bash
python -m venv .venv
source .venv/bin/activate   # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Editar .env
python src/main.py
```

## Base de datos PostgreSQL

Con Docker Compose se levanta un contenedor PostgreSQL; la aplicación espera a que esté listo antes de arrancar.

**Variables en `.env`:** `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`. En Docker el host es `postgres` (ya configurado en `docker-compose`); en local usa `DB_HOST=localhost`.

**Uso del módulo `src/db.py`** (sentencias y funciones reutilizables):

```python
from db import execute, fetch_one, fetch_all, transaction, call_function

# INSERT/UPDATE/DELETE
execute("INSERT INTO logs (mensaje) VALUES (%s)", ("hola",))

# SELECT una fila (diccionario o None)
row = fetch_one("SELECT * FROM usuarios WHERE id = %s", (1,))

# SELECT todas las filas (lista de diccionarios)
filas = fetch_all("SELECT * FROM eventos ORDER BY fecha DESC")

# Varias sentencias en una transacción
with transaction() as conn:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ...")
        cur.execute("UPDATE ...")

# Llamar a una función de PostgreSQL
resultado = call_function("mi_funcion", (arg1, arg2))
```

### Job Origen de Inventario (almacenamiento diario en BD)

La tarea `origen_inventario` ejecuta el flujo de `src/Codigo/Main.py`: genera el archivo `Resultado_YYYYMMDD.txt` e inserta todo el resultado en la tabla `resultado_origen_inventario` de PostgreSQL. Para que se ejecute **diariamente** (por ejemplo a las 6:00), configura en `.env`:

```
SCHEDULES="0 6 * * *:origen_inventario"
```

O combínala con otras tareas: `SCHEDULES="0 6 * * *:origen_inventario;0 9 * * *:analisis_email"`.

## Añadir tareas personalizadas

Edita `src/jobs/tasks.py` y usa el decorador `@register`:

```python
from jobs.registry import register

@register("mi_tarea")
def mi_tarea():
    # Tu lógica aquí
    print("Ejecutando mi tarea...")
```

Luego en `.env`: `SCHEDULES="0 10 * * *:mi_tarea;0 15 * * *:analisis_email"`

## Personalizar el análisis

Edita `src/analyzer.py` para adaptar el análisis a tus datos reales. Puedes:

- Conectarte a una base de datos
- Leer archivos CSV/Excel
- Cambiar las métricas calculadas
- Modificar el formato del reporte HTML

## Reconstruir y actualizar variables

Las variables de `.env` se cargan al **crear** el contenedor, no al reiniciarlo. Si cambiaste el `.env`, usa:

**Windows (PowerShell):**
```powershell
.\build.ps1
```

**Linux / EC2:**
```bash
chmod +x deploy.sh
./deploy.sh
```

Ambos hacen: `down` → `build --no-cache` → `up -d` para recrear todo con la config actual.

## Solución de problemas

### Error "exporting to image" al hacer build en Windows

Este error suele aparecer con Docker BuildKit. Prueba:

**Opción 1** – Usar el script de build (builder legacy):

```powershell
.\build.ps1
```

**Opción 2** – Desactivar BuildKit manualmente:

```powershell
$env:DOCKER_BUILDKIT = "0"
docker-compose build --no-cache
```

**Opción 3** – Usar `docker build` directamente:

```powershell
docker build -t analisis-email .
docker-compose up -d
```

## Notas

- Sin credenciales SMTP configuradas, el sistema simula el envío y muestra el reporte en los logs.
- Para producción, considera usar secretos de Docker o un gestor de secretos.
