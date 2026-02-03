"""Configuración de la aplicación."""
import os
from dotenv import load_dotenv

load_dotenv()

# Tareas programadas: "cron:nombre_tarea" separadas por ;
# Ejemplo: "0 9 * * *:analisis_email;0 14 * * *:reporte_resumen;0 17 * * *:solo_analisis"
# Tareas disponibles: analisis_email, reporte_resumen, solo_analisis (y las que añadas en jobs/tasks.py)
_raw = os.getenv("SCHEDULES", os.getenv("SCHEDULE_CRON", "0 9 * * *:analisis_email")).strip().strip('"').strip("'")
SCHEDULES: list[tuple[str, str]] = []  # [(cron, job_name), ...]
for item in _raw.split(";"):
    item = item.strip()
    if ":" in item:
        cron, job_name = item.split(":", 1)
        SCHEDULES.append((cron.strip(), job_name.strip()))
    elif item:
        # Formato antiguo: solo cron (usa analisis_email por defecto)
        SCHEDULES.append((item, "analisis_email"))

# Configuración SMTP para envío de correos
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip().strip('"').strip("'")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_TO = os.getenv("SMTP_TO", "").split(",") if os.getenv("SMTP_TO") else []
# Para Office 365: algunas cuentas requieren ciphers mas permisivos. Ej: "DEFAULT:@SECLEVEL=1"
SMTP_TLS_CIPHERS = os.getenv("SMTP_TLS_CIPHERS", "")

# Zona horaria
TIMEZONE = os.getenv("TIMEZONE", "America/Mexico_City")

# Modo de ejecución: "scheduler" (programa interno) o "once" (ejecuta y sale)
RUN_MODE = os.getenv("RUN_MODE", "scheduler")

# Si es "true" o "1", ejecuta el análisis inmediatamente al iniciar (útil para pruebas)
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "false").lower() in ("true", "1", "yes")

# SAP HANA (en Docker con túnel en el host usar HANA_HOST=host.docker.internal; en local 127.0.0.1)
HANA_HOST = os.getenv("HANA_HOST", "127.0.0.1").strip()
HANA_PORT = int(os.getenv("HANA_PORT", "30015"))

# PostgreSQL (en Docker usar host "postgres", en local "localhost")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "jobs_db"))
DB_USER = os.getenv("DB_USER", os.getenv("POSTGRES_USER", "jobs"))
DB_PASSWORD = os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "")).strip().strip('"').strip("'")
