"""Punto de entrada principal - Programa tareas a diferentes horarios."""
import logging
import sys
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

from config import SCHEDULES, TIMEZONE, RUN_MODE, RUN_ON_STARTUP

# Importar tareas para que se registren
import jobs.tasks  # noqa: F401
from jobs.registry import get_job, list_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Configura el scheduler o ejecuta una sola vez."""
    if RUN_MODE == "once":
        if not SCHEDULES:
            logger.error("No hay tareas configuradas en SCHEDULES")
            sys.exit(1)
        cron_expr, job_name = SCHEDULES[0]
        job_fn = get_job(job_name)
        if not job_fn:
            logger.error("Tarea desconocida: %s. Disponibles: %s", job_name, list_jobs())
            sys.exit(1)
        logger.info("Modo 'once': ejecutando %s...", job_name)
        job_fn()
        return

    # Modo scheduler
    scheduler = BlockingScheduler(timezone=TIMEZONE)

    if not SCHEDULES:
        logger.warning("SCHEDULES vacío. Usa formato: cron:nombre (ej: 0 9 * * *:analisis_email)")
        logger.info("Tareas disponibles: %s", list_jobs())
        scheduler.start()
        return

    for i, (cron_expr, job_name) in enumerate(SCHEDULES):
        job_fn = get_job(job_name)
        if not job_fn:
            logger.error("Tarea desconocida: %s. Disponibles: %s", job_name, list_jobs())
            continue

        parts = cron_expr.split()
        if len(parts) >= 5:
            minute, hour, day, month, day_of_week = parts[0], parts[1], parts[2], parts[3], parts[4]
        else:
            logger.warning("Cron incorrecto: '%s'. Usando 9:00 AM.", cron_expr)
            minute, hour, day, month, day_of_week = "0", "9", "*", "*", "*"

        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=TIMEZONE,
        )
        job_id = f"{job_name}_{i}" if len(SCHEDULES) > 1 else job_name
        scheduler.add_job(job_fn, trigger=trigger, id=job_id)

        logger.info("Tarea: %s -> %s a las %s:%s (%s)", job_id, job_name, hour, minute.zfill(2), TIMEZONE)
        try:
            tz = ZoneInfo(TIMEZONE)
            next_run = trigger.get_next_fire_time(None, datetime.now(tz))
            logger.info("  -> Proxima ejecucion: %s", next_run)
        except Exception:
            pass

    logger.info("Presiona Ctrl+C para detener.")

    if RUN_ON_STARTUP and SCHEDULES:
        cron_expr, job_name = SCHEDULES[0]
        job_fn = get_job(job_name)
        if job_fn:
            logger.info("RUN_ON_STARTUP: ejecutando %s ahora...", job_name)
            job_fn()

    scheduler.start()


if __name__ == "__main__":
    main()
