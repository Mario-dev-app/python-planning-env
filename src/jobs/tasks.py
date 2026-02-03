"""Tareas registradas - añade aquí tus propias funciones."""
import logging
import runpy
from pathlib import Path
from datetime import datetime

from jobs.registry import register

logger = logging.getLogger(__name__)


@register("analisis_email")
def analisis_y_email():
    """Análisis completo y envío por correo."""
    from analyzer import run_analysis
    from email_sender import send_report_email

    logger.info("Ejecutando: analisis_email")
    try:
        html, csv_content = run_analysis()
        subject = f"Reporte de Análisis - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        send_report_email(subject, html, csv_content)
        """enviar correo sin el analisis"""
        #send_report_email(subject, html, csv_content=None)
        logger.info("analisis_email completado correctamente.")
    except Exception as e:
        logger.error("Error en analisis_email: %s", e, exc_info=True)



@register("origen_inventario")
def origen_inventario():
    """Ejecuta el flujo de Codigo (Origen de Inventario): genera Resultado_YYYYMMDD.txt e inserta en PostgreSQL."""
    logger.info("Ejecutando: origen_inventario (flujo Codigo -> BD)")
    try:
        # Ejecutar Main.py de Codigo en el mismo proceso para que base = carpeta Codigo y encuentre config/db
        codigo_dir = Path(__file__).resolve().parent.parent / "origen_inventario"
        main_py = codigo_dir / "main.py"
        if not main_py.exists():
            logger.error("No se encuentra origen_inventario/Main.py en %s", main_py)
            return
        runpy.run_path(str(main_py), run_name="__main__")
        logger.info("origen_inventario completado: datos almacenados en resultado_origen_inventario.")
    except Exception as e:
        logger.error("Error en origen_inventario: %s", e, exc_info=True)
