"""Módulo para envío de correos electrónicos."""
import logging
import os
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional

from config import (
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    SMTP_FROM,
    SMTP_TO,
    SMTP_TLS_CIPHERS,
)

logger = logging.getLogger(__name__)


def send_report_email(
    subject: str,
    html_body: str,
    csv_content: Optional[str] = None,
) -> bool:
    """
    Envía un correo con el reporte de análisis.

    Args:
        subject: Asunto del correo
        html_body: Cuerpo del correo en HTML
        csv_content: Contenido CSV para adjuntar (opcional)

    Returns:
        True si se envió correctamente
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.warning("SMTP_USER o SMTP_PASSWORD no configurados. Simulando envio...")
        logger.info("Subject: %s | Destinatarios: %s", subject, SMTP_TO or ["No configurados"])
        return True

    if not SMTP_TO:
        logger.warning("SMTP_TO no configurado. No se enviara el correo.")
        return False

    # Diagnóstico: si SMTP_DEBUG=true, verifica que la contraseña llegue correctamente
    if os.getenv("SMTP_DEBUG", "").lower() in ("true", "1"):
        logger.info("SMTP_DEBUG: usuario=%s, longitud_password=%d", SMTP_USER, len(SMTP_PASSWORD))
        if len(SMTP_PASSWORD) < 6:
            logger.warning("La contraseña parece corta. Si tiene $ puede haberse corrompido. Usa $$ en .env")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = ", ".join(SMTP_TO)

    msg.attach(MIMEText(html_body, "html"))

    if csv_content:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(csv_content.encode("utf-8"))
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            "attachment",
            filename="reporte_analisis.csv",
        )
        msg.attach(part)

    try:
        # Contexto SSL compatible con Office 365 (similar a Nodemailer con tls.ciphers)
        context = ssl.create_default_context()
        if SMTP_TLS_CIPHERS:
            context.set_ciphers(SMTP_TLS_CIPHERS)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, SMTP_TO, msg.as_string())
        logger.info("Correo enviado correctamente a %s", SMTP_TO)
        return True
    except Exception as e:
        logger.error("Error al enviar correo: %s", e, exc_info=True)
        return False
