"""Módulo de análisis de datos."""
import io
from datetime import datetime
from typing import Tuple

import pandas as pd
import numpy as np


def generate_sample_data() -> pd.DataFrame:
    """Genera datos de ejemplo para el análisis."""
    np.random.seed(42)
    dates = pd.date_range(
        start=datetime.now() - pd.Timedelta(days=30),
        end=datetime.now(),
        freq="D",
    )
    return pd.DataFrame(
        {
            "fecha": dates,
            "ventas": np.random.randint(100, 1000, len(dates)),
            "clientes": np.random.randint(20, 200, len(dates)),
        }
    )


def run_analysis() -> Tuple[str, str]:
    """
    Ejecuta el análisis de datos y retorna el resumen HTML y el CSV como string.
    
    Returns:
        Tuple con (resumen_html, csv_content)
    """
    df = generate_sample_data()

    # Análisis
    total_ventas = df["ventas"].sum()
    promedio_ventas = df["ventas"].mean()
    total_clientes = df["clientes"].sum()
    tendencia = df["ventas"].iloc[-7:].mean() - df["ventas"].iloc[:7].mean()

    # Generar HTML del reporte
    html = f"""
    <h2>📊 Reporte de Análisis de Datos</h2>
    <p><strong>Fecha del análisis:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
    
    <h3>Resumen Ejecutivo</h3>
    <ul>
        <li>Total de ventas (30 días): <strong>${total_ventas:,.0f}</strong></li>
        <li>Promedio diario de ventas: <strong>${promedio_ventas:,.0f}</strong></li>
        <li>Total de clientes atendidos: <strong>{total_clientes:,}</strong></li>
        <li>Tendencia (últimos 7 vs primeros 7 días): <strong>{tendencia:+,.0f}</strong></li>
    </ul>

    <h3>Top 5 días con mayor venta</h3>
    {df.nlargest(5, 'ventas')[['fecha', 'ventas', 'clientes']].to_html(index=False)}
    
    <p><em>Este reporte fue generado automáticamente por el sistema de análisis.</em></p>
    """

    # CSV para adjuntar
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    return html, csv_content
