import pandas as pd
from hdbcli import dbapi
import os
import logging
from pathlib import Path
import socket
import getpass

_log = logging.getLogger("Codigo.Detalle_Ordenes_Fabricacion1")

# Si base ya viene definida (ej. desde Main.py vía Validacion_OF), no sobrescribir
if "base" not in dir():
    hostname = socket.gethostname()
    ip_local = socket.gethostbyname(hostname)
    rutainicial = Path.home()
    usuario = getpass.getuser()
    antes, sep, despues = str(rutainicial).partition(usuario)
    base = Path(antes + sep)


""" if ip_local == "172.16.5.43":
    print("Conectado en el terminal")
    host = "172.16.1.52" 
else:
    print("Conectado desde otro dispositivo")
    host = "127.0.0.1"  """
    
#fecha_corte = pd.to_datetime('2026-02-01')
#fecha_corte_k = fecha_corte.date()
# Configuración de conexión (variable de entorno HANA_HOST / HANA_PORT)
try:
    from config import HANA_HOST, HANA_PORT
    host, port = HANA_HOST, HANA_PORT
except Exception:
    host, port = "127.0.0.1", 30015
user = "USERBI"  # Usuario de SAP HANA
password = "UserBI2025$"  # Contraseña del usuario

# Inicializar por si falla la conexión (ej. en Docker sin HANA); así Validacion_OF no falla con "not defined"
dfListaDeOF = pd.DataFrame()
_log.info("[Detalle_Ordenes_Fabricacion1] Conectando a HANA en %s:%s...", host, port)

try:
    # Conexión a SAP HANA
    connection = dbapi.connect(address=host, port=port, user=user, password=password)
    _log.info("[Detalle_Ordenes_Fabricacion1] Conexión exitosa a SAP HANA")

    cursor = connection.cursor()
    
    query = """
            SELECT 
                A."FechaCreacion_OF"     AS "OF_FechaCreacion",
                A."NroOF"                AS "OF_Numero",
                A."EstadoGeneral_OF"     AS "OF_EstadoGeneral",
                A."Cliente"              AS "OF_Cliente",
                A."Principal"              AS "OF_Principal",
                A."ItemCode"              AS "ItemCode",
                A."CantidadPlanificada"   AS "CantidadPlanificada",
                IFNULL(B."CantidadEmitida", 0) AS "CantidadEmitida",
                IFNULL(C."CantidadRecibida", 0) AS "CantidadRecibida"
            
            FROM 
            (
                -- === COMPONENTES PLANIFICADOS (WOR1) ===
                SELECT 
                    T0."DocNum" AS "NroOF",
                    T0."CreateDate" AS "FechaCreacion_OF",
                    T0."ItemCode" AS "Principal",
                    COALESCE(T2."CardName", T3."CardName") AS "Cliente",
                    T1."ItemCode",
                    T0."Status"   AS "EstadoGeneral_OF",
                    SUM(T1."PlannedQty") AS "CantidadPlanificada"
                FROM "SBO_MARCO_PE".OWOR T0
                LEFT JOIN "SBO_MARCO_PE".WOR1 T1
                    ON T1."DocEntry" = T0."DocEntry"
                LEFT JOIN "SBO_MARCO_PE".ORDR T2
                    ON T2."DocNum" = T0."OriginNum" 
                LEFT JOIN "SBO_MARCO_PE".OCRD T3
                    ON T3."CardCode" = T0."CardCode"
                    
                WHERE T1."ItemCode" IS NOT NULL
                    AND T1."ItemCode" <> ''
                    ---AND T0."ItemCode" NOT LIKE 'A%
                GROUP BY T0."DocNum",T0."CreateDate",T0."ItemCode", T1."ItemCode",COALESCE(T2."CardName", T3."CardName"),T0."Status"
            ) A
            
            LEFT JOIN
            (
                -- === COMPONENTES EMITIDOS (IGE1) ===
                SELECT 
                    NULLIF(T3."BaseRef", '') AS "NroOF",
                    T3."ItemCode",
                    SUM(T3."Quantity") AS "CantidadEmitida"

                FROM "SBO_MARCO_PE".OIGE T2
                LEFT JOIN "SBO_MARCO_PE".IGE1 T3
                    ON T3."DocEntry" = T2."DocEntry" 
                ---WHERE NULLIF(T3."BaseRef", '') = 225100952
                WHERE T3."ItemCode" IS NOT NULL
                    AND T3."ItemCode" <> ''
                    AND T2."CANCELED" = 'N'
                    AND T2."CreateDate" <=?
                GROUP BY NULLIF(T3."BaseRef", ''), T3."ItemCode"
            ) B
            
            ON A."NroOF" = B."NroOF"
            AND A."ItemCode" = B."ItemCode"
            
            LEFT JOIN
            (
                -- === COMPONENTES EMITIDOS (IGE1) ===
                SELECT 
                    NULLIF(T5."BaseRef", '') AS "NroOF",
                    T5."ItemCode",
                    SUM(T5."Quantity") AS "CantidadRecibida"

                FROM "SBO_MARCO_PE".OIGN T4
                LEFT JOIN "SBO_MARCO_PE".IGN1 T5
                    ON T5."DocEntry" = T4."DocEntry" 
                ---WHERE NULLIF(T3."BaseRef", '') = 225100952
                WHERE T5."ItemCode" IS NOT NULL
                    AND T5."ItemCode" <> ''
                    AND T4."CANCELED" = 'N'
                    AND T4."CreateDate" <=?
                GROUP BY NULLIF(T5."BaseRef", ''), T5."ItemCode"
            ) C
            
            ON A."NroOF" = C."NroOF"
            AND A."ItemCode" = C."ItemCode"
            
            WHERE 
            A."Principal" NOT LIKE 'A%'
            
            ORDER BY A."ItemCode";        
    """
    
    
   
    _log.info("[Detalle_Ordenes_Fabricacion1] Ejecutando query")
    cursor.execute(query,(fecha_corte_k, fecha_corte_k))
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    dfListaDeOF = pd.DataFrame(results, columns=columns)
    _log.info("[Detalle_Ordenes_Fabricacion1] Query OK. dfListaDeOF filas: %d", len(dfListaDeOF))

    cursor.close()
    connection.close()
    _log.info("[Detalle_Ordenes_Fabricacion1] Conexión cerrada")
    
except Exception as e:
    _log.warning("[Detalle_Ordenes_Fabricacion1] Error SAP HANA (sin datos): %s", e)
    # Mantener DataFrame vacío con columnas esperadas para que Validacion_OF no falle
    _cols = [
        "OF_FechaCreacion", "OF_Numero", "OF_EstadoGeneral", "OF_Cliente", "OF_Principal",
        "ItemCode", "CantidadPlanificada", "CantidadEmitida", "CantidadRecibida",
    ]
    dfListaDeOF = pd.DataFrame(columns=_cols)


