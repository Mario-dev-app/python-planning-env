import pandas as pd
from hdbcli import dbapi
import os
import logging
from pathlib import Path
import socket
import getpass

_log = logging.getLogger("Codigo.Detalle_Ordenes_VentaV2")

# Si base ya viene definida (ej. desde Main.py vía Validacion_OV), no sobrescribir
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

#def obtener_detalle_ordenes_venta():
#fecha_corte = pd.to_datetime('2025-12-25')
#fecha_corte_k = fecha_corte.date()
# Configuración de conexión (variable de entorno HANA_HOST / HANA_PORT)
try:
    from config import HANA_HOST, HANA_PORT
    host, port = HANA_HOST, HANA_PORT
except Exception:
    host, port = "127.0.0.1", 30015
user = "USERBI"  # Usuario de SAP HANA
password = "UserBI2025$"  # Contraseña del usuario

# Inicializar por si falla la conexión (ej. en Docker sin HANA); así Validacion_OV no falla con "not defined"
df_ListaDeOV_C = pd.DataFrame()
_log.info("[Detalle_Ordenes_VentaV2] Conectando a HANA en %s:%s...", host, port)

try:
            # Conexión a SAP HANA
            connection = dbapi.connect(address=host, port=port, user=user, password=password)
            _log.info("[Detalle_Ordenes_VentaV2] Conexión exitosa a SAP HANA")
        
            cursor = connection.cursor()
        
            query_ListaDeOV_Cred = """
                SELECT  
            T0."DocEntry"     AS "OV_DocEntry",
            T0."DocNum"       AS "Orden de Venta",
            T0."CardName"     AS "OV_Cliente",
            T0."DocStatus"    AS "OV_EstadoGeneral",
            T0."CANCELED"     AS "OV Cancelada",
            T0."CreateDate"   AS "OV_FechaCreacion",
            T0."DocDate"      AS "OV_FechaContabilizacion",
            T0."DocDueDate"   AS "OV_FechaVencimiento",
            T0."GroupNum" AS "CodigoCondPAGO",
        
            T1."LineNum",
            T1."LineStatus" as "OV_LineStatus",
            T1."ItemCode",
            T1."Dscription",
            T1."Quantity"     AS "Cantidad OV",
        
            -- CANTIDAD EN LA FACTURA (solo basada en OV directa)
            F."DocEntry"      AS "FAC_DocEntry",
            F."DocNum"        AS "FAC_Numero",
            F."DocStatus"     AS "FAC_Estado",
            F."CreateDate"   AS "FAC_FechaCreacion",
            F."DocDate"      AS "FAC_FechaContabilizacion",
            F1."LineNum"    AS "FAC_LineNum",
            F1."Quantity"     AS "FAC_Quantity",
        
            -- CANTIDAD EN LA ENTREGA
            E."DocEntry"      AS "NE_DocEntry",
            E."DocNum"        AS "NE_Numero",
            E."DocStatus"     AS "NE_Estado",
            E."CreateDate"   AS "NE_FechaCreacion",
            E."DocDate"      AS "NE_FechaContabilizacion",
            E1."LineNum"    AS "NE_LineNum",
            E1."BaseLine"     as "NE_BaseLine",
            E1."Quantity"     AS "Cantidad Entregada",
        
            -- CANTIDAD EN LA DEVOLUCIÓN
            DV."DocEntry"     AS "DV_DocEntry",
            DV."DocNum"       AS "DV_Numero",
            DV."DocStatus"    AS "DV_Estado",
            DV."CreateDate"   AS "DV_FechaCreacion",
            DV."DocDate"      AS "DV_FechaContabilizacion",
            DV1."LineNum"     AS "DV_LineNum",
            DV1."Quantity"    AS "DV_Quantity",
            
            ----------------------------------------------------------
            -- NOTAS DE CRÉDITO DE CLIENTES BASADAS EN FACTURA
            -- SOLO SI GENERAN REINGRESO A INVENTARIO:
            -- BaseType = 13  (basada en factura)
            -- DropShip = 'N' (sí genera inventario)
            ----------------------------------------------------------
            NC."DocEntry"      AS "NC_DocEntry",
            NC."DocNum"        AS "NC_Numero",
            NC."DocStatus"     AS "NC_Estado",
            NC."CreateDate"    AS "NC_FechaCreacion",
            NC."DocDate"       AS "NC_FechaContabilizacion",
            NC1."LineNum"     AS "NC_LineNum",
            NC1."Quantity"     AS "NC_Quantity"
        
            FROM "SBO_MARCO_PE"."ORDR" T0
            INNER JOIN "SBO_MARCO_PE"."RDR1" T1 
                ON T0."DocEntry" = T1."DocEntry"
            
            ----------------------------------------------------------
            -- FACTURAS (solo si derivan de la OV directamente)
            ----------------------------------------------------------
            LEFT JOIN "SBO_MARCO_PE"."INV1" F1
                ON F1."BaseType" = 17
                AND F1."BaseEntry" = T0."DocEntry"
                AND F1."BaseLine" = T1."LineNum"
            
            LEFT JOIN "SBO_MARCO_PE"."OINV" F
                ON F."DocEntry" = F1."DocEntry"
                AND F."CANCELED" = 'N'
            
            ----------------------------------------------------------
            -- ENTREGAS (derivadas de OV o de la factura contado)
            ----------------------------------------------------------
            LEFT JOIN "SBO_MARCO_PE"."DLN1" E1
                ON (
                    (E1."BaseType" = 17 
                     AND E1."BaseEntry" = T0."DocEntry" 
                     AND E1."BaseLine" = T1."LineNum")
                    OR
                    (E1."BaseType" = 13 
                     AND E1."BaseEntry" = F."DocEntry"
                     AND E1."BaseLine" = F1."LineNum"
                     )
                )
            
            LEFT JOIN "SBO_MARCO_PE"."ODLN" E
                ON E."DocEntry" = E1."DocEntry"
                AND E."CANCELED" = 'N'
        
            ----------------------------------------------------------
            -- Facturas basadas en la entrega
            ----------------------------------------------------------
            LEFT JOIN "SBO_MARCO_PE"."INV1" F2
                ON F2."BaseType" = 15
                AND F2."BaseEntry" = E."DocEntry"
                AND F2."BaseLine" = E1."LineNum"
        
            LEFT JOIN "SBO_MARCO_PE"."OINV" F_E
                ON F_E."DocEntry" = F2."DocEntry"
                AND F_E."CANCELED" = 'N'
            
            ----------------------------------------------------------
            -- NOTAS DE CRÉDITO (desde entrega O factura)
            ----------------------------------------------------------
            
            LEFT JOIN "SBO_MARCO_PE"."RIN1" NC1
                ON NC1."DropShip" = 'N'
                AND 
                (
                     (NC1."BaseType" = 15
                     AND NC1."BaseEntry" = E."DocEntry"
                     AND NC1."BaseLine" = E1."LineNum")
                     OR
                     (NC1."BaseType" = 13
                     AND NC1."BaseEntry" = F."DocEntry"
                     AND NC1."BaseLine" = F1."LineNum")
                     OR
                     (NC1."BaseType" = 13
                     AND NC1."BaseEntry" = F_E."DocEntry"
                     AND NC1."BaseLine" = F2."LineNum")
                    )
            
            LEFT JOIN "SBO_MARCO_PE"."ORIN" NC
                ON NC."DocEntry" = NC1."DocEntry"
                AND NC."CANCELED" = 'N'
             
            
            ----------------------------------------------------------
            -- DEVOLUCIONES (basadas en la entrega)
            ----------------------------------------------------------
            LEFT JOIN "SBO_MARCO_PE"."RDN1" DV1
                ON DV1."BaseType" = 15
                AND DV1."BaseEntry" = E."DocEntry"
                AND DV1."BaseLine" = E1."LineNum"
            
            LEFT JOIN "SBO_MARCO_PE"."ORDN" DV
                ON DV."DocEntry" = DV1."DocEntry"
                AND DV."CANCELED" = 'N'
            ---WHERE  T0."DocNum" =234501531 AND  T1."ItemCode" = 'A18110006503'
            ---WHERE  T0."DocNum" =1907004410 AND  T1."ItemCode" = 'A18130007144'
            WHERE
            ---T0."DocNum" =1907004410 AND  T1."ItemCode" = 'A18130007144' AND
            (E."CreateDate" IS NULL OR E."CreateDate" <= ?)
            AND
            (DV."CreateDate" IS NULL OR DV."CreateDate" <= ?)
            
            
            ORDER BY
                T0."DocNum",
                T1."LineNum",
                E."DocNum",
                DV."DocNum",
                NC."DocNum"
            ;
        
            """
            
            _log.info("[Detalle_Ordenes_VentaV2] Ejecutando query...")
            cursor.execute(query_ListaDeOV_Cred, (fecha_corte_k, fecha_corte_k))
            #cursor.execute(query_ListaDeOV_Cred)
            
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df_ListaDeOV_C = pd.DataFrame(results, columns=columns)
            _log.info("[Detalle_Ordenes_VentaV2] Query OK. df_ListaDeOV_C filas: %d", len(df_ListaDeOV_C))
            cursor.close()
            connection.close()

            _log.info("[Detalle_Ordenes_VentaV2] Conexión cerrada")
        
except Exception as e:
            _log.warning("[Detalle_Ordenes_VentaV2] Error SAP HANA (sin datos): %s", e)
            # Mantener DataFrame vacío con columnas esperadas para que Validacion_OV no falle
            _cols = [
                "OV_DocEntry", "Orden de Venta", "OV_Cliente", "OV_EstadoGeneral", "OV Cancelada",
                "OV_FechaCreacion", "OV_FechaContabilizacion", "OV_FechaVencimiento", "CodigoCondPAGO",
                "LineNum", "OV_LineStatus", "ItemCode", "Dscription", "Cantidad OV",
                "FAC_DocEntry", "FAC_Numero", "FAC_Estado", "FAC_FechaCreacion", "FAC_FechaContabilizacion",
                "FAC_LineNum", "FAC_Quantity",
                "NE_DocEntry", "NE_Numero", "NE_Estado", "NE_FechaCreacion", "NE_FechaContabilizacion",
                "NE_LineNum", "NE_BaseLine", "Cantidad Entregada",
                "DV_DocEntry", "DV_Numero", "DV_Estado", "DV_FechaCreacion", "DV_FechaContabilizacion",
                "DV_LineNum", "DV_Quantity",
                "NC_DocEntry", "NC_Numero", "NC_Estado", "NC_FechaCreacion", "NC_FechaContabilizacion",
                "NC_LineNum", "NC_Quantity",
            ]
            df_ListaDeOV_C = pd.DataFrame(columns=_cols)
        #return df_ListaDeOV_C
        

         
