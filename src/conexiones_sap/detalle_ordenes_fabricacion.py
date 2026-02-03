import pandas as pd
from hdbcli import dbapi
import os

# Configuración de conexión (variable de entorno HANA_HOST / HANA_PORT)
try:
    from config import HANA_HOST, HANA_PORT
    host, port = HANA_HOST, HANA_PORT
except Exception:
    host, port = "127.0.0.1", 30015
user = "USERBI"  # Usuario de SAP HANA
password = "UserBI2025$"  # Contraseña del usuario


try:
    # Conexión a SAP HANA
    connection = dbapi.connect(address=host, port=port, user=user, password=password)
    print("Conexión exitosa a SAP HANA")

    cursor = connection.cursor()

    query_ListaDeRP = """
    SELECT DISTINCT
        T0."DocNum"           AS "N° Recibo Producción",
        T0."DocDate"          AS "Fecha Emisión RP",
        T0."Comments"         AS "Comentario RP",
        T0."CANCELED",
        T1."ItemCode"         AS "Código Componente",
        T5."ItemName"         AS "Descripción Componente",
        T8."ItmsGrpNam"       AS "Linea de Negocio Componente",
        T1."Quantity"         AS "Cantidad RP",
        T1."LineNum",

        T2."DocNum"           AS "N° OF",
        T2."CreateDate"       AS "Fecha OF",
        T2."ItemCode"         AS "Producto Fabricado",
        T9."ItmsGrpNam"       AS "Linea de Negocio Fabricado",
        T7."ItemName"         AS "Descripción Producto",
        T2."PlannedQty"       AS "Cantidad Planificada",
        T2."Status"           AS "Estado OF",

        T2."OriginNum"        AS "N° OV"

    FROM "SBO_MARCO_PE"."OIGN" T0                         -- Cabecera entrada de inventario
    JOIN "SBO_MARCO_PE"."IGN1" T1 
         ON T0."DocEntry" = T1."DocEntry"                 -- Detalle entrada

    LEFT JOIN "SBO_MARCO_PE"."OWOR" T2 
         ON T1."BaseType" = 202 
        AND T1."BaseEntry" = T2."DocEntry"                -- Orden de Producción

    LEFT JOIN "SBO_MARCO_PE"."OITM" T5 
         ON T1."ItemCode" = T5."ItemCode"                 -- Info componente

    LEFT JOIN "SBO_MARCO_PE"."OITB" T8 
         ON T8."ItmsGrpCod" = T5."ItmsGrpCod"

    LEFT JOIN "SBO_MARCO_PE"."OITM" T7 
         ON T2."ItemCode" = T7."ItemCode"                 -- Info producto fabricado

    LEFT JOIN "SBO_MARCO_PE"."OITB" T9 
         ON T9."ItmsGrpCod" = T7."ItmsGrpCod"

    WHERE 
        T0."ObjType" = 59
        AND T1."ItemCode"=T2."ItemCode" 
        ---AND (T2."ItemCode" LIKE 'S%' OR T2."ItemCode" LIKE 'MP%')

    ORDER BY 
        T0."DocDate" DESC, 
        T0."DocNum";

    """
    cursor.execute(query_ListaDeRP)
    results_1 = cursor.fetchall()
    columns_1 = [desc[0] for desc in cursor.description]
    df_ListaDeRP = pd.DataFrame(results_1, columns=columns_1)
    
    cursor.close()
    connection.close()
    print("Conexión cerrada")

except Exception as e:
    print(f"Error: {e}")


