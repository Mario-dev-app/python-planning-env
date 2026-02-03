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

    query_ListaDeOV_Cred = """
        WITH filtro1 AS (
        SELECT DISTINCT
            -- OV
            T0."DocNum" AS "Orden de Venta",
            T0."DocDate" AS "Fecha OV",
            T0."CardCode",
            T0."CardName",
            T0."GroupNum",
            T0."DocStatus",
            T0."CANCELED" AS "OV Cancelada",
            T1."LineNum",
            T1."ItemCode",
            T1."Quantity" AS "Cantidad OV",
            T1."LineStatus",

            -- Entrega
            T3."DocNum" AS "Entrega",
            T3."DocDate" AS "Fecha Entrega",
            T3."CANCELED" AS "Entrega Cancelada",
            T4."Quantity" AS "Cantidad Entregada",

            -- Devolución física basada en entrega
            T6."DocNum" AS "Devolucion",
            T6."DocDate" AS "Fecha Devolución",
            T6."CANCELED" AS "Devolucion Cancelada",
            T7."Quantity" AS "Cantidad Devuelta",

            -- Devolución física basada en solicitud
            T6B."DocNum" AS "Devolucion x Solicitud",
            T6B."DocDate" AS "Fecha Devolución x Solicitud",
            T6B."CANCELED" AS "Devolucion Cancelada x Solicitud",
            T7B."Quantity" AS "Cantidad Devuelta x Solicitud",

            -- Factura
            T10."DocNum" AS "Factura",
            T10."DocDate" AS "Fecha Factura",
            T10."CANCELED" AS "Factura Cancelada",
            T11."Quantity" AS "Cantidad Facturada",

            -- Nota de Crédito
            T12."DocNum" AS "Nota Crédito",
            T12."DocDate" AS "Fecha Nota Crédito",
            T12."CANCELED" AS "NC Cancelada",
            T13."Quantity" AS "Cantidad NC",
            'Credito' AS "Forma de pago"

        FROM "SBO_MARCO_PE"."ORDR" T0
        INNER JOIN "SBO_MARCO_PE"."RDR1" T1
                ON T0."DocEntry" = T1."DocEntry"

        -- Entrega basada en OV (CREDITO)
        LEFT JOIN "SBO_MARCO_PE"."DLN1" T4
                ON T4."BaseType" = 17
               AND T4."BaseEntry" = T1."DocEntry"
               AND T4."BaseLine" = T1."LineNum"
               AND T4."ItemCode" = T1."ItemCode"  

        LEFT JOIN "SBO_MARCO_PE"."ODLN" T3
                ON T3."DocEntry" = T4."DocEntry"

        -- Devolución física basada en entrega
        LEFT JOIN "SBO_MARCO_PE"."RDN1" T7
                ON T7."BaseType" = 15
               AND T7."BaseEntry" = T3."DocEntry"
               AND T7."BaseLine" = T4."LineNum"
               AND T7."ItemCode" = T4."ItemCode"  

        LEFT JOIN "SBO_MARCO_PE"."ORDN" T6
                ON T6."DocEntry" = T7."DocEntry"

        -- Devolución física basada en solicitud de devolución
        LEFT JOIN "SBO_MARCO_PE"."RDN1" T7B
                ON T7B."BaseType" = 234
               AND T7B."ItemCode" = T1."ItemCode"
               AND T7B."BaseEntry" = T1."DocEntry"
               AND T7B."BaseLine" = T1."LineNum"

        LEFT JOIN "SBO_MARCO_PE"."ORDN" T6B
                ON T6B."DocEntry" = T7B."DocEntry"

        -- Factura basada en entrega
        LEFT JOIN "SBO_MARCO_PE"."INV1" T11
                ON T11."BaseType" = 15
               AND T11."BaseEntry" = T4."DocEntry"
               AND T11."BaseLine" = T4."LineNum"
               AND T11."ItemCode" = T4."ItemCode"

        LEFT JOIN "SBO_MARCO_PE"."OINV" T10
                ON T10."DocEntry" = T11."DocEntry"

        -- Nota de crédito basada en factura
        LEFT JOIN "SBO_MARCO_PE"."RIN1" T13
                ON T13."BaseType" = 13
               AND T13."BaseEntry" = T10."DocEntry"
               AND T13."BaseLine" = T11."LineNum"
               AND T13."ItemCode" = T11."ItemCode"

        LEFT JOIN "SBO_MARCO_PE"."ORIN" T12
                ON T12."DocEntry" = T13."DocEntry"

                
        -- Filtro de ítems 
        WHERE 
            (T1."ItemCode" LIKE 'A%' OR T1."ItemCode" LIKE 'MP%')
            AND T1."ItemCode" IS NOT NULL
            AND LENGTH(TRIM(T1."ItemCode")) > 0
            AND (T3."CANCELED" != 'Y' OR T3."CANCELED" IS NULL)
            AND (T10."CANCELED" != 'Y' OR T10."CANCELED" IS NULL)
    ), 
    filtro2 AS (
        SELECT *
        FROM filtro1
        WHERE 
        ("Devolucion Cancelada" = 'Y' OR "Devolucion Cancelada" IS NULL)
        AND ("NC Cancelada" = 'Y' OR "NC Cancelada" IS NULL)
    )
    SELECT *
    FROM filtro2;

    """
    cursor.execute(query_ListaDeOV_Cred)
    results_1 = cursor.fetchall()
    columns_1 = [desc[0] for desc in cursor.description]
    df_ListaDeOV_Cred = pd.DataFrame(results_1, columns=columns_1)
    
# Segundo query
    query_ListaDeOV_Cont = """
    
        WITH filtro1 AS (
        SELECT DISTINCT
            -- OV
            T0."DocNum" AS "Orden de Venta",
            T0."DocDate" AS "Fecha OV",
            T0."CardCode",
            T0."CardName",
            T0."GroupNum",
            T0."DocStatus",
            T0."CANCELED" AS "OV Cancelada",
            T1."LineNum",
            T1."ItemCode",
            T1."Quantity" AS "Cantidad OV",
            T1."LineStatus",
            
           -- Entrega relacionada a Factura basada en OV
           T16."DocNum" AS "Entrega",
           T16."DocDate" AS "Fecha Entrega",
           T16."CANCELED" AS "Entrega Cancelada",
           T17."Quantity" AS "Cantidad Entregada",
            
            -- Devolución basada en Entrega x Factura x OV
           T18."DocNum" AS "Devolucion",
           T18."DocDate" AS "Fecha Devolución",
           T18."CANCELED" AS "Devolucion Cancelada",
           T19."Quantity" AS "Cantidad Devuelta",

           -- Devolución física basada en solicitud
           T6B."DocNum" AS "Devolucion x Solicitud",
           T6B."DocDate" AS "Fecha Devolución x Solicitud",
           T6B."CANCELED" AS "Devolucion Cancelada x Solicitud",
           T7B."Quantity" AS "Cantidad Devuelta x Solicitud",   
            
            -- Factura basada en OV directamente
            T14."DocNum" AS "Factura",
            T14."DocDate" AS "Fecha Factura",
            T14."CANCELED" AS "Factura Cancelada",
            T15."Quantity" AS "Cantidad Facturada",
            
           -- Nota de crédito basada en esa devolución
           T20."DocNum" AS "Nota Crédito",
           T20."DocDate" AS "Fecha Nota Crédito",
           T20."CANCELED" AS "NC Cancelada",
           T21."Quantity" AS "Cantidad NC",
           'Contado' AS "Forma de pago"
           
        FROM "SBO_MARCO_PE"."ORDR" T0
        INNER JOIN "SBO_MARCO_PE"."RDR1" T1
                ON T0."DocEntry" = T1."DocEntry"
                
        -- Factura basada directamente en OV (CONTADO)
        LEFT JOIN "SBO_MARCO_PE"."INV1" T15
                ON T15."BaseType" = 17
               AND T15."BaseEntry" = T1."DocEntry"
               AND T15."BaseLine" = T1."LineNum"
               AND T15."ItemCode" = T1."ItemCode"

        LEFT JOIN "SBO_MARCO_PE"."OINV" T14
                ON T14."DocEntry" = T15."DocEntry"
                
        -- Entrega relacionada a Factura basada en OV
        LEFT JOIN "SBO_MARCO_PE"."DLN1" T17
                ON T17."BaseType" = 13
               AND T17."BaseEntry" = T14."DocEntry"
               AND T17."BaseLine" = T15."LineNum"
               AND T17."ItemCode" = T15."ItemCode"

        LEFT JOIN "SBO_MARCO_PE"."ODLN" T16
                ON T16."DocEntry" = T17."DocEntry"    

        -- Devolución física basada en entrega x factura x OV
        LEFT JOIN "SBO_MARCO_PE"."RDN1" T19
                ON T19."BaseType" = 15
               AND T19."BaseEntry" = T17."DocEntry"
               AND T19."BaseLine" = T17."LineNum"
               AND T19."ItemCode" = T17."ItemCode"

        LEFT JOIN "SBO_MARCO_PE"."ORDN" T18
                ON T18."DocEntry" = T19."DocEntry"
          
        -- Devolución física basada en solicitud de devolución
        LEFT JOIN "SBO_MARCO_PE"."RDN1" T7B
                ON T7B."BaseType" = 234
               AND T7B."ItemCode" = T1."ItemCode"
               AND T7B."BaseEntry" = T1."DocEntry"
               AND T7B."BaseLine" = T1."LineNum"

        LEFT JOIN "SBO_MARCO_PE"."ORDN" T6B
                ON T6B."DocEntry" = T7B."DocEntry"

        -- Nota de crédito basada en esa devolución
        LEFT JOIN "SBO_MARCO_PE"."RIN1" T21
                ON T21."BaseType" = 16
               AND T21."BaseEntry" = T18."DocEntry"
               AND T21."BaseLine" = T19."LineNum"
               AND T21."ItemCode" = T19."ItemCode"

        LEFT JOIN "SBO_MARCO_PE"."ORIN" T20
                ON T20."DocEntry" = T21."DocEntry"

        -- Filtro de ítems
        WHERE 
            (T1."ItemCode" LIKE 'A%' OR T1."ItemCode" LIKE 'MP%')
            AND T1."ItemCode" IS NOT NULL
            AND LENGTH(TRIM(T1."ItemCode")) > 0
            AND (T16."CANCELED" != 'Y' OR T16."CANCELED" IS NULL)
            AND (T14."CANCELED" != 'Y' OR T14."CANCELED" IS NULL)
    ), 
    filtro2 AS (
        SELECT *
        FROM filtro1
        WHERE 
        ("Devolucion Cancelada" = 'Y' OR "Devolucion Cancelada" IS NULL)
        AND ("NC Cancelada"= 'Y' OR "NC Cancelada" IS NULL)
    )
    SELECT *
    FROM filtro2;

    """

    cursor.execute(query_ListaDeOV_Cont)
    results_2 = cursor.fetchall()
    columns_2 = [desc[0] for desc in cursor.description]
    df_ListaDeOV_Cont = pd.DataFrame(results_2, columns=columns_2)
    
    #Union de tablas
    df_ListaDeOV=pd.concat([df_ListaDeOV_Cred, df_ListaDeOV_Cont], ignore_index=True)


    #cursor.close()
    #connection.close()
    print("Conexión cerrada")

except Exception as e:
    print(f"Error: {e}")

    # Ruta local donde se guardará el archivo
    # carpeta_local = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos"
    # nombre_archivo = "hoy.txt"
    # csv_file_path = os.path.join(carpeta_local, nombre_archivo)
    # # Guardar CSV localmente
    # df_ListaDeOV.to_csv(csv_file_path, index=False, sep='	', encoding='utf-8-sig')
