import pandas as pd
from hdbcli import dbapi
import os
import logging
from pathlib import Path
import socket
import getpass

_log = logging.getLogger("Codigo.Kardex_MPSA")

# Si base ya viene definida (ej. desde Main.py), no sobrescribir
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
    host = "127.0.0.1" """

#fecha_corte = pd.to_datetime('2026-02-02')
#fecha_corte_k = fecha_corte.date()

#Coneccion  a SAP
# Configuración de conexión (variable de entorno HANA_HOST / HANA_PORT)
try:
    from config import HANA_HOST, HANA_PORT
    host, port = HANA_HOST, HANA_PORT
except Exception:
    host, port = "127.0.0.1", 30015
user = "USERBI"
password = "UserBI2025$"

# Inicializar por si falla la conexión (ej. en Docker sin HANA); así Main.py no falla con "not defined"
dfIncome = pd.DataFrame()
_log.info("[Kardex-MPSA] Conectando a HANA en %s:%s...", host, port)

try:
    # Conexión a SAP HANA
    connection = dbapi.connect(address=host, port=port, user=user, password=password)
    _log.info("[Kardex-MPSA] Conexión exitosa a SAP HANA")

    cursor = connection.cursor()
    
    query = """ 
    WITH kardex AS
(
	SELECT  T2."ItmsGrpNam"
	       ,T0."TransNum"
	       ,T0."DocLineNum"
	       ,T0."TransType"
	       ,CASE WHEN T0."TransType" = '18' THEN 'Factura de Proveedores'
	             WHEN T0."TransType" = '20' THEN 'Entrada por Compra'
	             WHEN T0."TransType" = '59' THEN 'Entrada por Inventario'  ELSE 'Revisar' END  AS "Tipo"
	       ,T0."CreatedBy"
           ,T0."BASE_REF" 
           ,COALESCE(
                TO_NVARCHAR(T27."DocNum"),
                TO_NVARCHAR(T29."DocNum"),
                TO_NVARCHAR(T30."U_HAK_NROOC")
            ) as "Numero_PO"      
	       ,T0."JrnlMemo"
	       ,T0."ItemCode"
	       ,T0."Dscription"
           ,T1."AvgPrice"
	       ,T15."U_EXM_CARGADOA"
           ,T15."U_EXM_CARGADOA2"
           ,T15."FreeTxt"
           --,T25."CardName" as "Proveedor en la PO"
	       ,T16."DocNum"  AS "OF"
	       ,T16."Status"  AS "Status OF"
	       ,T16."CardCode"   AS "ClienteOF"
	       ,T18."PlannedQty"-T18."IssuedQty"   AS "Cantidad pendiente de OF"
           ,T18."LineNum" AS "LineNumOF"
           ,T17."DocNum"  AS "OV"
	       ,T17."DocStatus"  AS "Status OV"
		   ,T17."CANCELED"   AS "OV Cancelada"
	       ,T17."CardCode"  AS "ClienteOV"
           ,T22."LineNum"  AS  "LineNumOV"
	       ,CASE WHEN T22."LineStatus" = 'O' THEN 'Abierto'
	             WHEN T22."LineStatus" = 'C' THEN 'Cerrado'  ELSE '' END  AS "Status Linea OV"
	       ,T1."U_EXX_CGET"
	       ,T0."Currency"
	       ,T0."Price"
	       ,T0."TransValue"
	       ,T0."CreateDate"  AS "Fecha Creacion"
		   ,T0."DocDate"  AS "Fecha Contabilizacion"
	       ,T0."InQty"
	       ,T0."OutQty"
	       ,T0."Warehouse"
	       ,T0."UserSign"
	       ,T0."Comments"
	       ,T0."CardName"
	       ,T0."DocTime"
           ,T0."TransSeq"
           ,T0."SubLineNum"
	       ,T19."BaseRef"  AS "OF Relacionada"
           ,T20."OriginNum"  AS "OV Relacionada"
           ,T20."ItemCode"   AS "Item_Principal"
           ,CASE WHEN T24."LineStatus" = 'O' THEN 'Abierto'
	             WHEN T24."LineStatus" = 'C' THEN 'Cerrado'  ELSE '' END  AS "Status Linea OV Relacionada"
	       ,T23."CANCELED"   AS "OV Relacionada Cancelada"
	       ,T21."FormatCode" AS "Codigo cuenta contable"
	       ,T21."AcctName"  AS "Nombre cuenta contable"
	       ,CASE WHEN T0."TransType" = '20' AND T15."U_EXM_CARGADOA" = 'GA' THEN 'COMPRA CALZADA GASTO'
	             WHEN T0."TransType" = '20' AND T15."U_EXM_CARGADOA" = 'G' THEN 'COMPRA CALZADA GARANTIA'
	             WHEN T0."TransType" = '20' AND (T15."U_EXM_CARGADOA" = 'OF' OR T16."DocNum" IS NOT NULL) THEN 'COMPRA CALZADA OF'
	             WHEN T0."TransType" = '20' AND (T15."U_EXM_CARGADOA" = 'OV' OR T15."U_EXM_CARGADOA" = 'OS' OR T17."DocNum" IS NOT NULL) THEN 'COMPRA CALZADA OV'
	             WHEN T0."TransType" = '20' AND (T15."U_EXM_CARGADOA" = 'ST' OR T15."U_EXM_CARGADOA" IS NULL) THEN 'COMPRA STOCK'
	             WHEN T0."TransType" = '18' THEN 'COMPRA STOCK'
	             WHEN T0."TransType" = '59' AND ((T0."JrnlMemo" LIKE 'Recibo de prod%') OR (T0."JrnlMemo" LIKE 'Receipt % Prod%')) AND T20."ItemCode" LIKE 'A%' THEN 'SETTING'
	             WHEN T0."TransType" = '59' AND ((T0."JrnlMemo" LIKE 'Recibo de prod%') OR (T0."JrnlMemo" LIKE 'Receipt % Prod%')) AND T20."ItemCode" = T0."ItemCode" THEN 'DEVOLUCION OF'
	             WHEN T0."TransType" = '59' AND T21."FormatCode" = '20111040000' THEN 'SETTING'
                 WHEN T0."TransType" = '20' AND T21."AcctName" = 'CUENTA DE ORDEN INVENTARIO (GEN., GEN.)' THEN 'MIGRACION DE SISTEMA GET A SAP'
	             WHEN T0."TransType" = '59' AND T21."FormatCode" <> '20111040000' THEN T21."AcctName"  ELSE '' END AS "Clasificacion"
	       ,ROW_NUMBER() OVER (PARTITION BY T0."TransNum" ORDER BY  T0."DocLineNum")                               AS rn,
           TO_TIMESTAMP(
                 TO_VARCHAR(T0."CreateDate",'YYYY-MM-DD') || ' ' ||
                 LPAD(T0."DocTime", 4, '0'),
                 'YYYY-MM-DD HH24MI'
                 ) AS "FechaHora"
	FROM "SBO_MARCO_PE".OINM T0 --movimiento de inventario 
	INNER JOIN "SBO_MARCO_PE".OITM T1 --maestro de items 
	ON T0."ItemCode" = T1."ItemCode"
    
	INNER JOIN "SBO_MARCO_PE".OITB T2 --grupo de items 
	ON T1."ItmsGrpCod" = T2."ItmsGrpCod"
	LEFT JOIN "SBO_MARCO_PE".OPCH T7 -- Factura de Proveedores 
	ON CAST(T7."DocNum" AS NVARCHAR) = CAST(T0."BASE_REF" AS NVARCHAR)
	LEFT JOIN "SBO_MARCO_PE".OPDN T9 -- Entrada por Compra 
	--CAMBIO1: ON CAST(T9."DocNum" AS NVARCHAR) = CAST(T0."BASE_REF" AS NVARCHAR)
    ON T0."CreatedBy" = T9."DocEntry" AND T0."TransType" = 20
	LEFT JOIN "SBO_MARCO_PE".PDN1 T15 -- Entrada por Compra Lineas 
	ON T15."DocEntry" = T9."DocEntry" AND T15."LineNum" = T0."DocLineNum"
    
    --Relacion con detalle
    LEFT JOIN "SBO_MARCO_PE".POR1 T26
	ON T26."U_HAK_DETOC" = T15."U_HAK_DETOC" AND T26."ItemCode" = T15."ItemCode" 
    LEFT JOIN "SBO_MARCO_PE".OPOR T27
	ON T27."DocEntry" = T26."DocEntry"
    
    --Cuando no cuentan con detalle
    LEFT JOIN "SBO_MARCO_PE".POR1 T28 
	ON T28."DocEntry" = T15."BaseEntry"
    LEFT JOIN "SBO_MARCO_PE".OPOR T29
	ON T29."DocEntry" = T28."DocEntry"
    
    --Cuando no cuentan con detalle ni BaseEntry 
    LEFT JOIN "SBO_MARCO_PE"."@HAK_DETOC" T30 
	ON T30."DocEntry" = T15."U_HAK_DETOC"

	LEFT JOIN "SBO_MARCO_PE".OIGN T11 -- Entrada por Inventario 
	ON CAST(T11."DocNum" AS NVARCHAR) = CAST(T0."BASE_REF" AS NVARCHAR)
	LEFT JOIN "SBO_MARCO_PE".OWOR T16 -- Orden de Produccion 
	ON T16."DocEntry" = T15."U_EXM_OF"
	LEFT JOIN "SBO_MARCO_PE".ORDR T17 -- Orden de Venta 
	ON T17."DocEntry" = T15."U_EXM_OV"
	LEFT JOIN "SBO_MARCO_PE".WOR1 T18 -- Orden de Produccion Lineas 
	ON T18."DocEntry" = T16."DocEntry" AND T18."ItemCode" = T0."ItemCode"
	LEFT JOIN "SBO_MARCO_PE".IGN1 T19 -- Entrada por Inventario Lineas 
	ON T19."DocEntry" = T11."DocEntry" AND T19."ItemCode" = T0."ItemCode"
	LEFT JOIN "SBO_MARCO_PE".OWOR T20 -- Orden de Produccion 
	ON CAST(T19."BaseRef" AS NVARCHAR) = CAST(T20."DocNum" AS NVARCHAR)  
    LEFT JOIN "SBO_MARCO_PE".ORDR T23 -- Orden de Venta 
	ON T23."DocNum" = T20."OriginNum"
    LEFT JOIN "SBO_MARCO_PE".RDR1 T24 -- Orden de Venta Lineas 
	ON T24."DocEntry" = T23."DocEntry" AND T24."ItemCode" = T20."ItemCode"
	LEFT JOIN "SBO_MARCO_PE".OACT T21 -- Cuentas contables 
	ON T19."AcctCode" = T21."AcctCode"
	LEFT JOIN "SBO_MARCO_PE".RDR1 T22 -- Orden de Venta Lineas 
	ON T22."DocEntry" = T17."DocEntry" AND T22."ItemCode" = T0."ItemCode"
	WHERE 
    --T0."CreateDate" BETWEEN [%0] AND [%1] 
    T0."CreateDate" <= ? 
    AND T0."InQty" > 0
    AND T0."TransType" NOT IN ('67', '16', '14') -- Excluir nc de clientes, devoluciones de clientes y transferencias 
	AND ( (T0."TransType" = '18' AND COALESCE(T7."CANCELED",'N') = 'N') OR (T0."TransType" = '20' AND COALESCE(T9."CANCELED",'N') = 'N') OR (T0."TransType" = '59' AND COALESCE(T11."CANCELED",'N') = 'N') ) -- Excluir documentos cancelados 
	AND NOT (T0."TransType" = '59' AND ((COALESCE(T0."JrnlMemo",'') LIKE 'Recibo de prod%') OR (COALESCE(T0."JrnlMemo",'') LIKE 'Receipt % Prod%')) AND COALESCE(T20."ItemCode",'') NOT LIKE 'A%' AND COALESCE(T20."ItemCode",'') <> T0."ItemCode")-- Excluir recibos de produccion que no son setting ni devolucion de OF
    AND NOT (T0."TransType" = '59' AND T21."FormatCode" IN (42111090000))---Filtrar Cuentas Contables
    --AND T0."ItemCode" = 'A21100000009'
    --ORDER BY T0."CreateDate" ASC

)
SELECT 
      ROW_NUMBER() OVER (ORDER BY
                         "FechaHora" DESC,
                         "TransSeq" DESC,
                         "DocLineNum" DESC                        
                         ) AS "Numero",
      kardex.*
FROM kardex
WHERE rn = 1 -- Eliminar duplicados

    """    
    _log.info("[Kardex-MPSA] Ejecutando query...")
    cursor.execute(query, (fecha_corte_k,))
    
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    dfIncome = pd.DataFrame(results, columns=columns)
    _log.info("[Kardex-MPSA] Query OK. dfIncome filas: %d", len(dfIncome))

    cursor.close()
    connection.close()
    
    _log.info("[Kardex-MPSA] Conexión cerrada")


    #cursor.execute(query)
except Exception as e:
    _log.warning("[Kardex-MPSA] Error SAP HANA (sin datos): %s", e)
    # Mantener DataFrame vacío con columnas que Main.py usa (chunk, merge, etc.)
    _cols = [
        "Numero", "ItmsGrpNam", "TransNum", "DocLineNum", "TransType", "Tipo", "CreatedBy", "BASE_REF",
        "Numero_PO", "JrnlMemo", "ItemCode", "Dscription", "AvgPrice", "U_EXM_CARGADOA", "U_EXM_CARGADOA2",
        "FreeTxt", "OF", "Status OF", "ClienteOF", "Cantidad pendiente de OF", "LineNumOF", "OV", "Status OV",
        "OV Cancelada", "ClienteOV", "LineNumOV", "Status Linea OV", "U_EXX_CGET", "Currency", "Price",
        "TransValue", "Fecha Creacion", "Fecha Contabilizacion", "InQty", "OutQty", "Warehouse", "UserSign",
        "Comments", "CardName", "DocTime", "TransSeq", "SubLineNum", "OF Relacionada", "OV Relacionada",
        "Item_Principal", "Status Linea OV Relacionada", "OV Relacionada Cancelada", "Codigo cuenta contable",
        "Nombre cuenta contable", "Clasificacion", "rn", "FechaHora",
    ]
    dfIncome = pd.DataFrame(columns=_cols)

    
 
