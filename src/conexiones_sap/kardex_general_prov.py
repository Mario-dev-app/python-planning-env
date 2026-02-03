import pandas as pd
from hdbcli import dbapi
import os
import logging
from pathlib import Path
import socket
import getpass

_log = logging.getLogger("Codigo.KardexGeneralProv")

# Si base ya viene definida (ej. desde Main.py vía Stock_0), no sobrescribir
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

# Configuración de conexión (variable de entorno HANA_HOST / HANA_PORT)
try:
    from config import HANA_HOST, HANA_PORT
    host, port = HANA_HOST, HANA_PORT
except Exception:
    host, port = "127.0.0.1", 30015
user = "USERBI"  # Usuario de SAP HANA
password = "UserBI2025$"  # Contraseña del usuario

# Socios.py está en la misma carpeta conexiones_sap
exec(open(conexiones_sap/"socios.py", encoding="utf-8-sig").read())

# Inicializar por si falla la conexión (ej. en Docker sin HANA); así Stock_0 no falla con "not defined"
dfkardexorigen = pd.DataFrame()
_log.info("[KardexGeneralProv] Conectando a HANA en %s:%s...", host, port)

try:
    # Conexión a SAP HANA
    connection = dbapi.connect(address=host, port=port, user=user, password=password)
    _log.info("[KardexGeneralProv] Conexión exitosa a SAP HANA")

    cursor = connection.cursor()
    
    query = """

            SELECT DISTINCT T28."FormatCode" AS "Codigo cuenta contable",T2."ItmsGrpNam" as "Nombre de grupo" , T0."TransNum" as  "Número de operación" , T0."TransType" as "Clase de operación", 
            CASE 
            WHEN T0."TransType" = '13' THEN 'Factura de Clientes' 
            WHEN T0."TransType" = '14' THEN 'NC de Clientes' 
            WHEN T0."TransType" = '15' THEN 'Entrega' 
            WHEN T0."TransType" = '16' THEN 'Devolucion de Venta'
            WHEN T0."TransType" = '18' THEN 'Factura de  Proveedores'
            WHEN T0."TransType" = '19' THEN 'NC de Proveedores'  
            WHEN T0."TransType" = '20' THEN 'Entrada por Compra' 
            WHEN T0."TransType" = '21' THEN 'Devolucion por Compra' 
            WHEN T0."TransType" = '59' THEN 'Entrada por Inventario' 
            WHEN T0."TransType" = '60' THEN 'Salida por Inventario' 
            WHEN T0."TransType" = '67' THEN 'Transferencia por Inventario'
WHEN T0."TransType" = '69' THEN 'Precio de Entrega' 
WHEN T0."TransType" = '162' THEN 'revalorizacion de Inventario' 
WHEN T0."TransType" = '202' THEN 'Orden de Produccion' 
ELSE 'Revisar' END AS "Tipo",

CASE 
WHEN T0."TransType" = '13' THEN T3."CANCELED"
WHEN T0."TransType" = '14' THEN T4."CANCELED"
WHEN T0."TransType" = '15' THEN T5."CANCELED"
WHEN T0."TransType" = '16' THEN T6."CANCELED"
WHEN T0."TransType" = '18' THEN T7."CANCELED"
WHEN T0."TransType" = '19' THEN T8."CANCELED"
WHEN T0."TransType" = '20' THEN T9."CANCELED"
WHEN T0."TransType" = '21' THEN T10."CANCELED"
WHEN T0."TransType" = '59' THEN T11."CANCELED"
WHEN T0."TransType" = '60' THEN T12."CANCELED"
WHEN T0."TransType" = '67' THEN T13."CANCELED"
WHEN T0."TransType" = '69' THEN T14."Canceled"
---WHEN T0."TransType" = '162' THEN T15."CANCELED"--
---WHEN T0."TransType" = '202' THEN T16."CANCELED"--
ELSE '' END AS "Status",

T0."CreatedBy" as  "Clave de documento creada" , T0."BASE_REF" as "Referencia base" , T0."JrnlMemo" as "Comentarios" , T0."ItemCode" as "Número de artículo" , 
T0."Dscription" as "Descripción del artículo" , T15."U_EXM_CARGADOA" as "Cargado a" ,T1."U_EXX_CGET" as "Codigo GET", T0."Currency" as "Moneda del precio" , 
T0."Rate" as "Precio de la moneda" , T0."Price" as "Precio",T0."TransValue" as "Valor de transacción", T0."DocDate" as "Fecha de contabilización",T0."CreateDate", 
T0."InQty" as "Cantidad de entrada" , T0."OutQty" as "Cantidad de salida" , T0."Warehouse" as "Código de almacén" ,T0."DocLineNum",
T0."UserSign" as "Firma del usuario" ,
T0."Comments" as "Comentarios.1",T0."CardName" as  "Nombre de deudor/acreedor" , T16."GrssProfit" as "Ingreso bruto línea", T17."GrssProfit" as "GrssProfit Devol", 
T19."CardCode" as "Código de cliente",T22."ItemCode" as "ItemPrincipalOF",T25."ItemCode" as "ItemPrincipalOFReciboProd",T18."AcctCode",T28."AcctName",

COALESCE (
    T5."CardCode",
    T6."CardCode",
    T25."CardCode",
    T22."CardCode"
) AS "CódigoClienteOV_OF"

FROM "SBO_MARCO_PE"."OINM" T0
INNER JOIN "SBO_MARCO_PE"."OITM" T1 ON T0."ItemCode" = T1."ItemCode"
INNER JOIN "SBO_MARCO_PE"."OITB" T2 ON T1."ItmsGrpCod" = T2."ItmsGrpCod"
LEFT JOIN "SBO_MARCO_PE"."OINV" T3 ON T3."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."ORIN" T4 ON T4."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."ODLN" T5 ON T5."DocEntry" = T0."CreatedBy" AND T0."TransType" = '15'
LEFT JOIN "SBO_MARCO_PE"."DLN1" T16 ON T16."DocEntry" = T5."DocEntry" and T16."LineNum" = T0."DocLineNum" and T16."BaseType" = '17' and T0."ItemCode" = T16."ItemCode"
LEFT JOIN "SBO_MARCO_PE"."ORDN" T6 ON T6."DocNum" = T0."CreatedBy" AND T0."TransType" = '16'
LEFT JOIN "SBO_MARCO_PE"."RDN1" T17 ON T17."DocEntry" = T6."DocEntry" and T17."LineNum" = T0."DocLineNum" and T17."BaseType" = '15' and T0."ItemCode" = T17."ItemCode"
LEFT JOIN "SBO_MARCO_PE"."OPCH" T7 ON T7."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."ORPC" T8 ON T8."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."OPDN" T9 ON T9."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."PDN1" T15 ON T15."DocEntry" = T9."DocEntry" and T15."LineNum" = T0."DocLineNum"
LEFT JOIN "SBO_MARCO_PE"."ORPD" T10 ON T10."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."OIGN" T11 ON T11."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."OIGE" T12 ON T12."DocNum" = T0."BASE_REF"
LEFT JOIN "SBO_MARCO_PE"."IGE1" T18 ON T18."DocEntry" = T12."DocEntry" and T18."LineNum" = T0."DocLineNum" and T0."ItemCode" = T18."ItemCode"
LEFT JOIN  "SBO_MARCO_PE"."OACT" T28 ON T28."AcctCode" = T18."AcctCode"
LEFT JOIN "SBO_MARCO_PE"."OWOR" T19 ON CAST(T19."DocNum" as varchar(30)) = T18."BaseRef"
LEFT JOIN "SBO_MARCO_PE"."OWTR" T13 ON T13."DocNum" = T0."BASE_REF" 
LEFT JOIN "SBO_MARCO_PE"."OIPF" T14 ON T14."DocNum" = T0."BASE_REF"
---modificacion3
LEFT JOIN "SBO_MARCO_PE"."OIGN" T23 ON T0."TransType" = '59' AND T23."DocEntry" = T0."CreatedBy"
LEFT JOIN "SBO_MARCO_PE"."IGN1" T24 ON T23."DocEntry" = T24."DocEntry" AND T0."DocLineNum" = T24."LineNum" AND T24."BaseType" = '202'
LEFT JOIN "SBO_MARCO_PE"."OWOR" T25 ON T24."BaseEntry" = T25."DocEntry"

---modificacion2
LEFT JOIN "SBO_MARCO_PE"."OIGE" T20 ON T0."TransType" = '60' AND T0."CreatedBy" = T20."DocEntry"
LEFT JOIN "SBO_MARCO_PE"."IGE1" T21 ON T20."DocEntry" = T21."DocEntry" AND T0."DocLineNum" = T21."LineNum" AND T21."BaseType" = '202'
LEFT JOIN "SBO_MARCO_PE"."OWOR" T22 ON T21."BaseEntry" = T22."DocEntry"
---modificacion 3
LEFT JOIN "SBO_MARCO_PE"."ORDR" T26 
    ON T26."DocEntry" = T5."BaseEntry" AND T5."BaseType" = '17'
LEFT JOIN "SBO_MARCO_PE"."ORDR" T27 
    ON T27."DocEntry" = T6."BaseEntry" AND T6."BaseType" = '17'
ORDER BY T0."CreateDate" ASC

    """
    
    _log.info("[KardexGeneralProv] Ejecutando query")
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    dfkardexorigen = pd.DataFrame(results, columns=columns)
    _log.info("[KardexGeneralProv] Query OK. dfkardexorigen filas: %d", len(dfkardexorigen))

    cursor.close()
    connection.close()
    _log.info("[KardexGeneralProv] Conexión cerrada")
    
    #Cruce
    dfkardexorigen = pd.merge(
        dfkardexorigen,
        clientes[['Codigo_Cliente','Nombre_Cliente']],
        left_on='CódigoClienteOV_OF',
        right_on='Codigo_Cliente',
        how='left'
    ).drop('Codigo_Cliente', axis=1)

    #Cliente final
    dfkardexorigen["Cliente_Final"] = dfkardexorigen["Nombre de deudor/acreedor"].where(
        dfkardexorigen["Nombre de deudor/acreedor"] != "PRODUCTOS EN FABRICACION (GEN., GEN.)",
        dfkardexorigen["Nombre_Cliente"]
    )
except Exception as e:
    _log.warning("[KardexGeneralProv] Error SAP HANA (sin datos): %s", e)
    # Mantener DataFrame vacío con columnas que Stock_0 usa
    _cols = [
        "Codigo cuenta contable", "Nombre de grupo", "Número de operación", "Clase de operación", "Tipo", "Status",
        "Clave de documento creada", "Referencia base", "Comentarios", "Número de artículo", "Descripción del artículo",
        "Cargado a", "Codigo GET", "Moneda del precio", "Precio de la moneda", "Precio", "Valor de transacción",
        "Fecha de contabilización", "CreateDate", "Cantidad de entrada", "Cantidad de salida", "Código de almacén",
        "DocLineNum", "Firma del usuario", "Comentarios.1", "Nombre de deudor/acreedor", "Ingreso bruto línea",
        "GrssProfit Devol", "Código de cliente", "ItemPrincipalOF", "ItemPrincipalOFReciboProd", "AcctCode", "AcctName",
        "CódigoClienteOV_OF",
    ]
    dfkardexorigen = pd.DataFrame(columns=_cols)

    
    



    


