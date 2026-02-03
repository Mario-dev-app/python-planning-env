import pandas as pd
from hdbcli import dbapi
import os
import sys
import logging
import numpy as np
##Descarga automatica de archivos##
import requests
from pathlib import Path
import getpass

_log = logging.getLogger("Codigo.Main")

### Ruta base = misma carpeta (Codigo); permite importar config/db del proyecto ###
_log.info("[Main] Inicio - cargando ruta base y fecha de corte")
base = Path(__file__).resolve().parent
conexiones_sap = base.parent / "conexiones_sap"
sys.path.insert(0, str(base.parent))
#Definimos la fecha de corte
# Fecha de corte de forma variable
fecha_corte = pd.to_datetime('today').normalize() - pd.Timedelta(days=1)
#fecha_corte = pd.to_datetime('2026-02-02')
fecha_corte_k = fecha_corte.date()
_log.info("[Main] Fecha de corte: %s", fecha_corte_k)

#Importamos datas (archivos en la misma carpeta Codigo)
_log.info("[Main] Paso 1/4 - Ejecutando Validacion_OV.py")
exec(open(base/"validacion_ov.py", encoding="utf-8-sig").read())
_log.info("[Main] Validacion_OV terminado. df_ListaDeOV_CantEnOV filas: %s", len(df_ListaDeOV_CantEnOV) if 'df_ListaDeOV_CantEnOV' in dir() else 'N/A')

# Ruta local donde se guardará el archivo
#carpeta_local = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos"
#nombre_archivo = "Validacion_OV.txt"
#csv_file_path = os.path.join(carpeta_local, nombre_archivo)
# Guardar CSV localmente
#df_ListaDeOV_CantEnOV.to_csv(csv_file_path, index=False, sep='	', encoding='utf-8-sig')

_log.info("[Main] Paso 2/4 - Ejecutando Validacion_OF.py")
exec(open(base/"validacion_of.py", encoding="utf-8-sig").read())
_log.info("[Main] Validacion_OF terminado. dfListaDeOF filas: %s", len(dfListaDeOF) if 'dfListaDeOF' in dir() else 'N/A')

# # Ruta local donde se guardará el archivo
# carpeta_local2 = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos"
# nombre_archivo2 = "Validacion_OF.txt"
# csv_file_path2 = os.path.join(carpeta_local2, nombre_archivo2)
# # Guardar CSV localmente
# dfListaDeOF.to_csv(csv_file_path2, index=False, sep='	', encoding='utf-8-sig')

_log.info("[Main] Paso 3/4 - Ejecutando Stock_0.py")
exec(open(base/"stock_0.py", encoding="utf-8-sig").read())
_um = ultimo_movimiento
_log.info("[Main] Stock_0 terminado. ultimo_movimiento: %s", len(_um) if hasattr(_um, '__len__') and not isinstance(_um, dict) else ("dict con %d artículos" % len(_um) if isinstance(_um, dict) else "N/A"))

# # Ruta local donde se guardará el archivo
# carpeta_local2 = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos"
# nombre_archivo2 = "ultimomov.txt"
# csv_file_path2 = os.path.join(carpeta_local2, nombre_archivo2)
# # Guardar CSV localmente
# ultimo_movimiento.to_csv(csv_file_path2, index=False, sep='	', encoding='utf-8-sig')

_log.info("[Main] Paso 4/4 - Ejecutando Kardex-MPSA Version AP prueba.py")
exec(open(conexiones_sap/"kardex_mpsa_version_ap_prueba.py", encoding="utf-8-sig").read())
_log.info("[Main] Kardex-MPSA terminado. dfIncome filas: %s", len(dfIncome) if 'dfIncome' in dir() else 'N/A')

#ultimo_movimiento=ultimo_movimiento[ultimo_movimiento["Número de artículo"]=="A20070000041"]
"""codigos = [
"A21100000009"
]

ultimo_movimiento = ultimo_movimiento[ultimo_movimiento['Número de artículo'].isin(codigos)]"""

# Asegurar que InQty sea numerico
ultimo_movimiento["Stock_historico"] = (
    ultimo_movimiento["Stock_historico"]
    .astype(str)  # Convertir todos los valores a string
    .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
#    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
    .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
    .astype(float)  # Convertir finalmente a float
)

ultimo_movimiento_rev = ultimo_movimiento.copy()
ultimo_movimiento_rev["Fecha_corte"] = fecha_corte
# # Ruta local donde se guardará el archivo
#carpeta_local2 = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos\Resultados\Origen de Inventario\Stock0"
#nombre_archivo2 = f"ultimo_movimiento_rev_{fecha_corte:%Y%m%d}.txt"
#csv_file_path2 = os.path.join(carpeta_local2, nombre_archivo2)
# # Guardar CSV localmente
#ultimo_movimiento_rev.to_csv(csv_file_path2, index=False, sep='	', encoding='utf-8-sig')

ultimo_movimiento = ultimo_movimiento.set_index('Número de artículo')['Stock_historico'].to_dict()
#ultimo_movimiento = {k: v for k, v in ultimo_movimiento.items() if k == "A18110001961"}
ultimo_movimiento = {k: float(v) for k, v in ultimo_movimiento.items()}
_log.info("[Main] ultimo_movimiento (dict) artículos: %d", len(ultimo_movimiento))
 
# -----------------------------
# Configuración
# -----------------------------
consumido = {k: 0.0 for k in ultimo_movimiento}
pendientes = set(ultimo_movimiento.keys())
resultados = {art: [] for art in ultimo_movimiento}
_log.info("[Main] pendientes (artículos a cubrir): %d", len(pendientes))

# asignamos dfIncome a chunk
chunk=dfIncome[dfIncome["ItemCode"].isin(pendientes)]
_log.info("[Main] chunk (dfIncome filtrado por pendientes) filas: %d", len(chunk))
#chunk = chunk[chunk["Item_Principal"] == "A19010000278"]

# Asegurar que InQty sea numerico
chunk["InQty"] = (
    chunk["InQty"]
    .astype(str)  # Convertir todos los valores a string
    .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
#    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
    .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
    .astype(float)  # Convertir finalmente a float
)

# Rellenamos algunos valores
cols = ['OF','Status OF','ClienteOF','Cantidad pendiente de OF']
# Crear columnas auxiliares que solo tienen valor cuando U_EXM_CARGADOA = "OF"
mask = chunk["U_EXM_CARGADOA"] == "OF"
aux = chunk[mask][cols]
if not aux.empty:
    # Rellenar SOLO dentro del grupo y solo en filas válidas
    filled = aux.groupby(chunk["BASE_REF"]).transform(lambda x: x.ffill().bfill())
    # Reemplazar valores originales SOLO para filas donde se aplicaba la condición
    chunk.loc[mask, cols] = filled
# Segmentos
df_ov = chunk[chunk["Clasificacion"] == "COMPRA CALZADA OV"].copy()
df_of = chunk[chunk["Clasificacion"] == "COMPRA CALZADA OF"].copy()
df_rest = chunk[~chunk["Clasificacion"].isin(["COMPRA CALZADA OV", "COMPRA CALZADA OF"])].copy()
# Funcion de agrupaciobn

def agrupar_si_repite(df, group_cols):

    #Filas con claves válidas (NO vacías ni NaN)
    df_valid = df[
        df[group_cols].notna().all(axis=1) &
        (df[group_cols] != "").all(axis=1)
    ].copy()

    #Filas inválidas → se quedan tal cual
    df_invalid = df.drop(df_valid.index).copy()

    #Agrupación normal SOLO sobre válidos
    g = df_valid.groupby(group_cols, as_index=False)

    df_multi = g.filter(lambda x: len(x) > 1)
    df_single = g.filter(lambda x: len(x) == 1)

    df_agrupado = (
        df_multi
        .groupby(group_cols, as_index=False)
        .agg({
            "InQty": "sum",
            **{c: "last" for c in df.columns if c not in group_cols + ["InQty"]}
        })
    )

    #Unir todo
    return pd.concat(
        [df_invalid, df_single, df_agrupado],
        ignore_index=True
    )

#Ejecucion de OV
df_ov_final = agrupar_si_repite(
    df_ov,
    group_cols=["OV", "ItemCode"]
)
#Ejecucion de OF
df_of_final = agrupar_si_repite(
    df_of,
    group_cols=["OF", "ItemCode"]
)
#df limpio
chunk = pd.concat(
    [df_rest, df_ov_final, df_of_final],
    ignore_index=True
)


# Normalizar claves para merge OV
chunk["OV"] = pd.to_numeric(chunk["OV"], errors="coerce").astype("Int64")
chunk["OV Relacionada"] = pd.to_numeric(chunk["OV Relacionada"], errors="coerce").astype("Int64")
df_ListaDeOV_CantEnOV["Orden de Venta"] = pd.to_numeric(
    df_ListaDeOV_CantEnOV["Orden de Venta"], errors="coerce"
).astype("Int64")

#Cruzamos para datos de OF relacionada a setting
df_ListaDeOV_CantEnOV.rename(columns={'ItemCode': 'ItemCode_OV'}, inplace=True)

chunk = chunk.merge(
      df_ListaDeOV_CantEnOV[['Orden de Venta', 'ItemCode_OV', 'OV_Estado']],  # Selecciona solo las columnas necesarias
      left_on=['OV Relacionada', 'Item_Principal'],
      right_on=['Orden de Venta','ItemCode_OV'],
      how='left'
)

#chunk_2 = chunk[chunk["Item_Principal"] == "A18110000260"]
#chunk_2 = chunk[chunk["OV Relacionada"] == "259902762"]

chunk.rename(columns={'OV_Estado': 'OV_Rel_Estado'}, inplace=True)
chunk = chunk.drop(columns=['Orden de Venta','ItemCode_OV'])
chunk.loc[chunk["OV Relacionada Cancelada"] == "Y", "OV_Rel_Estado"] = "C"

chunk = chunk.merge(
      df_ListaDeOV_CantEnOV[['Orden de Venta', 'ItemCode_OV', 'OV_Estado','OV_FechaCreacion', "OV_Cliente" ,'OV_EstadoGeneral','OV_FechaVencimiento', 'Cantidad OV', 'OV_Saldo',"Cantidad_Neta_Entregada"]],  # Selecciona solo las columnas necesarias
      left_on=['OV', 'ItemCode'],
      right_on=['Orden de Venta','ItemCode_OV'],
      how='left'
)
chunk = chunk.drop(columns=['Orden de Venta','ItemCode_OV'])

chunk.loc[chunk["OV Cancelada"] == "Y", "OV_Estado"] = "C"

#columna adicional de cantidad final
idx = chunk.columns.get_loc("InQty") + 1
chunk.insert(idx, "Cant Final", chunk["InQty"])

### ORDENES DE VENTA CONDICIONALES ###
"""
#Caso 1 : Si Cant InQyt = Cant OV entonces InQyt = Saldo
ov_cond1 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OV") &
    (chunk["InQty"] == chunk["Cantidad OV"])
)

chunk.loc[ov_cond1, "Cant Final"] = chunk.loc[ov_cond1, "OV_Saldo"]
#Caso 2 : Si  Cant InQyt < Cant OV
ov_cond2 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OV") &
    (chunk["InQty"] < chunk["Cantidad OV"])
)
#Caso 2A : Si  Cant OV - Saldo >=  Cant InQyt entonces 0
ov_cond2A = (
    (ov_cond2) &
    (chunk["Cantidad OV"]-chunk["OV_Saldo"]>=chunk["InQty"])
)
chunk.loc[ov_cond2A, "Cant Final"] = 0
#Caso 2B : Si  Cant OV - Saldo <  Cant InQyt  entonces  Saldo - Inqyt// Inqyt - Cant_Emitida 
ov_cond2B = (
    (ov_cond2) &
    (chunk["Cantidad OV"]-chunk["OV_Saldo"] < chunk["InQty"])
)
chunk.loc[ov_cond2B, "Cant Final"] = chunk.loc[ov_cond2B, "OV_Saldo"]- chunk.loc[ov_cond2B, "Cantidad_Neta_Entregada"]
#Caso 3 : Si  Cant InQyt > Cant OV  Cant InQyt entonces InQyt = (InQyt - Cant OV) + Saldo
ov_cond3 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OV") &
    (chunk["InQty"]  > chunk["Cantidad OV"])
)
chunk.loc[ov_cond3, "Cant Final"] = ((chunk.loc[ov_cond3, "InQty"] - chunk.loc[ov_cond3, "Cantidad OV"]) + chunk.loc[ov_cond3, "OV_Saldo"])
"""
ov_cond1 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OV") &
    (chunk["InQty"].notna()) &
    (chunk["InQty"] != "")
)
chunk.loc[ov_cond1, "Cant Final"] = np.maximum(0,chunk.loc[ov_cond1, "InQty"]- chunk.loc[ov_cond1, "Cantidad_Neta_Entregada"])

### ORDENES DE FABRICACION CONDICIONALES ###   
#chunk["OF"] = chunk["OF"].astype(str)
#dfListaDeOF["OF_Numero"] = dfListaDeOF["OF_Numero"].astype(str)
chunk["OF"] = pd.to_numeric(chunk["OF"], errors="coerce")
dfListaDeOF["OF_Numero"] = pd.to_numeric(dfListaDeOF["OF_Numero"], errors="coerce")

#dfListaDeOF.dtypes
chunk = chunk.merge(
      dfListaDeOF[['OF_Numero', 'ItemCode','CantidadPlanificada','OF_Estado','OF_FechaCreacion','OF_EstadoGeneral','OF_Cliente', 'OF_Saldo',"CantidadEmitida","CantidadRecibida"]],  # Selecciona solo las columnas necesarias
      left_on=['OF', 'ItemCode'],
      right_on=['OF_Numero','ItemCode'],
      how='left'
)
chunk = chunk.drop(columns=['OF_Numero'])
chunk.loc[chunk["Status OF"] == "C", "OF_Estado"] = "C"
"""
#Caso 1 : Si Cant InQyt = Cant Plan entonces InQyt = OF_Saldo
of_cond1 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OF") &
    (chunk["InQty"] == chunk["CantidadPlanificada"])
)

chunk.loc[of_cond1, "Cant Final"] = chunk.loc[of_cond1, "OF_Saldo"]
#Caso 2 : Si  Cant InQyt < Cant Plan
of_cond2 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OF") &
    (chunk["InQty"] < chunk["CantidadPlanificada"])
)
#Caso 2A : Si  Cant Plan - OF_Saldo >=  Cant InQyt entonces 0
of_cond2A = (
    (of_cond2) &
    (chunk["CantidadPlanificada"]-chunk["OF_Saldo"]>=chunk["InQty"])
)
chunk.loc[of_cond2A, "Cant Final"] = 0
#Caso 2B : Si  Cant Plan - OF_Saldo <  Cant InQyt  entonces  OF_Saldo - Inqyt// Saldo - Cant_Emitida
of_cond2B = (
    (of_cond2) &
    (chunk["CantidadPlanificada"]-chunk["OF_Saldo"] < chunk["InQty"])
)
chunk.loc[of_cond2B, "Cant Final"] = (chunk.loc[of_cond2B, "OF_Saldo"] - chunk.loc[of_cond2B, "CantidadEmitida"])
#Caso 3 : Si  Cant InQyt > Cant Plan  Cant InQyt entonces InQyt = InQyt - Cant Plan + OF_Saldo
of_cond3 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OF") &
    (chunk["InQty"]  > chunk["CantidadPlanificada"])
)

chunk.loc[of_cond3, "Cant Final"] = ((chunk.loc[of_cond3, "InQty"] - chunk.loc[of_cond3, "CantidadPlanificada"])+ chunk.loc[of_cond3, "OF_Saldo"])
"""
of_cond1 = (
    (chunk["Clasificacion"] == "COMPRA CALZADA OF") &
    (chunk["InQty"].notna()) &
    (chunk["InQty"] != "")
)
chunk.loc[of_cond1, "Cant Final"] = np.maximum(0,chunk.loc[of_cond1, "InQty"]- (chunk.loc[of_cond1, "CantidadRecibida"] - chunk.loc[of_cond1, "CantidadEmitida"]))

# --- Filtrar registros donde InQty quedó en 0 ---
#chunk = chunk[chunk["Cant Final"] != 0]

#Casos Observados
#OF
chunk.loc[
    (chunk["Clasificacion"] == "COMPRA CALZADA OF") & 
    ((chunk["OF_Estado"].isna()) | (chunk["OF_Estado"].str.strip() == '')),
    "OF_Estado"
] = "A"

#OV
chunk.loc[
    (chunk["Clasificacion"] == "COMPRA CALZADA OV") & 
    ((chunk["OV_Estado"].isna()) | (chunk["OV_Estado"].str.strip() == '')),
    "OV_Estado"
] = "A"


#No cambiar este orden , ni colocar orden posterior
chunk['FechaHora'] = pd.to_datetime(chunk['FechaHora'])
chunk['TransSeq'] = pd.to_numeric(chunk['TransSeq'], errors='coerce')
chunk['DocLineNum'] = pd.to_numeric(chunk['DocLineNum'], errors='coerce')
chunk = chunk.sort_values(by=['FechaHora', 'TransSeq', 'DocLineNum'],ascending=[False, False, False])
_log.info("[Main] Procesando bucle chunk.iterrows() (%d filas)...", len(chunk))

for _, row in chunk.iterrows():
            art = row["ItemCode"]
            if art not in pendientes:
                continue
  
            #Convertir todo a float desde el principio
            objetivo = float(ultimo_movimiento[art])
            usado = float(consumido[art])

            if usado >= objetivo:
                pendientes.remove(art)
                continue

            cantidad = float(row["Cant Final"])
            tomar = min(cantidad, objetivo - usado)
            
            resultados[art].append({ 
                "Num":row["Numero"],
                "Ejecucion": fecha_corte,
                "Fecha Movimiento": row["Fecha Creacion"],
                "Tipo": row["Clasificacion"],
                "InQty":row["InQty"],
                "Cant Final":row["Cant Final"],
                "Cantidad_tomada": tomar,
                "OF Setting":row["OF Relacionada"],
                "OV Setting":row["OV Relacionada"],
                "OV Setting Estado":row["OV_Rel_Estado"],
                "Lines Status OV Setting":row["Status Linea OV Relacionada"],
                "FechaCreacion OV":row["OV_FechaCreacion"],
                "FechaCreacion OF":row["OF_FechaCreacion"],
                "OV": row["OV"] ,
                "OF": row["OF"] ,
                "Cant OV":row["Cantidad OV"],
                "Cant OF":row["CantidadPlanificada"],
                "Saldo OV":row["OV_Saldo"],
                "Saldo OF":row["OF_Saldo"],
                "Estado OV":row["OV_Estado"],
                "Estado OF":row["OF_Estado"],
                "Lines Status OV":row["Status Linea OV"],
                "EstadoGeneral OV":row["OV_EstadoGeneral"] ,
                "EstadoGeneral OF":row["OF_EstadoGeneral"] ,
                "Cliente OV":row["OV_Cliente"] ,
                "Cliente OF":row["OF_Cliente"] ,
                "FechaVencimiento OV":row["OV_FechaVencimiento"],
                "Valor_Un_Soles": float(row["AvgPrice"]),
                "Linea":row["ItmsGrpNam"],
                "PO":row["Numero_PO"] if row["Clasificacion"] == "COMPRA STOCK" else None,
                "Proveedor":row["CardName"] if row["Clasificacion"] == "COMPRA STOCK" else None,
                "Cargado a":row["U_EXM_CARGADOA"] if row["Clasificacion"] == "COMPRA STOCK" else None,
                "Cargado a 2":row["U_EXM_CARGADOA2"] if row["Clasificacion"] == "COMPRA STOCK" else None,
                "Texto libre":row["FreeTxt"] if row["Clasificacion"] == "COMPRA STOCK" else None   
            })

            consumido[art] += tomar

            if consumido[art] >= objetivo:
                pendientes.remove(art)     
   
    # -----------------------------
    # Guardar resultados finales
    # -----------------------------
    # os.makedirs(carpeta_local, exist_ok=True)
    
    # Convertir resultados a DataFrame completo (vacío si no hay datos de HANA)
total_resultados = sum(len(v) for v in resultados.values())
_log.info("[Main] Bucle terminado. Registros en resultados: %d | artículos con datos: %d", total_resultados, sum(1 for v in resultados.values() if v))
if not resultados:
    _log.info("[Main] resultados vacío -> df_final vacío (sin datos de HANA/Income)")
    _cols_final = [
        "ItemCode", "Num", "Ejecucion", "Fecha Movimiento", "Tipo", "InQty", "Cant Final", "Cantidad_tomada",
        "OF Setting", "OV Setting", "OV Setting Estado", "Lines Status OV Setting", "FechaCreacion OV",
        "FechaCreacion OF", "OV", "OF", "Cant OV", "Cant OF", "Saldo OV", "Saldo OF", "Estado OV", "Estado OF",
        "Lines Status OV", "EstadoGeneral OV", "EstadoGeneral OF", "Cliente OV", "Cliente OF", "FechaVencimiento OV",
        "Valor_Un_Soles", "Linea", "PO", "Proveedor", "Cargado a", "Cargado a 2", "Texto libre",
    ]
    df_final = pd.DataFrame(columns=_cols_final)
else:
    df_final = pd.concat([pd.DataFrame(resultados[art]).assign(ItemCode=art) for art in resultados])
    df_final = df_final.sort_values(by=["ItemCode", "Num"], ascending=[True, False])
    col = df_final.pop("ItemCode")   # Saca la columna del DF
    df_final.insert(0, "ItemCode", col)   # La inserta en la posición 0 (primera)
    _log.info("[Main] df_final construido: %d filas", len(df_final))

#Quitamos lineas no deseadas
lineas_excluir = ['HERRAMIENTAS','SERVICIOS MANGUERAS', 'ACTIVOS FIJOS', 'SUMINISTRO COMPUTO','UTILES DE OFICINA', 'UNIFORMES Y EQ. SEGU', 'ACCESORIOS DE LIMPIE', 'SERVICIOS HERRAMIENT']
if not df_final.empty and 'Linea' in df_final.columns:
    df_final = df_final[~df_final['Linea'].isin(lineas_excluir)]

#Cruce con DB_Rotacion
# Primer día del mes
primer_dia_mes = fecha_corte.replace(day=1)
# Último día del mes anterior
ultimo_dia_mes_anterior = primer_dia_mes - pd.Timedelta(days=1)

archivos = [
    ("https://sistemasmarco-my.sharepoint.com/:x:/g/personal/aprado_marco_com_pe/IQBOe8XzXnkhRZQ3aXNecuvkATXYriu-rTOb067Erp07Sq0?download=1", "BD Rotacion.xlsx"),
    ("https://sistemasmarco-my.sharepoint.com/:x:/g/personal/aprado_marco_com_pe/IQDse3MNonwoT5K0wI084KHTAYOmQMgA2V-aFBdPze1rmvU?download=1", "Maestro_Art.xlsx")
]

# Rutas en la misma carpeta Codigo
carpeta_destino = base / "Datas"
ruta_excel = base / "Datas"

# Crear la carpeta si no existe
os.makedirs(carpeta_destino, exist_ok=True)

# Descargar cada archivo
_log.info("[Main] Descargando Excel (SharePoint)...")
for url, nombre_archivo in archivos:
    ruta_archivo = os.path.join(carpeta_destino, nombre_archivo)
    try:
        respuesta = requests.get(url, stream=True)
        respuesta.raise_for_status()  # Lanza error si la descarga falla

        # Guardar archivo (sobrescribe si ya existe)
        with open(ruta_archivo, "wb") as archivo:
            for chunk in respuesta.iter_content(chunk_size=8192):
                archivo.write(chunk)

    except requests.exceptions.RequestException as e:
        
        _log.error("[Main] Error descargando/cargando Excel: %s", e)
        
## Reporte  - Maestro_Art ##
_log.info("[Main] Cargando Maestro_Art.xlsx y BD Rotacion.xlsx")
try:
    dfMaestro_Art = pd.read_excel(ruta_excel/'Maestro_Art.xlsx', sheet_name="Hoja1")
    _log.info("[Main] Maestro_Art.xlsx cargado: %d filas", len(dfMaestro_Art))
except Exception as e:
    _log.warning("[Main] No se pudo cargar Maestro_Art.xlsx: %s", e)
    dfMaestro_Art = pd.DataFrame(columns=['Codigo Concateando', 'Codigo Unico'])
df_final["Conc_Art"] = "MP" + df_final["ItemCode"].astype(str)
df_final = df_final.merge(
    dfMaestro_Art[['Codigo Concateando', 'Codigo Unico']],
    left_on='Conc_Art',
    right_on='Codigo Concateando',
    how='left'
)
df_final['Codigo Unico'] = df_final['Codigo Unico'].fillna(df_final['Conc_Art'])
df_final.drop(columns=['Codigo Concateando'], inplace=True)
## Reporte  - BD Rotacion ##
try:
    dfBDRotacion = pd.read_excel(ruta_excel/'BD Rotacion.xlsx', sheet_name="Hoja1")
    dfBDRotacion['Periodo'] = pd.to_datetime(dfBDRotacion['Periodo'])
except Exception as e:
    _log.warning("[Main] No se pudo cargar BD Rotacion.xlsx: %s", e)
    dfBDRotacion = pd.DataFrame(columns=['Periodo', 'Concatenado', 'Maestro categorias.Categoría'])
if not dfBDRotacion.empty:
    _log.info("[Main] BD Rotacion.xlsx cargado: %d filas", len(dfBDRotacion))


#dfBDRotacion = dfBDRotacion[dfBDRotacion['Periodo'] == ultimo_dia_mes_anterior]

# Determinar el periodo a usar
if ultimo_dia_mes_anterior in dfBDRotacion['Periodo'].values:
    periodo_filtro = ultimo_dia_mes_anterior
else:
    periodo_filtro = dfBDRotacion['Periodo'].max()

# Filtrar dataframe
dfBDRotacion = dfBDRotacion[dfBDRotacion['Periodo'] == periodo_filtro]

#dfBDRotacion = dfBDRotacion[dfBDRotacion['Sociedad'] == 'MP']

df_final = df_final.merge(
    dfBDRotacion[['Concatenado', 'Maestro categorias.Categoría']],
    left_on='Conc_Art',
    right_on='Concatenado',
    how='left'
)

df_final['Maestro categorias.Categoría'] = df_final['Maestro categorias.Categoría'].fillna('NULA')

#Agregamos antiguedad
df_final['Fecha Movimiento'] = pd.to_datetime(df_final['Fecha Movimiento'], errors='coerce')
df_final['Años_Antiguedad'] = ((fecha_corte - df_final['Fecha Movimiento']).dt.days // 365)

def clasificar_antiguedad(a):
    if pd.isna(a):
        return "[0-1]"
    elif a < 1:
        return "[0-1]"
    elif a < 2:
        return "<1-2]"
    elif a < 3:
        return "<2-3]"
    elif a < 4:
        return "<3-4]"
    elif a < 5:
        return "<4-5]"
    else:
        return "<5-más]"

df_final['Antiguedad'] = df_final['Años_Antiguedad'].apply(clasificar_antiguedad)

#df_final = df_final.drop_duplicates() #agregado por AP
#df_final.drop(columns=['Articulo'], inplace=True)
#df_final = df_final.drop(columns=['Comentarios'])

_log.info("[Main] Script de distribucion finalizado. df_final: %d filas", len(df_final))

carpeta_local = base
nombre_archivo = f"Resultado_{fecha_corte:%Y%m%d}.txt"
csv_file_path = base / nombre_archivo
os.makedirs(carpeta_local, exist_ok=True)
# Guardar TXT localmente
df_final.to_csv(csv_file_path, index=False, sep='	', encoding='utf-8-sig')
_log.info("[Main] TXT guardado: %s", csv_file_path)

# Insertar todo el contenido del resultado en PostgreSQL (una columna por campo)
_log.info("[Main] Iniciando inserción en PostgreSQL...")

try:
    from db import execute, transaction

    n_filas = len(df_final)
    _log.info("Preparando inserción en PostgreSQL: %d filas (fecha_corte=%s)", n_filas, fecha_corte.strftime("%Y-%m-%d"))

    if n_filas == 0:
        _log.warning("[Main] PostgreSQL: df_final está vacío -> no se insertan filas.")
    else:
        def _serializar(v):
            if pd.isna(v):
                return None
            if hasattr(v, "isoformat"):
                return v.isoformat()
            if isinstance(v, (np.integer, np.int64, np.int32)):
                return int(v)
            if isinstance(v, (np.floating, np.float64, np.float32)):
                return float(v)
            return str(v) if v is not None else None

        # Columnas del DataFrame (nombres con espacios o puntos se guardan entre comillas en PG)
        columnas_df = list(df_final.columns)

        # Crear tabla con una columna por campo (todas TEXT para evitar errores de tipo)
        col_defs = [
            "id SERIAL PRIMARY KEY",
            "fecha_corte DATE NOT NULL",
            "fecha_carga TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP",
        ]
        for c in columnas_df:
            # Escapar comillas dobles en el nombre de columna
            col_esc = '"' + str(c).replace('"', '""') + '"'
            col_defs.append(f"{col_esc} TEXT")

        create_sql = "CREATE TABLE IF NOT EXISTS origen_inventario (\n    " + ",\n    ".join(col_defs) + "\n);"
        execute(create_sql)

        # INSERT con una columna por campo
        columnas_insert = ["fecha_corte"] + columnas_df
        placeholders = ", ".join(["%s"] * len(columnas_insert))
        cols_quoted = ", ".join('"' + str(c).replace('"', '""') + '"' for c in columnas_insert)
        insert_sql = f"INSERT INTO origen_inventario ({cols_quoted}) VALUES ({placeholders})"

        fecha_corte_str = fecha_corte.strftime("%Y-%m-%d")
        insertadas = 0
        with transaction() as conn:
            with conn.cursor() as cur:
                for _, row in df_final.iterrows():
                    valores = [fecha_corte_str] + [_serializar(row[c]) for c in columnas_df]
                    cur.execute(insert_sql, valores)
                    insertadas += 1
        _log.info("[Main] PostgreSQL: insertadas %d filas en resultado_origen_inventario.", insertadas)
except Exception as e:
    _log.exception("[Main] ERROR al insertar en PostgreSQL: %s", e)


    
    


