import numpy as np
import pandas as pd
from hdbcli import dbapi
import os
import logging
from pathlib import Path
import getpass

_log = logging.getLogger("Codigo.Validacion_OV")

### Ruta base: si ya viene definida (ej. desde Main.py), se usa; si no, carpeta del usuario ###
if "base" not in dir():
    rutainicial = Path.home()
    usuario = getpass.getuser()
    antes, sep, despues = str(rutainicial).partition(usuario)
    base = Path(antes + sep)

#Importamos datas (misma carpeta Codigo)
_log.info("[Validacion_OV] Ejecutando Detalle_Ordenes_VentaV2.py")
exec(open(conexiones_sap/"detalle_ordenes_ventaV2.py", encoding="utf-8-sig").read())
_log.info("[Validacion_OV] Detalle_Ordenes_VentaV2 terminado. df_ListaDeOV_C filas: %d", len(df_ListaDeOV_C))
#Agrupar conseguir el total x articulo
df_ListaDeOV_CantEnOV = df_ListaDeOV_C[['OV_FechaCreacion','OV Cancelada','Orden de Venta','ItemCode',"OV_Cliente" ,'OV_EstadoGeneral','OV_FechaVencimiento', 'CodigoCondPAGO']].drop_duplicates()
_log.info("[Validacion_OV] df_ListaDeOV_CantEnOV filas: %d", len(df_ListaDeOV_CantEnOV))
  
#Mascara
mask_NE = df_ListaDeOV_C["NE_FechaCreacion"] > fecha_corte
mask_DV = df_ListaDeOV_C["DV_FechaCreacion"] > fecha_corte
mask_NC = df_ListaDeOV_C["NC_FechaCreacion"] > fecha_corte

    
#df_ListaDeOV = df_ListaDeOV_C
df_ListaDeOV_C["Cantidad Entregada"] = (
        df_ListaDeOV_C["Cantidad Entregada"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
        .astype(float)  # Convertir finalmente a float
        .fillna(0)
    )    
    
df_ListaDeOV_C["DV_Quantity"] = (
        df_ListaDeOV_C["DV_Quantity"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
        .astype(float)  # Convertir finalmente a float
        .fillna(0)
    )
    
df_ListaDeOV_C["Cantidad OV"] = (
        df_ListaDeOV_C["Cantidad OV"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], None)
        .astype(float)  # Convertir finalmente a float
        .fillna(0)
    )
    
df_ListaDeOV_C["NC_Quantity"] = (
        df_ListaDeOV_C["NC_Quantity"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], None)
        .astype(float)  # Convertir finalmente a float
        .fillna(0)
    )
    
    #InQty por la resta (InQty - CantidadPlanificada)

    #Ajustes
df_ListaDeOV_C.loc[mask_NE, "Cantidad Entregada"] = 0
df_ListaDeOV_C.loc[mask_DV, "DV_Quantity"] = 0
df_ListaDeOV_C.loc[mask_NC, "NC_Quantity"] = 0

df_ListaDeOV_C["Cantidad Entregada"] = df_ListaDeOV_C["Cantidad Entregada"].fillna(0)
df_ListaDeOV_C["LineNum"] = df_ListaDeOV_C["LineNum"].fillna(0)

#Se realizara 4 df
# df_OV: OV, Item , LineNum ,Cant_OV unicos
# df_NE: OV, Item , LineNum ,LineNum_NE ,Nro_NE ,Cant_NE unicos
# df_DV: OV, Item , LineNum ,LineNum_DV ,Nro_DV ,Cant_DV unicos
# df_NC: OV, Item , LineNum ,LineNum_NC ,Nro_NC ,Cant_NC unicos

cols_OV = ["Orden de Venta", "ItemCode", "LineNum","Cantidad OV"]
df_OV = df_ListaDeOV_C[cols_OV].drop_duplicates()
cols_NE = ["Orden de Venta", "ItemCode", "LineNum","NE_LineNum","NE_Numero","Cantidad Entregada"]
df_NE = df_ListaDeOV_C[cols_NE].drop_duplicates()
cols_DV = ["Orden de Venta", "ItemCode", "LineNum","DV_LineNum","DV_Numero","DV_Quantity"]
df_DV = df_ListaDeOV_C[cols_DV].drop_duplicates()
cols_NC = ["Orden de Venta", "ItemCode", "LineNum","NC_LineNum","NC_Numero","NC_Quantity"]
df_NC = df_ListaDeOV_C[cols_NC].drop_duplicates()
#Quitamos lineas vacias
df_OV = df_OV[df_OV["Orden de Venta"].notna()]
df_NE = df_NE[df_NE["NE_Numero"].notna()]
df_DV = df_DV[df_DV["DV_Numero"].notna()]
df_NC = df_NC[df_NC["NC_Numero"].notna()]
#Se realizara 3 df agrupado en base a los 3 primeros
# df_NE_agrup: OV, Item , LineNum suma Cant_NE 
# df_DV_agrup: OV, Item , LineNum suma Cant_DV 
# df_NC_agrup: OV, Item , LineNum suma Cant_NC 
df_OV_agrup = (
df_OV.groupby(["Orden de Venta", "ItemCode"], as_index=False)["Cantidad OV"]
.sum()
)
df_NE_agrup = (
df_NE.groupby(["Orden de Venta", "ItemCode"], as_index=False)["Cantidad Entregada"]
.sum()
)
df_DV_agrup = (
df_DV.groupby(["Orden de Venta", "ItemCode"], as_index=False)["DV_Quantity"]
.sum()
)
df_NC_agrup = (
df_NC.groupby(["Orden de Venta", "ItemCode"], as_index=False)["NC_Quantity"]
.sum()
)
#En base a df_OV juntamos las demas cantidaades y calculamos cant neta entregada
keys = ["Orden de Venta", "ItemCode"]
df_ListaDeOV_C = (
    df_OV_agrup
    .merge(df_NE_agrup[keys + ["Cantidad Entregada"]], on=keys, how="left")
    .merge(df_DV_agrup[keys + ["DV_Quantity"]], on=keys, how="left")
    .merge(df_NC_agrup[keys + ["NC_Quantity"]], on=keys, how="left")
)
df_ListaDeOV_C = df_ListaDeOV_C.fillna(0)
df_ListaDeOV_C.loc[df_ListaDeOV_C["Cantidad Entregada"] == 0, "NC_Quantity"] = 0
df_ListaDeOV_C['Cantidad_Neta_Entregada'] = (
        df_ListaDeOV_C['Cantidad Entregada'] - df_ListaDeOV_C['DV_Quantity'] - df_ListaDeOV_C['NC_Quantity']
    )

    #Join con lista de of para traer cant entregada
df_ListaDeOV_CantEnOV = df_ListaDeOV_CantEnOV.merge(
        df_ListaDeOV_C[['Orden de Venta', 'ItemCode','Cantidad OV','Cantidad_Neta_Entregada']],  # Selecciona solo las columnas necesarias
        left_on=['Orden de Venta','ItemCode'],
        right_on=['Orden de Venta', 'ItemCode'],
        how='left'
    )

    #Calculamos saldo    
df_ListaDeOV_CantEnOV["OV_Saldo"] = df_ListaDeOV_CantEnOV["Cantidad OV"] - df_ListaDeOV_CantEnOV["Cantidad_Neta_Entregada"]

    #Estado por linea
df_ListaDeOV_CantEnOV["OV_Estado"] = np.where(
        df_ListaDeOV_CantEnOV["OV Cancelada"].str.strip().str.upper() == "Y",
        "C",
        np.where(
            df_ListaDeOV_CantEnOV["OV_Saldo"] <= 0,
            "Ce",
            "A"
        )
    )

df_ListaDeOV_CantEnOV["Cantidad_Neta_Entregada"] = (
        df_ListaDeOV_CantEnOV["Cantidad_Neta_Entregada"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
        .astype(float)  # Convertir finalmente a float
    )
    
df_ListaDeOV_CantEnOV["Cantidad OV"] = (
        df_ListaDeOV_CantEnOV["Cantidad OV"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
        .astype(float)  # Convertir finalmente a float
    )
    
df_ListaDeOV_CantEnOV["OV_Saldo"] = (
        df_ListaDeOV_CantEnOV["OV_Saldo"]
        .astype(str)  # Convertir todos los valores a string
        .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
    #    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
        .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
        .astype(float)  # Convertir finalmente a float
    )


# Ruta local donde se guardará el archivo
#carpeta_local = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos"
#nombre_archivo = "Detalle_OV.txt"
#csv_file_path = os.path.join(carpeta_local, nombre_archivo)
# Guardar CSV localmente
#df_ListaDeOV_C.to_csv(csv_file_path, index=False, sep='	', encoding='utf-8-sig')
