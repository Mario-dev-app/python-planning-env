import pandas as pd
import datetime
import numpy as np
import os
import logging
from pathlib import Path
import getpass

_log = logging.getLogger("Codigo.Stock_0")

### Ruta base: si ya viene definida (ej. desde Main.py), se usa; si no, carpeta del usuario ###
if "base" not in dir():
    rutainicial = Path.home()
    usuario = getpass.getuser()
    antes, sep, despues = str(rutainicial).partition(usuario)
    base = Path(antes + sep)
_log.info("[Stock_0] Inicio - CalculoStock0")
exec(open(conexiones_sap/"kardex_general_prov.py", encoding="utf-8-sig").read())
_log.info("[Stock_0] KardexGeneralProv terminado. dfkardexorigen filas: %d", len(dfkardexorigen))

dfkardexHC=dfkardexorigen
#dfkardexHC=dfkardexHC[dfkardexHC["Número de artículo"] == 'A18110000108']
#df = df.sort_index(ascending=True)

#dfkardexHC=dfkardexorigen[dfkardexorigen["Nombre de grupo"]=="HIDRAULI. COMPONENTE"]
art=dfkardexHC['Número de artículo'].unique()
#dfkardexHC = dfkardexorigen[dfkardexorigen["ItemCode"] == 'A18110000123']

dfkardexHC["Cantidad de entrada"] = (
    dfkardexHC["Cantidad de entrada"]
    .astype(str)                               # Convertir a string
    .str.replace(',', '', regex=False)         # Eliminar separadores de miles
    .replace({                                  # Reemplazar valores vacíos o literales
        r'^\s*$': None,
        "None": None,
        "nan": None
    }, regex=True)
    .astype(float)                             # Convertir a número
    .fillna(0)                                 # Rellenar con 0
)

dfkardexHC["Cantidad de salida"] = (
    dfkardexHC["Cantidad de salida"]
    .astype(str)                               # Convertir a string
    .str.replace(',', '', regex=False)         # Eliminar separadores de miles
    .replace({                                  # Reemplazar valores vacíos o literales
        r'^\s*$': None,
        "None": None,
        "nan": None
    }, regex=True)
    .astype(float)                             # Convertir a número
    .fillna(0)                                 # Rellenar con 0
)


#Entrada neta por artículo y fecha
dfkardexHC["Entrada neta"] = dfkardexHC["Cantidad de entrada"] - dfkardexHC["Cantidad de salida"]
dfkardexHC.rename(columns={"CreateDate": "Fecha"}, inplace=True)
#dfkardexHC['Fecha'] = pd.to_datetime(dfkardexHC['Fecha'], dayfirst=True, errors='coerce')
dfkardexHC['Fecha'] = pd.to_datetime(dfkardexHC['Fecha'], errors='coerce')
# Excluir el ítem A18130007558 en la fecha 04/03/2019
# dfkardexHC = dfkardexHC[~((dfkardexHC["Número de artículo"] == "A18130007558") & (dfkardexHC["Fecha"] == pd.Timestamp("2019-03-04")))]

entrada_neta = dfkardexHC.groupby(["Número de artículo", "Fecha"])["Entrada neta"].sum()
entrada_neta = entrada_neta.reset_index()


# Inventario diario desde kardex
entrada_neta.sort_values(by=["Número de artículo", "Fecha"], ascending=[True, True],inplace=True)
entrada_neta['Inventario final'] = entrada_neta.groupby("Número de artículo")["Entrada neta"].cumsum()
# Reemplazar valores muy pequeños por cero
entrada_neta["Inventario final"] = np.where(
    np.abs(entrada_neta["Inventario final"]) < 1e-10,
    0,
    entrada_neta["Inventario final"]
)

"""

GENERAR TUPLAS ITEMCODE, FECHA MIN Y FECHA MAX
"""


# # Obtener fecha mínima y máxima por artículo
# rangos = entrada_neta.groupby("Número de artículo")["Fecha"].agg(["min"]).reset_index()

# # Generar rangos de fechas como listas
# rangos["Fechas"] = rangos.apply(lambda x: pd.date_range(x["min"], fecha_corte, freq="D"), axis=1)

# # Explode para deshacer listas y tener un registro por día
# df_all = rangos[["Número de artículo", "Fechas"]].explode("Fechas").rename(columns={"Fechas": "Fecha"})


"""
GENERAR LA ENTRADA NETA FINAL
"""

# Filtrar movimientos hasta fecha de corte
entrada_neta_fc = entrada_neta[entrada_neta["Fecha"] <= fecha_corte].copy()

# Ordenar correctamente
#entrada_neta_fc.sort_values(["Número de artículo", "Fecha"], inplace=True)

# Propagar inventario hacia adelante
entrada_neta_fc["Inventario final"] = (
    entrada_neta_fc.groupby("Número de artículo")["Inventario final"].ffill()
)

# Obtener el último movimiento (el inventario vigente al cierre)
ultimo_movimiento = (
    entrada_neta_fc.groupby("Número de artículo")
    .tail(1)[["Número de artículo", "Inventario final"]]
    .rename(columns={"Inventario final": "Stock_historico"})
    .reset_index(drop=True)
)

# *** FORZAR que la fecha sea la fecha cierre ***
ultimo_movimiento["Fecha"] = fecha_corte
_log.info("[Stock_0] ultimo_movimiento (DataFrame) filas: %d artículos", len(ultimo_movimiento))

# Liberar memoria eliminando los DataFrames intermedios
del dfkardexHC
del entrada_neta
#del rangos
#del df_all
#del entrada_neta_full

# Forzar liberación de memoria del recolector de basura
import gc
gc.collect()