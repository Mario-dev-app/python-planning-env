import pandas as pd
from pathlib import Path

# Ruta a conexiones_sap (mismo nivel que origen_inventario)
_conexiones_sap = Path(__file__).resolve().parent.parent / "conexiones_sap"
# KardexGeneralProv requiere base y conexiones_sap en el scope; base = origen_inventario
base = Path(__file__).resolve().parent
conexiones_sap = _conexiones_sap
exec(open(_conexiones_sap/"kardex_general_prov.py", encoding="utf-8-sig").read())


# Columnas de fecha sean tipo datetime
dfkardexorigen['Fecha de contabilización'] = pd.to_datetime(dfkardexorigen['Fecha de contabilización'])
dfkardexorigen['CreateDate'] = pd.to_datetime(dfkardexorigen['CreateDate'])

#Filtrar hasta la fecha de corte
df_filtrado = dfkardexorigen[dfkardexorigen['CreateDate'] <= fecha_corte]

#Ordenar cronológicamente
df_filtrado = df_filtrado.sort_values(
    by=['Número de artículo', 'Fecha de contabilización', 'CreateDate', 'Número de operación', 'DocLineNum']
)

df_filtrado["Dif"]=df_filtrado["Cantidad de entrada"] - df_filtrado["Cantidad de salida"]

# Obtener el último movimiento por artículo
ultimo_movimiento = df_filtrado.groupby('Número de artículo').tail(1).reset_index(drop=True)

# Seleccionamos solo las columnas que queremos
ultimo_movimiento = ultimo_movimiento[['Número de artículo', 'Stock_historico']]




