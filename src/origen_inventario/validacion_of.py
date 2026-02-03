import numpy as np
import pandas as pd
import os
import logging
from pathlib import Path
import getpass

_log = logging.getLogger("Codigo.Validacion_OF")

### Ruta base: si ya viene definida (ej. desde Main.py), se usa; si no, carpeta del usuario ###
if "base" not in dir():
    rutainicial = Path.home()
    usuario = getpass.getuser()
    antes, sep, despues = str(rutainicial).partition(usuario)
    base = Path(antes + sep)

_log.info("[Validacion_OF] Ejecutando detalle_ordenes_fabricacion1.py")
exec(open(conexiones_sap/"detalle_ordenes_fabricacion1.py", encoding="utf-8-sig").read())
_log.info("[Validacion_OF] detalle_ordenes_fabricacion1 terminado. dfListaDeOF filas: %d", len(dfListaDeOF))

dfListaDeOF["CantidadPlanificada"] = (
    dfListaDeOF["CantidadPlanificada"]
    .astype(str)  # Convertir todos los valores a string
    .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
#    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
    .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
    .astype(float)  # Convertir finalmente a float
)

dfListaDeOF["CantidadEmitida"] = (
    dfListaDeOF["CantidadEmitida"]
    .astype(str)  # Convertir todos los valores a string
    .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
#    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
    .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
    .astype(float)  # Convertir finalmente a float
)

dfListaDeOF["CantidadRecibida"] = (
    dfListaDeOF["CantidadRecibida"]
    .astype(str)  # Convertir todos los valores a string
    .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
#    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
    .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
    .astype(float)  # Convertir finalmente a float
)

#Calculo de Pendiente
dfListaDeOF['OF_Saldo'] =dfListaDeOF['CantidadPlanificada'] - dfListaDeOF['CantidadEmitida'] + dfListaDeOF['CantidadRecibida']
# Columna 'ESTADO'
dfListaDeOF['OF_Estado'] = np.where(dfListaDeOF['OF_Saldo'] > 0, 'A', 'Ce')
# Actualizar las canceladas
dfListaDeOF.loc[dfListaDeOF['OF_EstadoGeneral'] == 'C', 'OF_Estado'] = 'C'

dfListaDeOF["OF_Saldo"] = (
    dfListaDeOF["OF_Saldo"]
    .astype(str)  # Convertir todos los valores a string
    .str.replace(',', '', regex=False)  # Remover separadores de miles (coma)
#    .replace(r'^\s*$', None, regex=True)  # Reemplazar valores vacíos o espacios con None (para que sean NaN)
    .replace(['', ' ', 'None', 'none', 'NULL', 'NaN'], 0)
    .astype(float)  # Convertir finalmente a float
)
