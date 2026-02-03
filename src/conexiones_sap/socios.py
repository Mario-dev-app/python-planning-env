import pandas as pd
from hdbcli import dbapi
import os
import logging
from pathlib import Path
import socket

_log = logging.getLogger("Codigo.Socios")

# Si base ya viene definida (ej. desde Main.py vía KardexGeneralProv), no sobrescribir
if "base" not in dir():
    hostname = socket.gethostname()
    ip_local = socket.gethostbyname(hostname)
    base = Path.home()

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

# Ruta local donde se guardará el archivo
#carpeta_local = r"C:\Users\planner01\MARCO PERUANA SA\Planeamiento de Inventarios - Documents\Archivos_Compartidos\Querys automatizados"

# Inicializar por si falla la conexión; KardexGeneralProv hace merge con clientes
clientes = pd.DataFrame(columns=['Codigo_Cliente', 'Nombre_Cliente'])
_log.info("[Socios] Conectando a HANA en %s:%s...", host, port)

try:
    # Conexión a SAP HANA
    connection = dbapi.connect(address=host, port=port, user=user, password=password)
    _log.info("[Socios] Conexión exitosa a SAP HANA")

    cursor = connection.cursor()
    
    query = """
SELECT 
    "CardCode" as "Codigo_Cliente",
    "CardName" as "Nombre_Cliente"
FROM "SBO_MARCO_PE".OCRD
---WHERE "CardType" = 'C';


    """
    
    _log.info("[Socios] Ejecutando query")
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    clientes = pd.DataFrame(results, columns=columns)
    _log.info("[Socios] Query OK. clientes filas: %d", len(clientes))

    cursor.close()
    connection.close()
    _log.info("[Socios] Conexión cerrada")

    # Crear carpeta si no existe
    #os.makedirs(carpeta_local, exist_ok=True)

    # Guardar CSV localmente
    #clientes.to_csv(csv_file_path, index=False, sep=',', encoding='utf-8-sig')
    #print(f"Archivo CSV guardado localmente en: {csv_file_path}")
except Exception as e:
    _log.warning("[Socios] Error SAP HANA (sin datos): %s", e)
    clientes = pd.DataFrame(columns=['Codigo_Cliente', 'Nombre_Cliente'])

