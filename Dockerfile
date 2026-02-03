FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Zona horaria del contenedor (TZ) y del scheduler (TIMEZONE)
ENV TZ=America/Lima
ENV TIMEZONE=America/Lima

CMD ["python", "src/main.py"]
