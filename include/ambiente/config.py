"""Configuracion del pipeline de calidad del aire.

Rutas del CONTENEDOR (nunca C:\\...). El bind mount traduce:

    .\\data  ->  /opt/airflow/data
"""

from __future__ import annotations

import os
from pathlib import Path

# --- Rutas -----------------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
AIRE_DIR = DATA_DIR / "aire"

BRONZE_DIR = AIRE_DIR / "bronze"
SILVER_DIR = AIRE_DIR / "silver"
GOLD_DIR = AIRE_DIR / "gold"
WAREHOUSE_DIR = AIRE_DIR / "warehouse"
WAREHOUSE_DB = WAREHOUSE_DIR / "calidad_aire.db"

TABLA_WAREHOUSE = "gold_calidad_aire"

# --- Fuente de datos -------------------------------------------------------
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
CIUDAD = os.getenv("AIRE_CIUDAD", "Cali, Colombia")
LATITUD = float(os.getenv("AIRE_LATITUD", "3.4516"))
LONGITUD = float(os.getenv("AIRE_LONGITUD", "-76.5320"))
ZONA_HORARIA = "America/Bogota"

# dias maximos que permite la API para datos pasados
DIAS_MAXIMOS = 92

# --- Variables -------------------------------------------------------------
# clave = nombre del parametro en la API de Open-Meteo
# columna = nombre que usamos en Silver/Gold
# rango = limites fisicos razonables para descartar valores corruptos
VARIABLES: dict[str, dict] = {
    "pm2_5": {"columna": "pm2_5", "unidad": "µg/m³", "min": 0.0, "max": 1000.0},
    "pm10": {"columna": "pm10", "unidad": "µg/m³", "min": 0.0, "max": 1500.0},
    "carbon_monoxide": {"columna": "co", "unidad": "µg/m³", "min": 0.0, "max": 50000.0},
    "nitrogen_dioxide": {"columna": "no2", "unidad": "µg/m³", "min": 0.0, "max": 2000.0},
    "sulphur_dioxide": {"columna": "so2", "unidad": "µg/m³", "min": 0.0, "max": 2000.0},
    "ozone": {"columna": "o3", "unidad": "µg/m³", "min": 0.0, "max": 1000.0},
    "us_aqi": {"columna": "aqi_us", "unidad": "AQI", "min": 0.0, "max": 500.0},
    "european_aqi": {"columna": "aqi_europa", "unidad": "AQI", "min": 0.0, "max": 500.0},
}

# Variables que se agregan a nivel diario en Gold
VARIABLES_AGREGADAS = ["pm2_5", "pm10", "co", "no2", "o3", "aqi_us"]

# --- Reglas de negocio -----------------------------------------------------
# Guias de calidad del aire de la OMS (2021), promedios de 24 horas.
OMS_PM2_5_24H = 15.0  # µg/m³
OMS_PM10_24H = 45.0  # µg/m³

# Categorias del indice AQI de la EPA (estados Unidos)
CATEGORIAS_AQI = [
    (50, "Buena"),
    (100, "Moderada"),
    (150, "Insalubre para grupos sensibles"),
    (200, "Insalubre"),
    (300, "Muy insalubre"),
    (float("inf"), "Peligrosa"),
]
