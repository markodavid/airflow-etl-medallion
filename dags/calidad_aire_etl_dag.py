"""Calidad del aire en Cali (Open-Meteo) — ETL Medallion.

Bronze  : JSON crudo de la API, tal como llega.
Silver  : tabla tidy (una fila por hora y variable) con control de calidad.
Gold    : agregado diario + reglas de negocio (OMS 2021 y AQI de la EPA).
Warehouse: SQLite con carga incremental por fecha.

Fuente: https://open-meteo.com/en/docs/air-quality-api (sin API key, CC BY 4.0)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.sdk import DAG, task

INCLUDE_DIR = Path(os.getenv("MEDALLON_INCLUDE_DIR", "/opt/airflow/include"))
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

from ambiente.config import (  # noqa: E402
    BRONZE_DIR,
    CIUDAD,
    DIAS_MAXIMOS,
    GOLD_DIR,
    SILVER_DIR,
    VARIABLES_AGREGADAS,
)
from ambiente.src.extract.open_meteo import extraer_calidad_aire  # noqa: E402
from ambiente.src.load.load_warehouse import cargar_gold  # noqa: E402
from ambiente.src.transform.clean_aire import (  # noqa: E402
    cargar_bruto,
    tabla_larga,
    validar_payload,
)
from ambiente.src.transform.gold_aire import construir_gold  # noqa: E402

DIAS_POR_DEFECTO = 7

default_args = {
    "owner": "gerencia-de-proyectos",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="calidad_aire_cali_etl",
    description="ETL Medallion de calidad del aire (Open-Meteo) para Cali, Colombia",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",  # todos los dias a las 06:00 (America/Bogota)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "medallion", "ambiente", "calidad-aire"],
    doc_md=__doc__,
) as dag:

    @task
    def extraer_bronze(**context) -> dict:
        """Descarga los ultimos N dias y guarda el JSON crudo en Bronze."""
        conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
        dias = int(conf.get("dias", DIAS_POR_DEFECTO))
        print(f"[bronze] Ventana solicitada: {dias} dia(s)")

        resumen = extraer_calidad_aire(dias=dias)
        print(f"[bronze] Archivo: {resumen['ruta']}")
        print(f"[bronze] Rango: {resumen['primera_hora']} -> {resumen['ultima_hora']}")
        return resumen

    @task
    def validar_bronze(resumen_bronze: dict) -> dict:
        """Verifica estructura y contenido del JSON antes de transformarlo."""
        print(f"[bronze] Validando {resumen_bronze['ruta']}")
        payload = cargar_bruto(resumen_bronze["ruta"])
        metricas = validar_payload(payload)

        print(f"[bronze] {metricas['horas']} horas x {metricas['variables']} variables")
        print(f"[bronze] Rango: {metricas['primera_hora']} -> {metricas['ultima_hora']}")

        if metricas["horas"] < 24:
            print("[bronze] Aviso: menos de 24 horas de datos; Gold tendra dias incompletos.")

        return {**resumen_bronze, **metricas}

    @task
    def transformar_a_silver(resumen_bronze: dict) -> dict:
        """JSON -> tabla tidy con control de calidad. Escribe Silver en CSV."""
        payload = cargar_bruto(resumen_bronze["ruta"])
        silver = tabla_larga(payload)

        if silver.empty:
            raise ValueError("Silver quedo vacio despues de la transformacion.")

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        destino = SILVER_DIR / f"calidad_aire_{resumen_bronze['marca']}.csv"
        silver.to_csv(destino, index=False, encoding="utf-8-sig")

        conteo = silver["calidad"].value_counts().to_dict()
        print(f"[silver] {destino.name}: {len(silver)} filas")
        print(f"[silver] Calidad de dato: {conteo}")

        return {
            "ruta": str(destino),
            "marca": resumen_bronze["marca"],
            "filas": len(silver),
            "sin_dato": int(conteo.get("SIN_DATO", 0)),
            "fuera_de_rango": int(conteo.get("FUERA_DE_RANGO", 0)),
        }

    @task
    def integrar_a_gold(resumen_silver: dict) -> dict:
        """Agrega por dia y aplica las reglas de negocio ambientales."""
        silver = pd.read_csv(resumen_silver["ruta"], parse_dates=["fecha_hora"])
        gold = construir_gold(silver)

        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        destino = GOLD_DIR / f"calidad_aire_{resumen_silver['marca']}.csv"
        gold.to_csv(destino, index=False, encoding="utf-8-sig")

        columnas = ["fecha", "pm2_5_promedio", "pm10_promedio", "aqi_us_maximo", "categoria_aqi", "nivel_alerta"]
        presentes = [c for c in columnas if c in gold.columns]
        print(f"[gold] {destino.name}: {len(gold)} dia(s)")
        print(gold[presentes].to_string(index=False))

        alertas = gold["nivel_alerta"].value_counts().to_dict()
        print(f"[gold] Niveles de alerta: {alertas}")

        return {
            "ruta": str(destino),
            "marca": resumen_silver["marca"],
            "dias": len(gold),
            "fechas": gold["fecha"].tolist(),
            "con_alerta": int((gold["nivel_alerta"] != "SIN ALERTA").sum()),
        }

    @task
    def cargar_datawarehouse(resumen_gold: dict) -> dict:
        """Carga incremental (por fecha) al warehouse SQLite."""
        gold = pd.read_csv(resumen_gold["ruta"], dtype={"fecha": str})
        resumen = cargar_gold(gold)

        print(f"[load] Dias escritos: {resumen['dias_escritos']}")
        print(f"[load] Dias en la tabla: {resumen['dias_totales']}")
        print(f"[load] Fuente: {CIUDAD} | variables: {', '.join(VARIABLES_AGREGADAS)}")
        return resumen

    bronze = extraer_bronze()
    validado = validar_bronze(bronze)
    silver = transformar_a_silver(validado)
    gold = integrar_a_gold(silver)
    load = cargar_datawarehouse(gold)

    bronze >> validado >> silver >> gold >> load
