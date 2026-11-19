"""Capa Gold: agregacion diaria y reglas de negocio ambientales.

Silver responde "que valor hubo cada hora". Gold responde "que paso ese dia
y si deberia encender una alerta".
"""

from __future__ import annotations

import pandas as pd

from ambiente.config import (
    CATEGORIAS_AQI,
    OMS_PM10_24H,
    OMS_PM2_5_24H,
    VARIABLES_AGREGADAS,
)


def _categoria_aqi(valor: float | None) -> str:
    if pd.isna(valor):
        return "SIN DATO"
    for limite, etiqueta in CATEGORIAS_AQI:
        if valor <= limite:
            return etiqueta
    return "Peligrosa"


def _nivel_alerta(categoria: str, pm2_5_maximo: float | None, excede_oms: bool) -> str:
    if categoria in {"Muy insalubre", "Peligrosa"}:
        return "ALTA"
    if categoria in {"Insalubre", "Insalubre para grupos sensibles"} or excede_oms:
        return "MEDIA"
    if not pd.isna(pm2_5_maximo) and pm2_5_maximo >= 2 * OMS_PM2_5_24H:
        return "MEDIA"
    if categoria == "Moderada":
        return "BAJA"
    return "SIN ALERTA"


def construir_gold(silver: pd.DataFrame) -> pd.DataFrame:
    """Agrega Silver por dia y aplica las reglas de negocio."""
    if silver.empty:
        raise ValueError("Silver esta vacio: no hay nada que agregar.")

    df = silver[silver["calidad"] == "OK"].copy()
    if df.empty:
        raise ValueError("Ningun dato paso el control de calidad: revisa Bronze.")

    df["fecha"] = pd.to_datetime(df["fecha_hora"]).dt.strftime("%Y-%m-%d")

    partes = []
    for variable in VARIABLES_AGREGADAS:
        sub = df[df["variable"] == variable]
        if sub.empty:
            print(f"[gold] Aviso: no hay datos de {variable}, se dejan columnas vacias.")
            continue
        agregado = sub.groupby("fecha")["valor"].agg(
            promedio="mean",
            maximo="max",
            p95=lambda serie: serie.quantile(0.95),
            horas="count",
        )
        agregado.columns = [f"{variable}_{columna}" for columna in agregado.columns]
        partes.append(agregado.round(2))

    if not partes:
        raise ValueError("Ninguna variable agregada tiene datos.")

    gold = pd.concat(partes, axis=1).reset_index()

    # Columnas derivadas (reglas de negocio)
    gold["excede_oms_pm2_5"] = gold["pm2_5_promedio"] > OMS_PM2_5_24H
    gold["excede_oms_pm10"] = gold["pm10_promedio"] > OMS_PM10_24H
    gold["categoria_aqi"] = gold["aqi_us_maximo"].map(_categoria_aqi)
    gold["nivel_alerta"] = [
        _nivel_alerta(categoria, maximo, excede)
        for categoria, maximo, excede in zip(
            gold["categoria_aqi"], gold["pm2_5_maximo"], gold["excede_oms_pm2_5"]
        )
    ]

    columnas = ["fecha"]
    for variable in VARIABLES_AGREGADAS:
        for sufijo in ("promedio", "maximo", "p95", "horas"):
            columna = f"{variable}_{sufijo}"
            if columna in gold.columns:
                columnas.append(columna)
    columnas += ["excede_oms_pm2_5", "excede_oms_pm10", "categoria_aqi", "nivel_alerta"]

    return gold[columnas].sort_values("fecha").reset_index(drop=True)
