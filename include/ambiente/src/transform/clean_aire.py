"""Capa Silver: el JSON de Bronze se convierte en una tabla ordenada y validada.

Formato largo (una fila por hora y variable), que es el formato "tidy":
facilita agregar, graficar y cargar a cualquier base de datos.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ambiente.config import VARIABLES

COLUMNAS_SILVER = ["fecha_hora", "variable", "valor", "unidad", "calidad"]


def validar_payload(payload: dict) -> dict:
    """Comprueba la estructura del JSON antes de gastar tiempo transformando."""
    if not isinstance(payload, dict) or "hourly" not in payload:
        raise ValueError("El JSON no tiene la clave 'hourly'.")

    hourly = payload["hourly"]
    if "time" not in hourly or not hourly["time"]:
        raise ValueError("'hourly.time' esta vacio.")

    tiempos = hourly["time"]
    faltantes = [v for v in VARIABLES if v not in hourly]
    if faltantes:
        raise ValueError(f"Faltan variables en la respuesta: {faltantes}")

    largos = {v: len(hourly[v]) for v in VARIABLES}
    inconsistentes = {v: n for v, n in largos.items() if n != len(tiempos)}
    if inconsistentes:
        raise ValueError(f"Series con largo distinto a 'time' ({len(tiempos)}): {inconsistentes}")

    vacias = [v for v in VARIABLES if all(x is None for x in hourly[v])]
    if vacias:
        raise ValueError(f"Variables sin ningun dato: {vacias}")

    return {
        "horas": len(tiempos),
        "variables": len(VARIABLES),
        "primera_hora": tiempos[0],
        "ultima_hora": tiempos[-1],
    }


def cargar_bruto(ruta: str | Path) -> dict:
    ruta = Path(ruta)
    if not ruta.exists():
        raise FileNotFoundError(f"No existe el archivo Bronze: {ruta}")
    return json.loads(ruta.read_text(encoding="utf-8"))


def tabla_larga(payload: dict) -> pd.DataFrame:
    """JSON anidado -> DataFrame tidy con una fila por hora y variable."""
    marcos = []

    for clave_api, meta in VARIABLES.items():
        marcos.append(
            pd.DataFrame(
                {
                    "fecha_hora": pd.to_datetime(payload["hourly"]["time"], errors="coerce"),
                    "variable": meta["columna"],
                    "valor": pd.to_numeric(pd.Series(payload["hourly"][clave_api]), errors="coerce"),
                    "unidad": meta["unidad"],
                }
            )
        )

    df = pd.concat(marcos, ignore_index=True)
    df = df.dropna(subset=["fecha_hora"])

    # Un valor fuera de rango fisico se marca, no se borra: la fila sigue
    # siendo evidencia de que hubo una hora con dato sospechoso.
    limites = {meta["columna"]: (meta["min"], meta["max"]) for meta in VARIABLES.values()}
    df["calidad"] = "OK"
    for columna, (minimo, maximo) in limites.items():
        mascara = df["variable"] == columna
        fuera = mascara & (df["valor"].notna()) & ((df["valor"] < minimo) | (df["valor"] > maximo))
        df.loc[fuera, "calidad"] = "FUERA_DE_RANGO"
        df.loc[fuera, "valor"] = pd.NA
    df.loc[df["valor"].isna(), "calidad"] = df["calidad"].where(
        df["calidad"] == "FUERA_DE_RANGO", "SIN_DATO"
    )

    df = df.drop_duplicates(subset=["fecha_hora", "variable"], keep="last")
    df["calidad"] = df["calidad"].astype("string")
    df["variable"] = df["variable"].astype("string")
    df["unidad"] = df["unidad"].astype("string")

    return df.sort_values(["fecha_hora", "variable"]).reset_index(drop=True)[COLUMNAS_SILVER]
