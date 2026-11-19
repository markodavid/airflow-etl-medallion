"""Carga de Gold al Data Warehouse (SQLite).

La carga es incremental por fecha: si un dia ya existia se reemplaza.
Asi el DAG puede correr todos los dias sin duplicar historia.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pandas as pd

from ambiente.config import TABLA_WAREHOUSE, WAREHOUSE_DB


def _tabla_existe(conn: sqlite3.Connection, tabla: str) -> bool:
    fila = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tabla,)
    ).fetchone()
    return fila is not None


def cargar_gold(
    df: pd.DataFrame,
    ruta_db: str | Path = WAREHOUSE_DB,
    tabla: str = TABLA_WAREHOUSE,
) -> dict:
    """Inserta/actualiza las fechas presentes en df. Devuelve un resumen."""
    if df.empty:
        raise ValueError("No hay datos para cargar: Gold esta vacio.")

    ruta_db = Path(ruta_db)
    ruta_db.parent.mkdir(parents=True, exist_ok=True)

    with closing(sqlite3.connect(ruta_db)) as conn:
        if _tabla_existe(conn, tabla):
            existente = pd.read_sql_query(f"SELECT * FROM {tabla}", conn)
        else:
            existente = pd.DataFrame()

        fechas_nuevas = set(df["fecha"])
        if not existente.empty and "fecha" in existente.columns:
            conservar = existente[~existente["fecha"].isin(fechas_nuevas)]
        else:
            conservar = existente

        combinado = pd.concat([conservar, df], ignore_index=True)
        combinado = combinado.sort_values("fecha").reset_index(drop=True)
        combinado.to_sql(tabla, conn, if_exists="replace", index=False)

        total = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
        conn.commit()

    esperado = len(df) + len(conservar)
    if total != esperado:
        raise RuntimeError(f"Carga inconsistente: {total} filas, se esperaban {esperado}.")

    print(f"[load] {len(df)} dia(s) escritos, {total} dia(s) en {ruta_db.name} (tabla {tabla})")
    return {"dias_escritos": len(df), "dias_totales": total, "ruta": str(ruta_db)}


def leer_tabla(
    ruta_db: str | Path = WAREHOUSE_DB,
    tabla: str = TABLA_WAREHOUSE,
) -> pd.DataFrame:
    ruta_db = Path(ruta_db)
    if not ruta_db.exists():
        raise FileNotFoundError(f"El warehouse no existe todavia: {ruta_db}")

    with closing(sqlite3.connect(ruta_db)) as conn:
        return pd.read_sql_query(f"SELECT * FROM {tabla}", conn)
