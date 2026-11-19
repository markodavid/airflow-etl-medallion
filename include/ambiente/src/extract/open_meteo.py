"""Capa Bronze: descarga cruda desde la API de Open-Meteo.

Se guarda el JSON TAL CUAL llega, sin transformar. Eso permite reprocesar
Silver y Gold sin volver a llamar la API (y sin depender de su disponibilidad).
"""

from __future__ import annotations

from datetime import datetime, timezone

import requests

from ambiente.config import (
    API_URL,
    BRONZE_DIR,
    CIUDAD,
    DIAS_MAXIMOS,
    LATITUD,
    LONGITUD,
    VARIABLES,
    ZONA_HORARIA,
)


def extraer_calidad_aire(
    dias: int = 7,
    latitud: float = LATITUD,
    longitud: float = LONGITUD,
    timeout: int = 30,
) -> dict:
    """Consulta la API y guarda el JSON crudo en Bronze.

    Devuelve metadatos pequenos (nunca el JSON completo): los XCom de Airflow
    no estan pensados para transportar payloads grandes.
    """
    if not 1 <= dias <= DIAS_MAXIMOS:
        raise ValueError(f"'dias' debe estar entre 1 y {DIAS_MAXIMOS} (recibido: {dias}).")

    parametros = {
        "latitude": latitud,
        "longitude": longitud,
        "hourly": ",".join(VARIABLES),
        "timezone": ZONA_HORARIA,
        "past_days": dias,
        "forecast_days": 0,  # solo observaciones pasadas, sin pronostico
    }

    print(f"[extract] GET {API_URL}")
    print(f"[extract] {CIUDAD} ({latitud}, {longitud}) - ultimos {dias} dias")

    respuesta = requests.get(API_URL, params=parametros, timeout=timeout)
    respuesta.raise_for_status()
    payload = respuesta.json()

    if "hourly" not in payload:
        raise ValueError(f"Respuesta inesperada de la API: {list(payload)}")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    destino = BRONZE_DIR / f"calidad_aire_{marca}.json"

    # El texto crudo (no json.dump) conserva exactamente lo que respondio la API.
    destino.write_text(respuesta.text, encoding="utf-8")

    horas = len(payload["hourly"].get("time", []))
    print(f"[extract] {horas} horas x {len(VARIABLES)} variables -> {destino.name}")

    if horas == 0:
        raise ValueError("La API respondio sin puntos horarios.")

    return {
        "ruta": str(destino),
        "marca": marca,
        "horas": horas,
        "dias_solicitados": dias,
        "ciudad": CIUDAD,
        "latitud": payload.get("latitude", latitud),
        "longitud": payload.get("longitude", longitud),
        "zona_horaria": payload.get("timezone", ZONA_HORARIA),
        "primera_hora": payload["hourly"]["time"][0],
        "ultima_hora": payload["hourly"]["time"][-1],
    }
