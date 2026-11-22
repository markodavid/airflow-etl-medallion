# Calidad del aire en Cali — ETL Medallion con Airflow 3 y Docker

Pipeline de datos ambientales con **arquitectura Medallion** sobre **Apache
Airflow 3.4** en Docker (Windows + WSL2). Extrae calidad del aire desde la API
pública de **Open-Meteo**, la limpia, la agrega por día y la carga en un
data warehouse SQLite.

No requiere instalar Ubuntu a mano ni `pip install apache-airflow`.

```
Windows
  └── Docker Desktop (backend WSL2)
        ├── postgres              ← metadatos de Airflow
        ├── airflow-apiserver     ← UI + API (localhost:8081)
        ├── airflow-scheduler     ← decide qué tarea corre y cuándo
        ├── airflow-dag-processor ← lee y valida los DAGs
        └── airflow-triggerer     ← tareas diferidas

DAG calidad_aire_cali_etl
  extraer_bronze → validar_bronze → transformar_a_silver → integrar_a_gold → cargar_datawarehouse
      JSON crudo  ──▶  validación  ──▶  tabla tidy      ──▶  diario + reglas ──▶  SQLite
```

---

## 1. Requisitos

- Windows 10 2004+ / Windows 11
- **Docker Desktop** con *Use WSL 2 based engine* activado (el instalador ya trae WSL2)
- 8 GB de RAM recomendados (este compose está limitado a ~4 GB entre todos los servicios)
- **10 GB libres en disco** (la imagen ocupa ~2,4 GB)
- Puerto **8081** libre (`AIRFLOW_UI_PORT` en `.env`); el 8080 lo usa otro contenedor de este equipo
- **Salida a internet**: el contenedor consulta `air-quality-api.open-meteo.com`

Comprobación rápida:

```powershell
docker version
docker compose version
wsl -l -v      # docker-desktop debe estar en VERSION 2
```

---

## 2. Estructura

```
airflow/
├── docker-compose.yaml              # servicios, volúmenes, LocalExecutor
├── Dockerfile                       # imagen propia (Airflow + dependencias)
├── requirements-airflow.txt         # librerías extra de la imagen
├── .env                             # AIRFLOW_UID, secretos, puerto, login
├── dags/
│   └── calidad_aire_etl_dag.py      # el DAG (5 tareas encadenadas)
├── include/
│   └── ambiente/                    # la lógica, independiente de Airflow
│       ├── config.py                # API, coordenadas, variables, umbrales OMS
│       └── src/
│           ├── extract/open_meteo.py        # Bronze: descarga
│           ├── transform/clean_aire.py      # Silver: validación + tabla tidy
│           ├── transform/gold_aire.py       # Gold: agregación + reglas
│           └── load/load_warehouse.py       # DW: carga incremental
├── data/
│   └── aire/
│       ├── bronze/                  # JSON crudo, inmutable, con marca de tiempo
│       ├── silver/                  # tabla tidy (hora × variable)
│       ├── gold/                    # una fila por día
│       └── warehouse/calidad_aire.db
├── logs/                            # logs de Airflow
├── plugins/
└── config/                          # airflow.cfg generado por airflow-init
```

**Regla de oro:** el contenedor nunca ve `C:\...`. Ve `/opt/airflow/...`, porque
`docker-compose.yaml` monta `./data` en `/opt/airflow/data`.

**Separación de responsabilidades:** el DAG decide *cuándo y en qué orden*;
`include/ambiente` decide *cómo*. Las transformaciones son funciones puras,
probables sin Airflow.

---

## 3. Puesta en marcha

```powershell
# 1) construir la imagen (instala requirements-airflow.txt)
docker compose build

# 2) crear la base de Airflow, el usuario del panel y los permisos
docker compose up airflow-init

# 3) levantar todo en segundo plano
docker compose up -d

# 4) comprobar
docker compose ps
```

`airflow-init` debe terminar con `exited with code 0`. Si falla:
`docker compose logs airflow-init`.

---

## 4. Panel web

<http://localhost:8081>

| usuario | contraseña |
|---|---|
| `airflow` | `airflow` |

Se cambian en `.env` (`_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`)
**antes** de correr `airflow-init`; si el usuario ya existe hay que borrarlo:

```powershell
docker compose exec airflow-scheduler airflow users delete --username airflow
docker compose up airflow-init
```

El DAG aparece activo y corre solo todos los días a las **06:00 de Bogotá**
(`AIRFLOW__CORE__DEFAULT_TIMEZONE`).

---

## 5. Qué hace cada tarea

| Tarea | Capa | Qué hace | Salida |
|---|---|---|---|
| `extraer_bronze` | Bronze | GET a Open-Meteo (`past_days=N`, sin pronóstico) y guarda el JSON **tal como llega**, con marca de tiempo | `bronze/calidad_aire_<marca>.json` |
| `validar_bronze` | Bronze | Verifica `hourly.time`, que estén las 8 variables, largos consistentes y que ninguna serie sea 100% nula | métricas al XCom |
| `transformar_a_silver` | Silver | JSON anidado → tabla tidy (una fila por hora y variable), rangos físicos, flags de calidad, dedupe | `silver/*.csv` (≈17.280 filas por 90 días) |
| `integrar_a_gold` | Gold | Agrupa por día: promedio, máximo, p95 y horas válidas; aplica OMS 2021 y categorías AQI | `gold/*.csv` (1 fila por día) |
| `cargar_datawarehouse` | DW | Upsert por `fecha`: reemplaza los días ya existentes, inserta los nuevos, verifica el conteo | tabla `gold_calidad_aire` |

Si una tarea falla, las siguientes **no** se ejecutan y Airflow reintenta 2 veces
con 1 minuto de espera. Los XCom transportan solo metadatos (rutas, conteos); el
dato viaja por el volumen compartido.

---

## 6. Contratos de datos

**Silver** — una fila por hora y variable:

| columna | ejemplo |
|---|---|
| `fecha_hora` | `2026-11-17 14:00:00` |
| `variable` | `pm2_5`, `pm10`, `co`, `no2`, `so2`, `o3`, `aqi_us`, `aqi_europa` |
| `valor` | `4.6` |
| `unidad` | `µg/m³` |
| `calidad` | `OK` / `SIN_DATO` / `FUERA_DE_RANGO` |

Un valor fuera de rango físico no se borra: se marca y se anula. Queda evidencia
del problema.

**Gold** — una fila por día (29 columnas):

```
fecha | pm2_5_promedio | pm2_5_maximo | pm2_5_p95 | pm2_5_horas | … | aqi_us_maximo |
excede_oms_pm2_5 | excede_oms_pm10 | categoria_aqi | nivel_alerta
```

## 7. Reglas de negocio

| Regla | Definición |
|---|---|
| `excede_oms_pm2_5` | promedio diario > **15 µg/m³** (OMS 2021, 24 h) |
| `excede_oms_pm10` | promedio diario > **45 µg/m³** (OMS 2021, 24 h) |
| `categoria_aqi` | Buena / Moderada / Insalubre para grupos sensibles / Insalubre / Muy insalubre / Peligrosa (EPA) |
| `nivel_alerta` | `SIN ALERTA` → `BAJA` → `MEDIA` → `ALTA` |

Todo se ajusta en `include/ambiente/config.py` (líneas 41–66): variables,
rangos, `OMS_PM2_5_24H`, `OMS_PM10_24H` y `CATEGORIAS_AQI`.

---

## 8. Ejecutar, programar y reprocesar

```powershell
# ventana por defecto (90 días ≈ 3 meses)
docker compose exec airflow-scheduler airflow dags trigger calidad_aire_cali_etl

# ventana explícita (1 a 92 días)
docker compose exec airflow-scheduler python -c "import json,subprocess;open('/tmp/c.json','w').write(json.dumps({'dias':14}));subprocess.run(['airflow','dags','trigger','calidad_aire_cali_etl','-c',open('/tmp/c.json').read()],check=True)"

# estado de las corridas
docker compose exec airflow-scheduler airflow dags list-runs calidad_aire_cali_etl
```

> En PowerShell 5.1 pasar JSON con `--conf` en línea rompe las comillas; por eso
> el ejemplo lo arma con Python dentro del contenedor.

Cambiar la programación en `dags/calidad_aire_etl_dag.py`:

```python
schedule="0 6 * * *"    # diario 06:00
schedule="@daily"       # equivalente
schedule=None           # solo manual
```

`catchup=False` evita que Airflow rellene días pasados al activar la programación.
La ventana por defecto es de **90 días (~3 meses)**; el máximo que permite la API
es 92 (`DIAS_MAXIMOS` en `config.py`). Cada corrida diaria vuelve a pedir los
últimos 90 días y el upsert reemplaza solo las fechas nuevas, sin duplicar.

---

## 9. Ver resultados

```powershell
Get-Content .\data\aire\gold\*.csv | Select-Object -First 10
```

```powershell
docker compose exec airflow-scheduler python -c "import sqlite3;c=sqlite3.connect('/opt/airflow/data/aire/warehouse/calidad_aire.db');print(c.execute('select fecha,pm2_5_promedio,aqi_us_maximo,categoria_aqi,nivel_alerta from gold_calidad_aire order by fecha desc limit 7').fetchall())"
```

Salida esperada (ejemplo real):

```
2026-11-12 | PM2.5 8.81 | AQI 51 | Moderada | BAJA
2026-11-17 | PM2.5 10.33 | AQI 52 | Moderada | BAJA
resto      | Buena     | SIN ALERTA
```

---

## 10. Bucle de trabajo

| Cambiaste... | Qué hacer |
|---|---|
| el DAG (`dags/`) | nada: el `dag-processor` lo detecta en ~30 s |
| `include/ambiente/` | nada: está montado como volumen |
| `requirements-airflow.txt` o `Dockerfile` | `docker compose build` y luego `docker compose up -d` |
| `docker-compose.yaml` o `.env` | `docker compose up -d` (recrea lo necesario) |

---

## 11. Comandos del día a día

```powershell
docker compose ps                        # estado de los servicios
docker compose logs -f airflow-scheduler # logs en vivo
docker compose restart airflow-scheduler
docker compose exec airflow-scheduler bash      # shell en el contenedor
docker compose down                      # detener (conserva datos)
docker compose up -d                     # levantar
docker compose down -v                   # detener y BORRAR la BD de Airflow
```

---

## 12. Problemas frecuentes

| Síntoma | Causa / solución |
|---|---|
| El DAG no aparece | `airflow dags list-import-errors` en el scheduler |
| `ModuleNotFoundError: No module named 'ambiente'` | Falta `PYTHONPATH=/opt/airflow/include` o el volumen `include/` |
| `ModuleNotFoundError: No module named 'requests'` | Agregalo a `requirements-airflow.txt` y reconstruí |
| `requests.exceptions.ConnectionError` | El contenedor no tiene salida a internet, o Open-Meteo está caído |
| `FileNotFoundError: /opt/airflow/data/...` | Estás usando rutas de Windows dentro del contenedor |
| `docker compose up` falla por memoria | Subí la RAM de Docker Desktop (Settings → Resources) |
| Docker Desktop se cuelga y `docker ps` no responde | Casi siempre **disco lleno**: `Get-PSDrive C`, `docker system df`, `docker builder prune` |
| La primera tarea falla con `Could not reach the Execution API server` tras reiniciar | Arranque en frío: el api-server todavía está importando el DAG. `retries` la recupera en el segundo intento |
| Pylance subraya `from airflow.sdk import DAG` | Normal: tu Python de Windows no tiene Airflow |
| Quedó un DAG eliminado en la UI | `docker compose exec airflow-scheduler airflow dags delete --yes <dag_id>` |

---

## 13. Advertencias del equipo

**OneDrive.** El proyecto vive en `OneDrive\Documentos`. Los bind mounts de
`dags/`, `logs/` y `data/` sobre una carpeta sincronizada pueden dar lentitud,
bloqueos y permisos inconsistentes. Si aparecen errores raros:

```powershell
New-Item -ItemType Directory -Force C:\dev | Out-Null
robocopy "$PWD" C:\dev\airflow /E
```

**RAM.** Con ~8 GB totales el compose está recortado: `LocalExecutor` (sin
Celery/Redis), sin `airflow-worker` y con `mem_limit` por servicio. Si necesitás
más aire, creá `C:\Users\<usuario>\.wslconfig`:

```ini
[wsl2]
memory=5GB
processors=2
```

**Disco.** Si `C:` se llena, el motor de Docker se cae y `docker` se queda
colgado sin mensaje (el síntoma está en
`%LOCALAPPDATA%\Docker\log\host\monitor.log`: *"There is not enough space on the
disk"*).

---

## 14. Fuente de datos

**Open-Meteo Air Quality API** — <https://open-meteo.com/en/docs/air-quality-api>

- Sin API key ni registro; licencia **CC BY 4.0** (citar la fuente).
- Datos horarios, cobertura global, hasta 92 días hacia atrás.
- Variables: `pm2_5`, `pm10`, `carbon_monoxide`, `nitrogen_dioxide`,
  `sulphur_dioxide`, `ozone`, `us_aqi`, `european_aqi`.
- Consulta por defecto: **Cali, Colombia** (3.4516, -76.5320), `America/Bogota`.

Atribución sugerida para el informe: *Open-Meteo.com (CC BY 4.0)*.

**Cambiar de ciudad** en `include/ambiente/config.py`, o por variable de entorno:

```powershell
# Bogotá
AIRE_CIUDAD=Bogotá ; AIRE_LATITUD=4.711 ; AIRE_LONGITUD=-74.0721
```

---

## 15. Siguientes pasos

- Añadir un `@task.branch` para enrutar según `nivel_alerta`.
- Notificar cuando `nivel_alerta = ALTA` (webhook a n8n, correo o WhatsApp).
- Leer el warehouse con Power BI / Metabase.
- Añadir `pytest` sobre `include/ambiente` (las funciones son puras).
