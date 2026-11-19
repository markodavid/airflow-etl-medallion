# Airflow 3 + Docker en Windows — Pipeline ETL Medallion

Entorno local de Apache Airflow listo para correr en Windows con Docker Desktop,
sin instalar Ubuntu a mano y sin instalar Airflow con pip.

```
Windows
  └── Docker Desktop (backend WSL2)
        ├── postgres              ← metadatos de Airflow
        ├── airflow-apiserver     ← UI + API (localhost:8080)
        ├── airflow-scheduler     ← decide qué tarea corre y cuándo
        ├── airflow-dag-processor ← lee y valida los DAGs
        └── airflow-triggerer     ← tareas diferidas

DAG medallon_etl_students          (demo de CSV → SQLite)
  validar_bronze → transformar_a_silver → integrar_a_gold → cargar_datawarehouse
        Bronze (CSV) ──▶ Silver (CSV) ──▶ Gold (CSV) ──▶ SQLite warehouse.db

DAG calidad_aire_cali_etl          (datos reales de Open-Meteo)
  extraer_bronze → validar_bronze → transformar_a_silver → integrar_a_gold → cargar_datawarehouse
        Bronze (JSON) ──▶ Silver (tidy CSV) ──▶ Gold (diario) ──▶ SQLite calidad_aire.db
```

---

## 1. Requisitos

- Windows 10 2004+ / Windows 11
- **Docker Desktop** con *Use WSL 2 based engine* activado (el instalador ya trae WSL2 y la distro `docker-desktop`)
- 8 GB de RAM recomendados (este compose está limitado a ~4 GB entre todos los servicios)
- **10 GB libres en disco** (la imagen ocupa ~2,4 GB)
- Puerto **8081** libre en Windows (`AIRFLOW_UI_PORT` en `.env`). El 8080 lo usa
  otro contenedor de este equipo (`pgadmin_container`).

No hace falta instalar Ubuntu, ni WSL a mano, ni `pip install apache-airflow`.
Airflow solo ejecuta en Linux: Docker Desktop le da ese Linux a través de WSL2.

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
├── docker-compose.yaml          # servicios, volúmenes, LocalExecutor
├── Dockerfile                   # imagen propia (Airflow + dependencias)
├── requirements-airflow.txt     # librerías extra que se instalan en la imagen
├── .env                         # AIRFLOW_UID, secretos, usuario del panel
├── dags/
│   ├── medallon_etl_dag.py          # demo: ETL de estudiantes (4 tareas)
│   └── calidad_aire_etl_dag.py      # datos reales: calidad del aire (5 tareas)
├── include/
│   ├── medallon/                # pipeline demo: extract / transform / load
│   │   ├── config.py            # rutas del contenedor
│   │   └── src/
│   │       ├── extract/extract_csv.py
│   │       ├── transform/clean_data.py
│   │       ├── transform/gold_data.py
│   │       └── load/load_warehouse.py
│   └── ambiente/                # pipeline de calidad del aire
│       ├── config.py            # API, coordenadas, variables, umbrales OMS
│       └── src/
│           ├── extract/open_meteo.py
│           ├── transform/clean_aire.py
│           ├── transform/gold_aire.py
│           └── load/load_warehouse.py
├── data/
│   ├── bronze/                  # entradas (datos sucios, a propósito)
│   ├── silver/                  # salida limpia y tipada
│   ├── gold/                    # salida con reglas de negocio
│   ├── warehouse/warehouse.db   # "data warehouse" (SQLite)
│   └── aire/                    # mismo esquema, para calidad del aire
│       ├── bronze/  silver/  gold/
│       └── warehouse/calidad_aire.db
├── logs/                        # logs de Airflow
├── plugins/                     # plugins (vacío)
└── config/                      # airflow.cfg generado por airflow-init
```

La regla de oro: **el contenedor nunca ve `C:\...`**. Ve `/opt/airflow/...`,
porque `docker-compose.yaml` monta `./data` en `/opt/airflow/data`.

---

## 3. Puesta en marcha

Desde la carpeta del proyecto, en PowerShell:

```powershell
# 1) construir la imagen (instala requirements-airflow.txt)
docker compose build

# 2) crear la base de datos de Airflow, el usuario del panel y los permisos
docker compose up airflow-init

# 3) levantar todo en segundo plano
docker compose up -d

# 4) comprobar
docker compose ps
```

Cuando `airflow-init` termina debe mostrar `airflow-init exited with code 0`.
Si termina con error, mira `docker compose logs airflow-init`.

Abre <http://localhost:8081> e inicia sesión con:

| usuario | contraseña |
|---|---|
| `airflow` | `airflow` |

(Se cambian en `.env` con `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`
**antes** de ejecutar `airflow-init`.)

El DAG aparece **pausado** (es lo correcto). Actívalo con el toggle y luego
`Trigger` para ejecutarlo.

---

## 4. Ejecutar el DAG desde la terminal

```powershell
# ¿Airflow ve mis DAGs?
docker compose exec airflow-scheduler airflow dags list

# ¿algún DAG tiene errores de importación? (el comando más útil de todos)
docker compose exec airflow-scheduler airflow dags list-import-errors

# ¿el contenedor ve los archivos?
docker compose exec airflow-scheduler ls /opt/airflow/dags
docker compose exec airflow-scheduler ls /opt/airflow/data/bronze

# disparar y consultar ejecuciones
docker compose exec airflow-scheduler airflow dags trigger medallon_etl_students
docker compose exec airflow-scheduler airflow dags list-runs medallon_etl_students

# logs del scheduler en vivo
docker compose logs -f airflow-scheduler
```

---

## 5. Ver los resultados

```powershell
Get-Content .\data\silver\silver_students.csv
Get-Content .\data\gold\gold_data.csv
```

El warehouse queda en `data\warehouse\warehouse.db` (SQLite). Para consultarlo
sin instalar nada:

```powershell
docker compose exec airflow-scheduler python -c "import sqlite3;c=sqlite3.connect('/opt/airflow/data/warehouse/warehouse.db');print(c.execute('select student_id,nivel_riesgo,creditos_totales from gold_students').fetchall())"
```

---

## 6. Qué hace cada tarea

| Tarea | Capa | Qué hace |
|---|---|---|
| `validar_bronze` | Bronze | Los dos CSV existen, no están vacíos y traen las columnas mínimas |
| `transformar_a_silver` | Silver | Normaliza texto, deduplica, tipa `gpa`/`semestre`/`creditos` y escribe Silver |
| `integrar_a_gold` | Gold | Agrega el histórico por estudiante, calcula variación de GPA y nivel de riesgo |
| `cargar_datawarehouse` | DW | Reemplaza la tabla `gold_students` en SQLite y verifica el conteo |

Dependencias: `bronze >> silver >> gold >> load`. Si una tarea falla, las
siguientes **no** se ejecutan y la ejecución queda marcada como `failed`.

El código de limpieza no vive en el DAG: vive en `include/medallon/`. Airflow
solo orquesta. Eso permite probar el pipeline sin Airflow y reutilizarlo.

---

## 7. Programar la ejecución

En `dags/medallon_etl_dag.py`:

```python
schedule="@daily"          # todos los días a medianoche
schedule="0 6 * * *"       # todos los días a las 06:00
schedule="0 8 * * 1"       # lunes a las 08:00
```

`catchup=False` evita que Airflow rellene todos los días pasados desde
`start_date` cuando activas la programación.

---

## 8. Bucle de trabajo

| Cambiaste... | Qué hacer |
|---|---|
| un DAG (`dags/`) | nada: el `dag-processor` lo detecta en ~30 s |
| el código de `include/` | nada: está montado como volumen |
| `requirements-airflow.txt` o `Dockerfile` | `docker compose build` y luego `docker compose up -d` |
| `docker-compose.yaml` o `.env` | `docker compose up -d` (recrea lo necesario) |

---

## 9. Comandos del día a día

```powershell
docker compose ps                       # estado de los servicios
docker compose restart airflow-scheduler
docker compose logs -f airflow-scheduler
docker compose exec airflow-scheduler bash     # shell dentro del contenedor
docker compose down                     # detener (conserva la base de datos)
docker compose up -d                    # volver a levantar
docker compose down -v                  # detener y BORRAR la base de datos de Airflow
```

---

## 10. Problemas frecuentes

| Síntoma | Causa / solución |
|---|---|
| El DAG no aparece en la UI | `airflow dags list-import-errors` en el scheduler |
| `ModuleNotFoundError: No module named 'medallon'` | Falta `PYTHONPATH=/opt/airflow/include` o el volumen `include/` |
| `ModuleNotFoundError: No module named 'pandas'` | Añade la librería a `requirements-airflow.txt` y `docker compose build` |
| `FileNotFoundError: /opt/airflow/data/...` | Estás usando rutas de Windows dentro del contenedor |
| `docker compose up` falla por memoria | Baja `mem_limit` o sube la RAM de Docker Desktop (Settings → Resources) |
| Docker Desktop se queda colgado y `docker ps` no responde | Casi siempre es **disco lleno**. Mira `Get-PSDrive C` y libera espacio (`docker system df`, `docker builder prune`) |
| El contenedor no arranca tras cambiar el código | `docker compose down` y `docker compose up -d` |
| Acentos raros en los CSV | Los CSV se escriben en `utf-8-sig`; ábrelos con Excel o VS Code, no con `type` |
| Pylance subraya `from airflow.sdk import DAG` | Normal: tu Python de Windows no tiene Airflow. Añade `include` a `python.analysis.extraPaths` si te molesta |

---

## 11. Dos advertencias sobre este equipo

**OneDrive.** Este proyecto está dentro de `OneDrive\Documentos`. Los bind mounts
de `dags/`, `logs/` y `data/` sobre una carpeta sincronizada pueden producir
lentitud, bloqueos de archivo y permisos inconsistentes mientras OneDrive
sincroniza. Si aparecen errores raros, muévelo fuera:

```powershell
New-Item -ItemType Directory -Force C:\dev | Out-Null
robocopy "$PWD" C:\dev\airflow /E
# luego abre C:\dev\airflow y ejecuta docker compose up -d
```

**RAM.** Con ~8 GB totales el compose está recortado a propósito: `LocalExecutor`
(no Celery/Redis), sin `airflow-worker` y con `mem_limit` por servicio. Si necesitas
más aire, crea `C:\Users\<usuario>\.wslconfig`:

```ini
[wsl2]
memory=5GB
processors=2
```

y reinicia Docker Desktop (`wsl --shutdown`).

**Disco.** Docker Desktop necesita margen libre en `C:`. Cuando el disco se llena,
el motor se cae y `docker ps` se queda colgado sin decir por qué (el síntoma
aparece en `%LOCALAPPDATA%\Docker\log\host\monitor.log` como
*"There is not enough space on the disk"*). Antes de trabajar:

```powershell
Get-PSDrive C
docker system df          # cuánto ocupan imágenes, volúmenes y caché
docker builder prune -f   # borra caché de build (seguro)
docker system prune -f    # borra contenedores parados e imágenes huérfanas
```

---

## 12. Pipeline de calidad del aire en Cali (Open-Meteo)

Segundo DAG, con **datos reales**: `calidad_aire_cali_etl`.

### El dataset

**Open-Meteo Air Quality API** — <https://open-meteo.com/en/docs/air-quality-api>

| Criterio | Por qué |
|---|---|
| Sin API key ni registro | solo HTTP GET, se puede correr en clase sin cuentas |
| Licencia CC BY 4.0 | se puede usar y publicar citando la fuente |
| Datos horarios | permite agregar a diario y calcular percentiles |
| Cobertura global | se cambia la ciudad con dos coordenadas |
| Incluye AQI | índice US y europeo ya calculados por la API |

Se consulta para **Cali, Colombia** (3.4516, -76.5320), últimos *N* días
(1 a 92), sin pronóstico (`forecast_days=0`).

Variables: `pm2_5`, `pm10`, `co`, `no2`, `so2`, `o3`, `aqi_us`, `aqi_europa`.

> Alternativas descartadas: OpenAQ (hoy exige API key), SISAIRE/IDEAM
> (descargas manuales en Excel, sin API estable), World Bank / OWID (anuales,
> no permiten mostrar un pipeline incremental).

### Las cinco tareas

| Tarea | Capa | Qué hace |
|---|---|---|
| `extraer_bronze` | Bronze | GET a la API, guarda el JSON crudo con marca de tiempo (inmutable) |
| `validar_bronze` | Bronze | Verifica `hourly.time`, que estén las 8 variables y que los largos coincidan |
| `transformar_a_silver` | Silver | JSON anidado → tabla tidy (hora × variable) con flag `calidad` |
| `integrar_a_gold` | Gold | Agrega por día (promedio, máximo, p95, horas) + reglas OMS/AQI |
| `cargar_datawarehouse` | DW | Upsert por `fecha` en SQLite (correr dos veces no duplica) |

Reglas de negocio en Gold:

- `excede_oms_pm2_5`: promedio diario > 15 µg/m³ (OMS 2021, 24 h).
- `excede_oms_pm10`: promedio diario > 45 µg/m³.
- `categoria_aqi`: Buena / Moderada / Insalubre para grupos sensibles / Insalubre / Muy insalubre / Peligrosa.
- `nivel_alerta`: `SIN ALERTA` → `BAJA` → `MEDIA` → `ALTA`.

### Ejecutarlo

Se ejecuta solo todos los días a las 06:00 (`America/Bogota`) y también a mano:

```powershell
# ventana por defecto: 7 días
docker compose exec airflow-scheduler airflow dags trigger calidad_aire_cali_etl

# ventana explícita (1 a 92 días) — el DAG lee dag_run.conf["dias"]
docker compose exec airflow-scheduler python -c "import json,subprocess;open('/tmp/c.json','w').write(json.dumps({'dias':14}));subprocess.run(['airflow','dags','trigger','calidad_aire_cali_etl','-c',open('/tmp/c.json').read()],check=True)"

docker compose exec airflow-scheduler airflow dags list-runs calidad_aire_cali_etl
```

### Ver los resultados

```powershell
Get-Content .\data\aire\gold\*.csv | Select-Object -First 20
```

```powershell
docker compose exec airflow-scheduler python -c "import sqlite3;c=sqlite3.connect('/opt/airflow/data/aire/warehouse/calidad_aire.db');print(c.execute('select fecha,pm2_5_promedio,aqi_us_maximo,categoria_aqi,nivel_alerta from gold_calidad_aire order by fecha').fetchall())"
```

### Cambiar de ciudad o de fuente

Todo vive en `include/ambiente/config.py`:

```python
CIUDAD = "Cali, Colombia"
LATITUD = 3.4516
LONGITUD = -76.5320
OMS_PM2_5_24H = 15.0
```

También por variable de entorno (`AIRE_CIUDAD`, `AIRE_LATITUD`, `AIRE_LONGITUD`).
Para Bogotá: `AIRE_CIUDAD=Bogotá`, `AIRE_LATITUD=4.711`, `AIRE_LONGITUD=-74.0721`.

---

## 13. Siguientes pasos

- Reemplazar `schedule=None` por una programación real y observar `catchup`.
- Añadir un `@task.branch` para enrutar según `nivel_riesgo` / `nivel_alerta`.
- Cambiar la carga de SQLite a PostgreSQL (otra capa, mismo DAG).
- Añadir `pytest` sobre `include/medallon` e `include/ambiente` para probar la
  limpieza sin Airflow.
- Graficar Gold con Power BI o Metabase usando la tabla `gold_calidad_aire`.
