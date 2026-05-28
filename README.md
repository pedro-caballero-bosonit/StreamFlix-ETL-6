# StreamFlix ETL — Módulo 6

Pipeline de procesamiento de datos para StreamFlix Analytics.
Lee logs de reproducción, los enriquece con metadata de películas,
calcula KPIs y guarda los resultados en formato Parquet.

## Requisitos

- Java 11
- Scala 2.12.15
- Apache Spark 3.4.1
- SBT 1.8.0

## Estructura del proyecto

```
src/main/scala/com/streamflix/
├── Main.scala                 ← punto de entrada
└── processor/
    ├── DataLoader.scala       ← lee los archivos de entrada
    ├── Cleaner.scala          ← limpia y tipifica los datos
    ├── Enricher.scala         ← une logs con metadata de películas
    ├── KPICalculator.scala    ← calcula top géneros y binge watchers
    └── Writer.scala           ← guarda resultados en Parquet
```

## Datos de entrada

| Archivo | Descripción |
|---------|-------------|
| `server_logs.txt` | Logs de reproducción de usuarios |
| `movies_metadata.csv` | Catálogo de películas con géneros y metadata |

## Ejecución en desarrollo con SBT

```bash
sbt "run <logsPath> <moviesPath> <outputPath>"
```

Ejemplo:
```bash
sbt "run src/main/resources/data/server_logs.txt \
         src/main/resources/data/movies_metadata.csv \
         src/main/resources/output/analytics_warehouse"
```

## Generar el JAR para producción

El JAR empaqueta todo el código compilado en un solo archivo
ejecutable — no necesitas SBT ni IntelliJ para ejecutarlo.

```bash
sbt package
```

El JAR se genera en:
```
target/scala-2.12/streamflix-etl-6_2.12-0.1.0-SNAPSHOT.jar
```

## Ejecución en producción con spark-submit

`spark-submit` es el comando oficial de Spark para ejecutar jobs
en producción. Solo necesita Java y Spark instalados.

```bash
spark-submit \
  --class com.streamflix.Main \
  --master local[*] \
  target/scala-2.12/streamflix-etl-6_2.12-0.1.0-SNAPSHOT.jar \
  /ruta/server_logs.txt \
  /ruta/movies_metadata.csv \
  /ruta/output/analytics_warehouse
```

### Parámetros

| Parámetro | Descripción |
|-----------|-------------|
| `--class com.streamflix.Main` | Clase principal del job |
| `--master local[*]` | Ejecuta en local usando todos los cores |
| Argumento 1 | Ruta del archivo server_logs.txt |
| Argumento 2 | Ruta del archivo movies_metadata.csv |
| Argumento 3 | Ruta de salida para los resultados Parquet |

## Output

Los resultados se guardan en formato Parquet particionado por año y país:

```
output/analytics_warehouse/
├── year=2025/
│   ├── country=ES/part-0000.parquet
│   └── country=US/part-0000.parquet
└── year=2024/
    ├── country=ES/part-0000.parquet
    └── country=US/part-0000.parquet
```

El particionamiento permite filtrar eficientemente
por año y país sin leer todos los datos.