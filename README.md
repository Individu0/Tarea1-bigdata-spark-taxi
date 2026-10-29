# Tarea 3 – Procesamiento de datos con Apache Spark (Big Data, UNAD)

**Curso:** Big Data (código 202016911) – Universidad Nacional Abierta y a Distancia
**Autor:** [Daniel David Gomez Sandoval]

## Problema

¿Cómo varían la **demanda** y el **valor de los viajes** de taxi amarillo en Nueva York según la **hora del día** y el **día de la semana**?

## Dataset

- **Fuente:** NYC Taxi & Limousine Commission (TLC) – *Yellow Taxi Trip Records*.
- **Periodo:** enero de 2024.
- **Formato:** Parquet, con alrededor de 3 millones de registros.
- **Descarga (sin registro):**

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

> El archivo del dataset no se incluye en este repositorio por su tamaño.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `batch_taxi_nyc.py` | Procesamiento batch con PySpark: carga, limpieza, EDA, ejemplo con RDD y gráficas |
| `viajes_por_hora.png` | Viajes por hora del día |
| `tarifa_por_hora.png` | Tarifa promedio por hora del día |
| `viajes_por_dia.png` | Viajes por día de la semana |
| `kafka_producer.py` | Productor de Kafka que simula datos de sensores (Anexo 3) |
| `spark_streaming_consumer.py` | Consumidor con Spark Streaming (Anexo 3) |

## Qué hace el procesamiento batch

1. **Carga** del archivo Parquet con Spark y revisión del esquema.
2. **Limpieza y transformación:** elimina nulos y valores inconsistentes (distancias, tarifas y duraciones imposibles, fechas fuera de enero) y crea las columnas hora, día de la semana, duración y velocidad.
3. **Análisis exploratorio (EDA):** estadísticas descriptivas, viajes y tarifa por hora, viajes por día, tipo de pago y propina promedio.
4. **Ejemplo con RDDs:** top 10 de zonas de recogida con `map`, `reduceByKey` y `takeOrdered`.
5. **Visualización:** gráficas en PNG y resúmenes en CSV dentro de `resultados_batch/`.

## Requisitos

- Python 3
- Apache Spark 3.5.x (PySpark)
- Java 11
- Librerías de Python: `matplotlib`, `pandas`, `pyarrow`

```bash
pip install matplotlib pandas pyarrow
```

## Cómo ejecutarlo

```bash
# 1. Descargar el dataset
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

# 2. Ejecutar el análisis batch
spark-submit batch_taxi_nyc.py
```

Los resultados se guardan en la carpeta `resultados_batch/`.

## Resultados principales

- La demanda es mínima de madrugada y alcanza su máximo entre las 17:00 y las 18:00.
- La tarifa promedio más alta se da hacia las 5:00 de la mañana, justo cuando hay menos viajes.
- Los totales por día de la semana no son comparables de forma directa, porque enero de 2024 tiene cinco lunes, martes y miércoles, y solo cuatro de los demás días.
- La propina promedio en pagos con tarjeta fue de 25,64 % sobre la tarifa.
- Las zonas de recogida 132, 237 y 161 concentran más de 130.000 viajes cada una.

## Procesamiento en tiempo real (Anexo 3)

Con Apache Kafka 3.9.2 y Spark Streaming 3.5.3:

```bash
# Terminal 1: productor (con Kafka y el topic sensor_data ya creados)
python3 kafka_producer.py

# Terminal 2: consumidor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

El productor envía un dato por segundo al topic `sensor_data`, y el consumidor calcula la temperatura y la humedad promedio por sensor en ventanas de 1 minuto.
