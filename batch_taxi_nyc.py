"""
Tarea 3 - Big Data (UNAD)
Procesamiento batch con Apache Spark (PySpark)

Problema: ¿Cómo varían la demanda y el valor de los viajes de taxi amarillo
en Nueva York según la hora del día y el día de la semana?

Dataset: NYC TLC Yellow Taxi Trip Records (enero 2024, formato Parquet).
Descarga (sin registro):
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

Ejecución:
  pip install matplotlib pandas pyarrow
  spark-submit batch_taxi_nyc.py
"""
import os
from operator import add

import matplotlib
matplotlib.use("Agg")  # sin interfaz gráfica (la VM se usa por SSH)
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PARQUET = "yellow_tripdata_2024-01.parquet"
OUT_DIR = "resultados_batch"
os.makedirs(OUT_DIR, exist_ok=True)

spark = (SparkSession.builder
         .appName("TaxiNYC_Batch")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------
# 1. CARGA
# ---------------------------------------------------------------------
df = spark.read.parquet(PARQUET)
total_raw = df.count()
print(f"\n=== Registros cargados: {total_raw:,} ===")
df.printSchema()
df.show(5, truncate=False)

# ---------------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN
# ---------------------------------------------------------------------
cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "PULocationID", "payment_type",
        "fare_amount", "tip_amount", "total_amount"]

clean = (df.select(*cols)
         .dropna()
         .withColumn("pickup", F.col("tpep_pickup_datetime").cast("timestamp"))
         .withColumn("dropoff", F.col("tpep_dropoff_datetime").cast("timestamp"))
         .withColumn("duration_min",
                     (F.unix_timestamp("dropoff") - F.unix_timestamp("pickup")) / 60)
         .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 100))
         .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
         .filter(F.col("passenger_count").between(1, 6))
         .filter(F.col("duration_min").between(1, 180))
         # solo viajes cuyo inicio cae en enero de 2024
         .filter((F.col("pickup") >= "2024-01-01") & (F.col("pickup") < "2024-02-01"))
         .withColumn("hour", F.hour("pickup"))
         .withColumn("dow", F.dayofweek("pickup"))  # 1 = domingo ... 7 = sábado
         .withColumn("speed_mph", F.col("trip_distance") / (F.col("duration_min") / 60))
         .filter(F.col("speed_mph") < 100)
         .cache())

total_clean = clean.count()
print(f"\n=== Registros tras la limpieza: {total_clean:,} "
      f"({total_raw - total_clean:,} descartados) ===")

# ---------------------------------------------------------------------
# 3. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------------
print("\n--- Estadísticas descriptivas ---")
clean.select("trip_distance", "duration_min", "fare_amount",
             "tip_amount", "total_amount", "speed_mph").describe().show()

by_hour = (clean.groupBy("hour")
           .agg(F.count("*").alias("viajes"),
                F.round(F.avg("fare_amount"), 2).alias("tarifa_prom"),
                F.round(F.avg("trip_distance"), 2).alias("distancia_prom"))
           .orderBy("hour"))
print("\n--- Viajes por hora ---")
by_hour.show(24)

by_dow = (clean.groupBy("dow")
          .agg(F.count("*").alias("viajes"),
               F.round(F.avg("total_amount"), 2).alias("total_prom"))
          .orderBy("dow"))
print("\n--- Viajes por día de la semana ---")
by_dow.show()

by_payment = (clean.groupBy("payment_type")
              .agg(F.count("*").alias("viajes"))
              .orderBy(F.desc("viajes")))
print("\n--- Viajes por tipo de pago (1=tarjeta, 2=efectivo, ...) ---")
by_payment.show()

# Propina promedio (%) solo en pagos con tarjeta, donde la propina se registra
tip = (clean.filter(F.col("payment_type") == 1)
       .withColumn("tip_pct", F.col("tip_amount") / F.col("fare_amount") * 100)
       .agg(F.round(F.avg("tip_pct"), 2).alias("propina_prom_pct")))
print("\n--- Propina promedio con tarjeta ---")
tip.show()

# ---------------------------------------------------------------------
# 4. EJEMPLO CON RDDs (transformaciones y acciones)
# ---------------------------------------------------------------------
# Top 10 zonas de recogida: map -> reduceByKey -> takeOrdered
zonas = (clean.select("PULocationID").rdd
         .map(lambda r: (r["PULocationID"], 1))
         .reduceByKey(add))
top_zonas = zonas.takeOrdered(10, key=lambda kv: -kv[1])
print("\n--- Top 10 zonas de recogida (RDD) ---")
for zona, n in top_zonas:
    print(f"Zona {zona}: {n:,} viajes")

# ---------------------------------------------------------------------
# 5. VISUALIZACIÓN
# ---------------------------------------------------------------------
h = by_hour.toPandas()
d = by_dow.toPandas()
dias = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
d["dia"] = d["dow"].apply(lambda x: dias[x - 1])

plt.figure(figsize=(9, 4))
plt.bar(h["hour"], h["viajes"], color="#1f77b4")
plt.title("Viajes por hora del día (enero 2024)")
plt.xlabel("Hora")
plt.ylabel("Número de viajes")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/viajes_por_hora.png", dpi=120)
plt.close()

plt.figure(figsize=(9, 4))
plt.plot(h["hour"], h["tarifa_prom"], marker="o", color="#d62728")
plt.title("Tarifa promedio por hora del día (USD)")
plt.xlabel("Hora")
plt.ylabel("Tarifa promedio")
plt.xticks(range(0, 24))
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/tarifa_por_hora.png", dpi=120)
plt.close()

plt.figure(figsize=(7, 4))
plt.bar(d["dia"], d["viajes"], color="#2ca02c")
plt.title("Viajes por día de la semana")
plt.xlabel("Día")
plt.ylabel("Número de viajes")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/viajes_por_dia.png", dpi=120)
plt.close()

h.to_csv(f"{OUT_DIR}/resumen_por_hora.csv", index=False)
d.to_csv(f"{OUT_DIR}/resumen_por_dia.csv", index=False)

print(f"\nGráficas y resúmenes guardados en ./{OUT_DIR}/")
spark.stop()
