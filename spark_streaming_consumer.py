from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, from_unixtime, avg, round as fround
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

# Crear la sesion de Spark y reducir los mensajes INFO
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos de entrada.
# El productor envia el timestamp como entero (segundos epoch),
# por eso se lee como LongType y luego se convierte a fecha.
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

# Lector de streaming desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsear el JSON y convertir el timestamp a tipo fecha/hora
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))

# Estadisticas por ventana de 1 minuto y por sensor
windowed_stats = parsed_df \
    .groupBy(window(col("event_time"), "1 minute"), "sensor_id") \
    .agg(fround(avg("temperature"), 2).alias("avg_temperature"),
         fround(avg("humidity"), 2).alias("avg_humidity"))

# Escribir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
