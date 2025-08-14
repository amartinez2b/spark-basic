from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import time

# Crear sesión de Spark
spark = SparkSession.builder.appName("ParticionesYParalelismo").getOrCreate()
# Crear un DataFrame grande de prueba con 20 millones de filas
df = spark.range(0, 20_000_000).withColumn("valor", (rand() * 1000).cast("int"))
# Ver cuántas particiones tiene inicialmente
print(f"Particiones iniciales: {df.rdd.getNumPartitions()}")
# Reparticionar el DataFrame a un número mayor de particiones
df_repartitioned = df.repartition(20)
print(f"Después de repartition(20): {df_repartitioned.rdd.getNumPartitions()}")
# Escribir en disco y medir el tiempo
start = time.time()
df_repartitioned.write.mode("overwrite").parquet("/content/sample_data/dataset_repartitioned")
end = time.time()
print(f"Tiempo de escritura con repartition: {end - start:.2f} segundos")
# Leer el dataset con muchas particiones
df_read = spark.read.parquet("/content/sample_data/dataset_repartitioned")
print(f"Particiones al leer: {df_read.rdd.getNumPartitions()}")

# Reducir el número de particiones con coalesce
df_coalesced = df_read.coalesce(1)
print(f"Después de coalesce(1): {df_coalesced.rdd.getNumPartitions()}")
# Escribir en disco y medir el tiempo
start = time.time()
df_coalesced.write.mode("overwrite").parquet("/tmp/dataset_coalesced")
end = time.time()
print(f"Tiempo de escritura con coalesce: {end - start:.2f} segundos")

# Mantener la sesión abierta para observación en UI si es necesario
time.sleep(120)
# Detener la sesión
spark.stop()