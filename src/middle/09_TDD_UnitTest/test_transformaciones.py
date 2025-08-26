from pyspark.sql import SparkSession
from transformaciones import limpiar_columna_nombre

# Inicializar Spark (solo una vez en los tests)
spark = SparkSession.builder.appName("con_tdd").getOrCreate()

# Crear un DataFrame de prueba
data = [("  PEPE ",), ("  maria",)]
df = spark.createDataFrame(data, ["nombre"])

# Ejecutar función
df_limpio = limpiar_columna_nombre(df)

# Comprobación básica
resultados = [row["nombre_limpio"] for row in df_limpio.select("nombre_limpio").collect()]
assert resultados == ["pepe", "maria"], "❌ La limpieza de nombres no funcionó correctamente."
assert df_limpio.isEmpty() == False, "❌ El DataFrame resultante está vacío."

print("✅ Test pasado correctamente.")