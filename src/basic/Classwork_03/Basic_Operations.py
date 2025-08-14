# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Sesión 3: Operaciones Básicas con DataFrames
# MAGIC
# MAGIC En esta sesión aprenderás a realizar las operaciones fundamentales sobre un DataFrame de PySpark:
# MAGIC
# MAGIC ✅ Selección de columnas  
# MAGIC ✅ Filtrado de registros  
# MAGIC ✅ Ordenamiento  
# MAGIC ✅ Uso de alias para renombrar columnas  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Objetivo del ejercicio
# MAGIC
# MAGIC Aplicar operaciones básicas sobre una tabla existente en el catálogo de Hive (`samples.bakehouse.sales_customers`), la cual contiene información de clientes de una tienda de productos horneados.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📄 Esquema de la tabla `sales_customers`
# MAGIC
# MAGIC | Campo              | Tipo    | Descripción                           |
# MAGIC |--------------------|---------|----------------------------------------|
# MAGIC | customerID         | long    | ID del cliente                         |
# MAGIC | first_name         | string  | Nombre                                 |
# MAGIC | last_name          | string  | Apellido                               |
# MAGIC | email_address      | string  | Correo electrónico                     |
# MAGIC | phone_number       | string  | Teléfono                               |
# MAGIC | address            | string  | Dirección                              |
# MAGIC | city               | string  | Ciudad                                 |
# MAGIC | state              | string  | Estado/Provincia                       |
# MAGIC | country            | string  | País                                   |
# MAGIC | continent          | string  | Continente                             |
# MAGIC | postal_zip_code    | long    | Código postal                          |
# MAGIC | gender             | string  | Género (M/F/Otro)                      |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧪 Actividad práctica
# MAGIC
# MAGIC 1. Cargar el DataFrame desde la tabla del catálogo.
# MAGIC 2. Seleccionar columnas clave como `first_name`, `last_name`, `email_address`, `country`, `gender`.
# MAGIC 3. Filtrar los registros donde el país sea `Japan` y el género sea `F`.
# MAGIC 4. Ordenar los resultados por `last_name` y luego `first_name`.
# MAGIC 5. Renombrar columnas usando alias para visualización más clara.
# MAGIC
# MAGIC 🚀 Bonus: Mostrar cuántos registros cumplen la condición con `.count()`
# MAGIC
# MAGIC ---

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Leer la tabla del metastore
df = spark.table("samples.bakehouse.sales_customers")

# Vista previa de datos
display(df.limit(3))

# Seleccionar columnas relevantes
df_seleccionado = df.select(
    col("first_name").alias("nombre"),
    col("last_name").alias("apellido"),
    col("email_address").alias("email"),
    col("country").alias("pais"),
    col("gender").alias("genero")
)

# Filtrar registros: mujeres de Japon
df_filtrado = df_seleccionado.filter(
    (col("pais") == "Japan") & (col("genero") == "female")
)

# Ordenar por apellido y nombre
df_ordenado = df_filtrado.orderBy(col("apellido"), col("nombre"))

# Mostrar resultados
df_ordenado.show(truncate=False)

# Contar cuántos registros cumplen la condición
cantidad = df_ordenado.count()
print(f"Total de mujeres en Japon: {cantidad}")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧪 Ejercicio práctico — Sesión 3: Operaciones básicas con DataFrames
# MAGIC
# MAGIC ## 🗂️ Tabla: `samples.bakehouse.sales_transactions`
# MAGIC
# MAGIC Este ejercicio está basado en una tabla real del catálogo Hive que contiene el historial de transacciones de una cadena de panaderías. A partir de esta tabla, aplicaremos transformaciones y filtros utilizando funciones básicas de PySpark.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Objetivos
# MAGIC
# MAGIC 1. Aplicar **funciones sobre campos de fecha** para separar la fecha y la hora desde una columna `timestamp`.
# MAGIC 2. **Ofuscar el número de tarjeta** (`cardNumber`) mostrando solo los **primeros 3 dígitos** y reemplazando el resto por `X`.
# MAGIC 3. Filtrar las transacciones realizadas con el método de pago **"Visa"**.
# MAGIC 4. Mostrar las **3 transacciones con mayor valor (`totalPrice`) pagadas con Visa**, incluyendo información clave.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧱 Esquema de la tabla `sales_transactions`
# MAGIC
# MAGIC | Columna         | Tipo       | Descripción                             |
# MAGIC |------------------|------------|------------------------------------------|
# MAGIC | transactionID    | long       | ID de la transacción                     |
# MAGIC | customerID       | long       | ID del cliente                           |
# MAGIC | franchiseID      | long       | ID de la franquicia                      |
# MAGIC | dateTime         | timestamp  | Fecha y hora de la transacción           |
# MAGIC | product          | string     | Producto adquirido                       |
# MAGIC | quantity         | long       | Cantidad comprada                        |
# MAGIC | unitPrice        | long       | Precio unitario                          |
# MAGIC | totalPrice       | long       | Precio total                             |
# MAGIC | paymentMethod    | string     | Método de pago (ej. Visa, Cash, etc.)    |
# MAGIC | cardNumber       | long       | Número de tarjeta de crédito/débito      |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📝 Instrucciones
# MAGIC
# MAGIC 1. Leer la tabla desde el catálogo de Hive usando `spark.table(...)`.
# MAGIC 2. Crear nuevas columnas:
# MAGIC    - `fecha`: a partir de `dateTime` usando `to_date(...)`
# MAGIC    - `hora`: a partir de `dateTime` usando `date_format(...)`
# MAGIC 3. Crear una nueva columna llamada `cardMasked` que:
# MAGIC    - Muestre solo los **primeros 3 dígitos** de `cardNumber`.
# MAGIC    - Reemplace el resto con `"XXXXXXXXXXXX"` para ocultar la información sensible.
# MAGIC 4. Filtrar el DataFrame para quedarte **solo con registros donde `paymentMethod` sea "Visa"**.
# MAGIC 5. Ordenar las transacciones por `totalPrice` de mayor a menor.
# MAGIC 6. Mostrar solo las **3 transacciones de mayor valor**, mostrando las siguientes columnas:
# MAGIC    - `transactionID`, `fecha`, `hora`, `product`, `quantity`, `totalPrice`, `paymentMethod`, `cardMasked`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Resultado esperado
# MAGIC
# MAGIC Una tabla final con 3 filas que contengan solo transacciones Visa, ordenadas por monto total de compra, con fecha y hora separadas y tarjeta ofuscada.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎓 Bonus (opcional)
# MAGIC
# MAGIC - Agrupar por `fecha` y calcular el total vendido por día con Visa.
# MAGIC - Contar cuántas transacciones Visa se realizaron por ciudad o país si se integra con la tabla de clientes.
# MAGIC
# MAGIC ---

# COMMAND ----------

## Escribe aqui tu
