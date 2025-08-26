from pyspark.sql.functions import col, lower, trim

def limpiar_columna_nombre(df, columna_entrada="nombre", columna_salida="nombre_limpio"):
    return df.withColumn(columna_salida, trim(lower(col(columna_entrada))))