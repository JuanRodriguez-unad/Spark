from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear una sesión Spark
spark = SparkSession.builder.appName("ChocolateSalesBatch").getOrCreate()

# Cargar el dataset desde un archivo CSV
df = spark.read.csv("s3://unadhome/Tarea/input/Chocolate Sales.csv", header=True, inferSchema=True)

# Mostrar las primeras filas del dataframe
df.show()

# Análisis exploratorio:
# Verificar el esquema de los datos (tipos de datos y estructura)
df.printSchema()

# Resumen estadístico de las columnas numéricas
df.describe().show()

# Verificar valores nulos en cada columna
df.select([col(c).isNull().alias(c) for c in df.columns]).show()

# Verificar las distribuciones de las variables "Amount" y "Boxes Shipped"
df.groupBy("Amount").count().show()  # Distribución de las ventas
df.groupBy("Boxes Shipped").count().show()  # Distribución de cajas enviadas

# Limpiar los datos: Eliminar filas con valores nulos
df_clean = df.dropna()

# Transformar los datos: Crear una nueva columna "Income" (Ingreso)
# Primero, necesitamos eliminar el símbolo '$' y convertir "Amount" a numérico
df_clean = df_clean.withColumn("Amount", df_clean["Amount"].substr(2, 100).cast("float"))

# Ahora calculamos el ingreso multiplicando "Amount" por "Boxes Shipped"
df_clean = df_clean.withColumn("Income", df_clean["Amount"] * df_clean["Boxes Shipped"])

# Realizar un análisis exploratorio: Contar ventas por país
df_sales_by_country = df_clean.groupBy("Country").sum("Income")
df_sales_by_country.show()

# Almacenar los resultados procesados en Parquet
df_clean.write.parquet("s3://unadhome/Tarea/output")
