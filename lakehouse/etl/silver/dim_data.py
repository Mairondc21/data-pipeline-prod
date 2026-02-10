from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import date_format, when, col, lit, row_number
from datetime import datetime, timedelta

spark = SparkSession.builder\
        .appName("Dimensao Data")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:3900") \
        .config("spark.hadoop.fs.s3a.access.key", "GK092b24f828f48e9106904881") \
        .config("spark.hadoop.fs.s3a.secret.key", "dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

tabela = 'orders'
caminho = f"s3a://raw-data/postgres/bronze/{tabela}/"

df = spark.read.format("parquet").load(caminho)

df_order_date = df.select(
    "order_date"
)

df_estimativa_entrega = df.select(
    "estimativa_de_entrega"
)

df_union = df_order_date.union(df_estimativa_entrega).distinct()
dim_produto = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_produto")

dim_produto = dim_produto.select("dta_inicio_vigencia")

window_spec = Window().orderBy("order_date")

df = df_union.withColumns({
    "sk_date": row_number().over(window_spec),
    "ano": date_format("order_date","yyyy"),
    "mes": date_format("order_date","MM"),
    "dia": date_format("order_date","d"),
    "trimestre": date_format("order_date","q"),
    "semestre": when(col("trimestre").isin([1,2]), lit(1)).otherwise(lit(2)),
    "nome_mes": date_format("order_date","MMM"),
    "dia_semana": date_format("order_date","E"),
    "flag_fim_semana": when(col("dia_semana").isin(["Sat","Sun"]),lit(True)).otherwise(lit(False))
}).withColumnRenamed("order_date","data_completa")


df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/silver/dim_data")