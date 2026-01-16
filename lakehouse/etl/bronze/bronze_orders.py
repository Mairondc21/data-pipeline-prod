from pyspark.sql import SparkSession
from pyspark.sql.functions import col,upper, regexp_replace

spark = SparkSession.builder \
    .appName("LeituraLakehouse") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:3900") \
    .config("spark.hadoop.fs.s3a.access.key", "GK092b24f828f48e9106904881") \
    .config("spark.hadoop.fs.s3a.secret.key", "dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

tabela = "orders"
caminho = f"s3a://raw-data/postgres/raw_data/{tabela}/"

df = spark.read.format("parquet").load(caminho)

df = df.select(
    upper(col("status")).alias("status"),
    "order_id",
    col("order_date").cast("date"),
    "customer_id",
    col("total_amount").alias("total"),
    col("shipping_city").alias("cidade_envio"),
    col("shipping_cost").alias("frete"),
    col("payment_method").alias("tipo_pagamento"),
    col("shipping_state").alias("estado_envio"),
    col("shipping_address").alias("endereco_envio"),
    col("shipping_zipcode").alias("cep"),
    col("estimated_delivery").alias("estimativa_de_entrega")
)

df = df.withColumn("new_cep", regexp_replace("cep","\-",""))

df = df.select(
    "status",
    "order_id",
    "order_date",
    "customer_id",
    "total",
    "cidade_envio",
    "frete",
    "tipo_pagamento",
    "estado_envio",
    "endereco_envio",
    col("new_cep").alias("cep"),
    "estimativa_de_entrega"
)

df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/bronze/orders")
