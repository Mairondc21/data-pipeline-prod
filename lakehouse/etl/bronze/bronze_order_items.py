import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ETL_DIR = os.path.join(BASE_DIR, '..')
sys.path.insert(0, os.path.normpath(ETL_DIR))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from clients.spark_client import spark

tabela = "order_items"
caminho = f"s3a://raw-data/postgres/raw_data/{tabela}/"

df = spark.spark.read.format("parquet").load(caminho)

df = df.select(
    col("discount").alias("desconto"),
    "order_id",
    col("quantity").alias("quantidade"),
    "subtotal",
    "product_id",
    col("unit_price").alias("preco_unitario"),
    "order_item_id"
)

df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/bronze/order_items")