from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder\
        .appName("ETL Bronze order items")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:3900") \
        .config("spark.hadoop.fs.s3a.access.key", "GK092b24f828f48e9106904881") \
        .config("spark.hadoop.fs.s3a.secret.key", "dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

tabela = "order_items"
caminho = f"s3a://raw-data/postgres/raw_data/{tabela}/"

df = spark.read.format("parquet").load(caminho)

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