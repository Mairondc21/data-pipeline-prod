from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
spark = SparkSession.builder\
        .appName("Dimensao Endereco de entrega")\
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

window_spec = Window().orderBy("cep")

df = df.withColumn("sk_endereco", row_number().over(window_spec))

df = df.select(
    "sk_endereco",
    "endereco_envio",
    "cidade_envio",
    "estado_envio",
    "cep"
)

df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/silver/dim_endereco_entrega")