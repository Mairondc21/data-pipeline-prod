from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder \
    .appName("LeituraLakehouse") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint","http://garage-s3:3900") \
    .config("spark.hadoop.fs.s3a.access.key","GK092b24f828f48e9106904881") \
    .config("spark.hadoop.fs.s3a.secret.key","dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
    .config("spark.hadoop.fs.s3a.path.style.access","true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout","60000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum","3") \
    .getOrCreate()

tabela = "customers"
caminho = f"s3a://raw-data/postgres/raw_data/{tabela}/"

df = spark.read.format("parquet").load(caminho)

df = df.select(
    col("cpf"),
    col("city").alias("cidade"),
    "email",
    "customer_id",
    col("phone").alias("tel"),
    col("state").alias("uf"),
    col("gender").alias("genero"),
    col("address").alias("endereco"),
    col("zipcode").alias("cep"),
    col("last_name").alias("sobrenome"),
    col("birth_date").alias("dta_nas"),
    col("first_name").alias("nome")
)

df = df \
    .withColumn("new_cpf", regexp_replace(col("cpf"), "[.-]", "")) \
    .withColumn("new_tel", regexp_replace(col("tel"), "[()+]", "")) \
    .withColumn("new_cep", regexp_replace(col("cep"), "-", ""))

df = df.select(
    "customer_id",
    col("new_cpf").alias("cpf"),
    "cidade",
    "email",
    col("new_tel").alias("tel"),
    "uf",
    "genero",
    "endereco",
    col("new_cep").alias("cep"),
    "sobrenome",
    "dta_nas",
    "nome"
)

df.write.mode("overwrite").format("parquet").save(
    "s3a://raw-data/postgres/bronze/customers"
)