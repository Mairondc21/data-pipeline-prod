from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, DataType
from pyspark.sql.functions import col, replace, lit, regexp_replace

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

tabela = "customers"
caminho = f"s3a://raw-data/postgres/raw_data/{tabela}/"


df = spark.read.format("parquet").load(caminho)

df = df.select(
    col("cpf"),
    col("city").alias("cidade"),
    "email",
    col("phone").alias("tel"),
    col("state").alias("uf"),
    col("gender").alias("genero"),
    col("address").alias("endereco"),
    col("zipcode").alias("cep"),
    col("last_name").alias("sobrenome"),
    col("birth_date").alias("dta_nas"),
    col("first_name").alias("nome")
)

df = df.withColumns({
    "new_cpf": regexp_replace(col("cpf"), "[.-]", ""),
    "new_tel": regexp_replace(col("tel"), "[()+]",""),
    "new_cep": regexp_replace(col("cep"), "\-","")
    })
df = df.select(
    col("new_cpf").alias("cpf"),
    col("cidade"),
    "email",
    col("new_tel").alias("tel"),
    col("uf"),
    col("genero"),
    col("endereco"),
    col("new_cep").alias("cep"),
    col("sobrenome"),
    col("dta_nas"),
    col("nome")
)

df.write.mode("overwrite").format("parquet").save("s3a://raw-data/postgres/bronze/customers")
