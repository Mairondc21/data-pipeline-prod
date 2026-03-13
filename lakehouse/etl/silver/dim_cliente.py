from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, current_date, lit
from pyspark.sql.types import StructField, StringType, StructType, DateType, LongType
from pyspark.sql.window import Window

spark = SparkSession.builder\
        .appName("ETL Silver dimensão cliente")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:3900") \
        .config("spark.hadoop.fs.s3a.access.key", "GK092b24f828f48e9106904881") \
        .config("spark.hadoop.fs.s3a.secret.key", "dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

tabela = "customers"
caminho = f"s3a://raw-data/postgres/bronze/{tabela}/"

_schema = StructType([
    StructField("customer_id", LongType(), False),
    StructField("cpf", StringType(), False),
    StructField("cidade", StringType(), True),
    StructField("email", StringType(), False),
    StructField("tel", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("genero", StringType(), False),
    StructField("endereco", StringType(), False),
    StructField("cep", StringType(), False),
    StructField("sobrenome", StringType(), True),
    StructField("dta_nas", DateType(), False),
    StructField("nome", StringType(), False)
])

df = spark.read.format("parquet").schema(_schema).load(caminho)

window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))

df = df.withColumns({
    "sk_cliente": row_number().over(window_spec),
    "dta_inicio_vigencia": current_date(),
    "dta_fim_vigencia": lit("1900-01-01"),
    "flag_atual": lit(True)
    })

df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/silver/dim_cliente")