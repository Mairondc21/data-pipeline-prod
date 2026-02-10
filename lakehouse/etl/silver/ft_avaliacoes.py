from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder\
        .appName("Fato avaliacoes")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:3900") \
        .config("spark.hadoop.fs.s3a.access.key", "GK092b24f828f48e9106904881") \
        .config("spark.hadoop.fs.s3a.secret.key", "dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

dim_cliente = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_cliente")
dim_produto = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_produto")
dim_data = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_data")

bronze_review = spark.read.format("parquet").load("s3a://raw-data/postgres/bronze/reviews")

join_review_cliente = bronze_review.join(dim_cliente,
                                         "customer_id",
                                         "left")\
                                         .select(
                                           *bronze_review,
                                           dim_cliente.sk_cliente
                                         ).drop("customer_id")

join_review_produto = join_review_cliente.join(dim_produto,
                                               "product_id",
                                               "left")\
                                               .select(
                                                *join_review_cliente,
                                                dim_produto.sk_produto
                                               ).drop("product_id")
join_review_data = join_review_produto.join(dim_data,
                                            join_review_produto.dta_analise == dim_data.data_completa,
                                            "left")\
                                            .select(
                                               *join_review_produto,
                                                dim_data.sk_date
                                            ).drop("dta_analise")
window_spec = Window().orderBy("review_id")

df = join_review_data.withColumn("sk_avaliacao", row_number().over(window_spec))

df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/silver/ft_avaliacoes")