from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col,round as spark_round,count

spark = SparkSession.builder\
        .appName("Fato Estoque")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:3900") \
        .config("spark.hadoop.fs.s3a.access.key", "GK092b24f828f48e9106904881") \
        .config("spark.hadoop.fs.s3a.secret.key", "dcd93f148c32faca0074310ae16845b4f94b9a52f111d2ae07604ac12a8dd7f2") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

dim_produto = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_produto")
dim_data = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_data")

df = dim_produto.where("flag_atual = true").join(dim_data,
                      dim_produto["dta_inicio_vigencia"] == dim_data["data_completa"],
                      "inner").select(
                          "sk_produto",
                          "sk_date",
                          "product_id",
                          "custo",
                          "preco",
                          "quantidade_em_estoque"
                      )
df = df.withColumns({
    "custo_total_estoque": spark_round(col("custo") * col("quantidade_em_estoque"),2),
    "valor_total_estoque": spark_round(col("preco") * col("quantidade_em_estoque"),2)
}).drop("custo","preco")

df.write.format("parquet").mode("overwrite").save("s3a://raw-data/postgres/silver/ft_estoque")