from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import upper, col, sum as spark_sum, round, row_number

spark = SparkSession.builder\
        .appName("Fato vendas")\
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
dim_endereco_entrega = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_endereco_entrega")
dim_pagamento = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_pagamento")
dim_data = spark.read.format("parquet").load("s3a://raw-data/postgres/silver/dim_data")

bronze_order = spark.read.format("parquet").load("s3a://raw-data/postgres/bronze/orders")
bronze_order_item = spark.read.format("parquet").load("s3a://raw-data/postgres/bronze/order_items")

df_join_order = bronze_order_item.join(bronze_order, "order_id", "inner")

df_join_cliente = df_join_order.join(dim_cliente.where("flag_atual = true"), "customer_id", "left")\
                    .select(
                        *df_join_order,
                        dim_cliente.sk_cliente
                    )
df_join_produto = df_join_cliente.join(dim_produto.where("flag_atual = true"), "product_id", "left")\
                    .select(
                        *df_join_cliente,
                        dim_produto.sk_produto,
                        dim_produto.custo
                    )
df_join_endereco_entrega = df_join_produto.join(dim_endereco_entrega, "cep", "left")\
                            .select(
                                *df_join_produto,
                                dim_endereco_entrega.sk_endereco
                            )
df_join_endereco_entrega = df_join_endereco_entrega.withColumn("tipo_pagamento_new", upper("tipo_pagamento"))

df_join_pagamento = df_join_endereco_entrega.join(dim_pagamento, 
                                                  df_join_endereco_entrega.tipo_pagamento_new == dim_pagamento.tipo_pagamento, 
                                                  "left")\
                                                    .select(
                                                        *df_join_endereco_entrega,
                                                        dim_pagamento.sk_pagamento
                                                    )

# dim_data_pedido = dim_data.alias("dim_pedido").select(
#     col("data_completa").alias("data_pedido"),
#     col("sk_date").alias("sk_data_pedido")
# )

# dim_data_entrega = dim_data.alias("dim_entrega").select(
#     col("data_completa").alias("data_entrega"),
#     col("sk_date").alias("sk_data_entrega_estimada")
# )

# # Fazer os dois joins em sequência
# df_final = df_join_pagamento \
#     .join(
#         dim_data_pedido,
#         df_join_pagamento["order_date"] == dim_data_pedido["data_pedido"],
#         "left"
#     ) \
#     .join(
#         dim_data_entrega,
#         df_join_pagamento["estimativa_de_entrega"] == dim_data_entrega["data_entrega"],
#         "left"
#     ) \
#     .select(
#         "sk_cliente",
#         "sk_produto",
#         "sk_endereco",
#         "sk_pagamento",
#         "sk_data_pedido",
#         "sk_data_entrega_estimada",
#         "order_id",
#         "order_item_id",
#         "quantidade",
#         "preco_unitario",
#         "desconto",
#         "subtotal",
#         "status"
#     )

df_join_data_pedido = df_join_pagamento.join(dim_data,
                                             df_join_pagamento.order_date == dim_data.data_completa,
                                             "left")\
                                             .select(
                                                 *df_join_pagamento,
                                                 dim_data["sk_date"].alias("sk_data_pedido")
                                             )
dim_data_entrega = dim_data.select(
    col("sk_date").alias("sk_data_entrega_estimada"),
    col("data_completa").alias("dta_entrega")
)

df_join_data_entrega = df_join_data_pedido.join(dim_data_entrega,
                                             df_join_data_pedido.estimativa_de_entrega == dim_data_entrega.dta_entrega,
                                             "left")\
                                             .select(
                                                 "sk_cliente",
                                                 "sk_produto",
                                                 "sk_endereco",
                                                 "sk_pagamento",
                                                 "sk_data_pedido",
                                                 dim_data_entrega["sk_data_entrega_estimada"],
                                                 "order_id",
                                                 "order_item_id",
                                                 "quantidade",
                                                 "preco_unitario",
                                                 "desconto",
                                                 "subtotal",
                                                 "status",
                                                 "frete",
                                                 "custo"
                                             )

window_spec_sum = Window().partitionBy("order_id").orderBy("order_id")

df_subtotal = df_join_data_entrega.withColumn("subtota_final", round(col("subtotal") + col("frete"),2))\
                .drop("subtotal")\
                .withColumnRenamed("subtota_final","subtotal")

window_spec_sk_fato = Window().orderBy("sk_cliente")


df = df_subtotal.withColumns({
    "total": round(spark_sum("subtotal").over(window_spec_sum),2),
    "margem_bruta": round(col("preco_unitario") - col("custo"),2),
    "sk_vendas": row_number().over(window_spec_sk_fato)
}).drop("custo")


df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/silver/ft_vendas")