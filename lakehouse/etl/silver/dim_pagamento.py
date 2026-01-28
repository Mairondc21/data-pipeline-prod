from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, when, upper, col

spark = SparkSession.builder\
        .appName("Dimensao Pagamento")\
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

window_spec = Window().orderBy("tipo_pagamento")
df = df.select(
    upper("tipo_pagamento").alias("tipo_pagamento"),
).distinct()

df = df.withColumns({
    "sk_pagamento": row_number().over(window_spec),
    "desc_tipo_pagamento": when(col("tipo_pagamento") == "BOLETO","Pagamento feito pelo boleto")\
                           .when(col("tipo_pagamento") == 'CREDIT CARD', 'Pagamento aceita somente as bandeiras Mastercard e Visa')\
                           .when(col("tipo_pagamento") == 'DEBIT CARD', 'Pagamento aceita somente as bandeiras Mastercard e Visa')\
                           .when(col("tipo_pagamento") == 'PAYPAL', 'Pagamento via Paypal')\
                           .when(col("tipo_pagamento") == 'PIX', 'Pagamento feito com a chave aleatoria')
                           .otherwise("Pagamento invalido")
})


df.write.format("parquet").mode("append").save("s3a://raw-data/postgres/silver/dim_pagamento")