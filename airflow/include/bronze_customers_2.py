from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder \
    .appName("LeituraLakehouse") \
    .getOrCreate()

data = [
    ("John D.", 30, "Software Engineer"),
    ("Alice G.", 25, "Data Scientist"),
    ("Bob T.", 35, "Project Manager")
]

columns = ["Name", "Age", "Occupation"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()