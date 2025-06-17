import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'kafka_bootstrap_servers',
    'kafka_topic'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

# Schema de exemplo
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("ClienteID", StringType(), True),
    StructField("Data", StringType(), True),
    StructField("Valor", DoubleType(), True),
    StructField("Moeda", StringType(), True)
])

# Leitura contínua do Kafka (streaming)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args['kafka_bootstrap_servers']) \
    .option("subscribe", args['kafka_topic']) \
    .option("startingOffsets", "earliest") \
    .load()

# Parsing do JSON
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Escrita no console (logs do CloudWatch)
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
