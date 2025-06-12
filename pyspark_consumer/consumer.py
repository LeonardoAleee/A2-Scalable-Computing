import os
import sys

# Define o HADOOP_HOME programaticamente para a pasta local do projeto
try:
    if sys.platform.startswith('win'):
        print("Plataforma Windows detectada. Configurando HADOOP_HOME...")
        project_root = os.path.dirname(os.path.abspath(__file__))
        hadoop_utils_path = os.path.join(os.path.dirname(project_root), 'hadoop_utils')
        
        print(f"Definindo HADOOP_HOME como: {hadoop_utils_path}")
        os.environ['HADOOP_HOME'] = hadoop_utils_path
        
        # Adiciona o diretório bin ao PATH para que o winutils.exe seja encontrado
        sys.path.append(os.path.join(hadoop_utils_path, 'bin'))
        print("Configuração de ambiente para Windows concluída.")
except Exception as e:
    print(f"Ocorreu um erro ao configurar o HADOOP_HOME: {e}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaPySparkStreaming") \
        .config("spark.jars.packages", KAFKA_PACKAGE) \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

def define_schemas():
    transaction_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("ClienteID", StringType(), True),
        StructField("Data", StringType(), True), # Lendo como string, pode ser convertido para Timestamp depois
        StructField("Valor", DoubleType(), True),
        StructField("Moeda", StringType(), True)
    ])

    score_schema = StructType([
        StructField("CPF", StringType(), True),
        StructField("Score", IntegerType(), True),
        StructField("RendaMensal", DoubleType(), True),
        StructField("LimiteCredito", DoubleType(), True),
        StructField("AtualizadoEm", StringType(), True)
    ])
    
    return transaction_schema, score_schema

def process_streams(spark, transaction_schema, score_schema):
    """Lê dos tópicos Kafka e processa as análises."""
    kafka_bootstrap_servers = 'localhost:29092'

    # Leitura do tópico de transações
    trans_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "transacoes") \
        .option("startingOffsets", "latest") \
        .load()

    # Leitura do tópico de scores
    score_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "scores") \
        .option("startingOffsets", "latest") \
        .load()

    # --------------- Análise 1: Volume de Transações por Moeda ---------------
    parsed_trans_df = trans_df.select(from_json(col("value").cast("string"), transaction_schema).alias("data")).select("data.*")
    
    transactions_by_currency = parsed_trans_df.groupBy("Moeda").agg(
        _sum("Valor").alias("VolumeTotal"),
        count("ID").alias("Quantidade")
    )

    # --------------- Análise 2: Distribuição de Clientes por Faixa de Score ---------------
    parsed_score_df = score_df.select(from_json(col("value").cast("string"), score_schema).alias("data")).select("data.*")

    score_distribution = parsed_score_df.withColumn("FaixaScore",
        when(col("Score") < 300, "Baixo")
        .when((col("Score") >= 300) & (col("Score") < 700), "Médio")
        .when((col("Score") >= 700) & (col("Score") < 900), "Alto")
        .otherwise("Excelente")
    ).groupBy("FaixaScore").count()

    # Saída para o console
    query1 = transactions_by_currency.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query2 = score_distribution.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    return query1, query2

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    transaction_schema, score_schema = define_schemas()
    
    query1, query2 = process_streams(spark, transaction_schema, score_schema)
    
    print("Streaming queries iniciadas. Pressione Ctrl+C para parar.")
    
    # Aguarda a finalização das queries
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main() 