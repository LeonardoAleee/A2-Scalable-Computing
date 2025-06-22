import os
import sys
import logging
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,from_json, col, sum as _sum, when, count, avg, window, to_date, current_timestamp, desc, year, to_timestamp, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.window import Window

# --- Configuração do logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - PysparkConsumerDB - %(message)s',
    stream=sys.stdout
)

# --- Configurações ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToPostgresStreaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.0") \
        .getOrCreate()

def define_schemas():
    transaction_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("ClienteID", StringType(), True),
        StructField("Data", StringType(), True),
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
    client_schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Nome", StringType(), True),
        StructField("CPF", StringType(), True),
        StructField("DataNasc", StringType(), True),
        StructField("Endereco", StringType(), True)
    ])
    return transaction_schema, score_schema, client_schema

# -------- Funções de Escrita no Banco de Dados --------

def write_truncate(df, epoch_id, table_name):
    if df.rdd.isEmpty():
        logging.info(f"write_truncate: DataFrame para '{table_name}' está vazio. Pulando.")
        df.write.format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", table_name) \
            .option("truncate", "true") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        return
    df.write.format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", table_name) \
        .option("truncate", "true") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def write_postgres_upsert(df, epoch_id, table_name, pk_column):
    data = df.collect()
    if not data:
        return

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        cols = df.columns
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        
        update_cols = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk_column])
        
        sql = f"""
            INSERT INTO {table_name} ({cols_sql})
            VALUES %s
            ON CONFLICT ({pk_column}) DO UPDATE SET
            {update_cols};
        """
        
        values = [tuple(row) for row in data]
        execute_values(cursor, sql, values)

        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

# --- Função para Processar o Top 10 ---
def process_and_write_top_10(df, epoch_id):
    windowSpec = Window.orderBy(desc("TotalGasto"))
    top_10_df = df.withColumn("rank", row_number().over(windowSpec)) \
                   .filter(col("rank") <= 10) \
                   .select(col("rank"), col("ClienteID").alias("cliente_id"), col("TotalGasto").alias("total_gasto"))
    
    if not top_10_df.rdd.isEmpty():
        write_truncate(top_10_df, epoch_id, "analysis_top_10_clientes")

def log_message_consumption(df, epoch_id):
    if df.rdd.isEmpty():
        return
    
    consumed_df = df.withColumn("consume_timestamp", current_timestamp())
    
    for row in consumed_df.select("ID", "consume_timestamp").collect():
        logging.info(f"CONSUME_MSG_LOG: id={row['ID']}, timestamp={row['consume_timestamp'].isoformat()}")

def process_streams(spark, schemas):
    transaction_schema, score_schema, client_schema = schemas

    # Leitura dos Tópicos Kafka
    def read_topic(topic_name):
        return spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic_name) \
            .load()

    trans_df = read_topic("transacoes")
    score_df = read_topic("scores")
    client_df = read_topic("clientes")

    # Parse e Preparação dos Dados
    parsed_trans_df = trans_df.select(from_json(col("value").cast("string"), transaction_schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("Data")))
    parsed_score_df = score_df.select(from_json(col("value").cast("string"), score_schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("AtualizadoEm")))
    parsed_client_df = client_df.select(from_json(col("value").cast("string"), client_schema).alias("data")).select("data.*").withColumn("timestamp", current_timestamp()).withColumn("DataNasc", to_date(col("DataNasc"), "yyyy-MM-dd"))

    # --- Logging de Latência ---
    parsed_trans_df.writeStream \
        .outputMode("append") \
        .foreachBatch(log_message_consumption) \
        .start()

    # --- Definição das Análises ---

    # 1. Distribuição por Score (UPSERT)
    score_distribution = parsed_score_df.withColumn("faixa_score",
        when(col("Score") < 300, "Baixo")
        .when((col("Score") >= 300) & (col("Score") < 700), "Médio")
        .when(col("Score") >= 700, "Alto")
        .otherwise("Excelente")
    ).groupBy("faixa_score").agg(count("*").alias("count"))

    # 2. Ticket Médio (UPSERT - tabela de 1 linha)
    ticket_medio = parsed_trans_df.agg(
        coalesce(avg("Valor"), lit(0.0)).alias("ticket_medio")
    ).withColumn("id", lit("singleton"))

    # 3. Volume por Moeda (UPSERT)
    volume_by_currency = parsed_trans_df.groupBy("Moeda").agg(
        _sum("Valor").alias("volume_total"), 
        count("ID").alias("quantidade")
    ).withColumnRenamed("Moeda", "moeda")

    # 4. Top 10 Clientes - Apenas agregação aqui. O ranking será feito no foreachBatch.
    cliente_gastos = parsed_trans_df \
        .groupBy("ClienteID").agg(_sum("Valor").alias("TotalGasto"))

    # 5. Distribuição por Idade (UPSERT)
    age_distribution = parsed_client_df \
        .withColumn("Idade", (year(current_timestamp()) - year(col("DataNasc")))) \
        .withColumn("faixa_etaria",
            when(col("Idade") < 25, "18-24")
            .when((col("Idade") >= 25) & (col("Idade") < 35), "25-34")
            .when((col("Idade") >= 35) & (col("Idade") < 45), "35-44")
            .when((col("Idade") >= 45) & (col("Idade") < 55), "45-54")
            .when(col("Idade") >= 55, "55+")
            .otherwise("N/A")
        ).groupBy("faixa_etaria").agg(count("*").alias("count"))
    
    # 6. Alertas de Risco (Acumulativo)
    risk_alerts = parsed_score_df \
        .filter((col("Score") < 400) & (col("LimiteCredito") > 15000)) \
        .select(
            col("CPF").alias("cpf"),
            col("Score").alias("score"),
            col("LimiteCredito").alias("limite_credito")
        )

    # 7. Volume Diário (Histórico - UPSERT)
    daily_volume = parsed_trans_df.withWatermark("timestamp", "1 day") \
        .groupBy(window("timestamp", "1 day")).agg(_sum("Valor").alias("total_volume")) \
        .select(col("window.start").cast(DateType()).alias("date"), "total_volume")

    # 8. Novos Clientes Diário (Histórico - UPSERT)
    daily_new_clients = parsed_client_df.withWatermark("timestamp", "1 day") \
        .groupBy(window("timestamp", "1 day")).agg(count("ID").alias("new_clients_count")) \
        .select(col("window.start").cast(DateType()).alias("date"), "new_clients_count")

    # 9. Score Médio Diário (Histórico - UPSERT)
    daily_average_score = parsed_score_df.withWatermark("timestamp", "1 day") \
        .groupBy(window("timestamp", "1 day")).agg(avg("Score").alias("average_score")) \
        .select(col("window.start").cast(DateType()).alias("date"), "average_score")

    
    # --- Análises que precisam de UPSERT para manter o estado atual ---
    score_distribution.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "analysis_score_distribution", 
            pk_column="faixa_score")) \
        .start()

    volume_by_currency.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "analysis_volume_by_currency", 
            pk_column="moeda")) \
        .start()

    age_distribution.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "analysis_age_distribution", 
            pk_column="faixa_etaria")) \
        .start()

    # --- Análises que precisam SOBRESCREVER (UPSERT) ---
    ticket_medio.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "analysis_ticket_medio", 
            pk_column="id")) \
        .start()

    risk_alerts.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "analysis_risk_alerts", 
            pk_column="cpf")) \
        .start()

    daily_volume.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "daily_transaction_volume", 
            pk_column="date")) \
        .start()

    daily_new_clients.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "daily_new_clients", 
            pk_column="date")) \
        .start()

    daily_average_score.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, 
            "daily_average_score", 
            pk_column="date")) \
        .start()

    # --- Análises que precisam APAGAR E REESCREVER (TRUNCATE) ---
    cliente_gastos.writeStream.outputMode("complete").foreachBatch(process_and_write_top_10).start()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logging.info("Definindo schemas...")
    schemas = define_schemas()
    
    logging.info("Iniciando processamento dos streams para o banco de dados...")
    process_streams(spark, schemas)
    
    logging.info("Streaming queries iniciadas. Pressione Ctrl+C para parar.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main() 