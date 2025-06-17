import os
import sys
import redis
import json
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - PysparkConsumer - %(message)s',
    stream=sys.stdout
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, when, count, avg, window, to_date, current_timestamp, desc, year, to_timestamp, date_format, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# --- Kafka e Redis Config ---
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")

# --- RDS Config ---
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}" if DB_HOST else None

def create_spark_session():
    builder = SparkSession.builder \
        .appName("KafkaPySparkStreaming") \
        .config("spark.sql.shuffle.partitions", 4)
    
    return builder.getOrCreate()

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

def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# -------- Funções de Escrita no RDS (para foreachBatch) --------
def write_to_rds(spark, df, epoch_id, table_name, p_key, value_col):
    mode = "overwrite"
    temp_table_name = f"{table_name}_temp_{epoch_id}"

    try:
        if df.rdd.isEmpty():
            logging.debug(f"write_to_rds({table_name}): DataFrame está vazio.")
            return

        logging.info(f"Escrevendo batch (Epoch ID: {epoch_id}) para a tabela {table_name}.")
        
        # Escreve o DataFrame do batch em uma tabela temporária
        df.write.format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", temp_table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()

        DriverManager = spark._jvm.java.sql.DriverManager
        conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
        
        update_set_clause = f"{value_col} = EXCLUDED.{value_col}"
        
        sql = f"""
            INSERT INTO {table_name} ({p_key}, {value_col})
            SELECT {p_key}, {value_col} FROM {temp_table_name}
            ON CONFLICT ({p_key}) DO UPDATE SET {update_set_clause}
        """
        stmt = conn.createStatement()
        stmt.executeUpdate(sql)
        stmt.executeUpdate(f"DROP TABLE {temp_table_name}")
        stmt.close()
        conn.close()

        logging.info(f"Batch (Epoch ID: {epoch_id}) para a tabela {table_name} escrito com sucesso.")

    except Exception as e:
        logging.error(f"write_to_rds({table_name}): Falha ao escrever para o RDS (Epoch ID: {epoch_id}): {e}", exc_info=True)

# -------- Funções de Escrita no Redis (para foreachBatch) --------

def write_to_redis_generic(df, epoch_id, key_name, field_col, value_col):
    try:
        if df.rdd.isEmpty():
            logging.info(f"write_to_redis_generic({key_name}): DataFrame está vazio. Nada a fazer.")
            return
        
        r = get_redis_connection()
        records = df.toJSON().map(lambda x: json.loads(x)).collect()
        
        logging.info(f"write_to_redis_generic({key_name}): Escrevendo {len(records)} registros no Redis.")
        with r.pipeline() as pipe:
            for record in records:
                pipe.hset(key_name, record[field_col], record[value_col])
            pipe.execute()
        logging.info(f"write_to_redis_generic({key_name}): Escrita concluída com sucesso.")
    except Exception as e:
        logging.error(f"write_to_redis_generic({key_name}): Falha ao escrever para o Redis: {e}", exc_info=True)

def write_score_distribution(df, epoch_id):
    logging.info(f"Iniciando batch para 'score_distribution' (Epoch ID: {epoch_id})")
    write_to_redis_generic(df, epoch_id, "analysis:score_distribution", "FaixaScore", "count")

def write_ticket_medio(df, epoch_id):
    logging.info(f"Iniciando batch para 'ticket_medio' (Epoch ID: {epoch_id})")
    try:
        if df.rdd.isEmpty():
            logging.info("write_ticket_medio: DataFrame está vazio.")
            return

        r = get_redis_connection()
        last_record = df.orderBy(col("window.end").desc()).first()
        ticket_medio = last_record["TicketMedio"]
        
        logging.info(f"write_ticket_medio: Escrevendo no Redis -> Chave: analysis:ticket_medio, Valor: {ticket_medio}")
        r.set("analysis:ticket_medio", ticket_medio)
        logging.info("write_ticket_medio: Escrita concluída.")
    except Exception as e:
        logging.error(f"write_ticket_medio: Falha ao escrever para o Redis: {e}", exc_info=True)

def write_volume_by_currency(df, epoch_id):
    logging.info(f"Iniciando batch para 'volume_by_currency' (Epoch ID: {epoch_id})")
    try:
        if df.rdd.isEmpty():
            logging.info("write_volume_by_currency: DataFrame está vazio.")
            return

        r = get_redis_connection()
        records = df.toJSON().map(lambda x: json.loads(x)).collect()
        
        logging.info(f"write_volume_by_currency: Escrevendo {len(records)} registros.")
        with r.pipeline() as pipe:
            for record in records:
                pipe.hset("analysis:volume_by_currency", record["Moeda"], json.dumps({"VolumeTotal": record["VolumeTotal"], "Quantidade": record["Quantidade"]}))
            pipe.execute()
        logging.info("write_volume_by_currency: Escrita concluída.")
    except Exception as e:
        logging.error(f"write_volume_by_currency: Falha ao escrever para o Redis: {e}", exc_info=True)

def write_top_10_clientes(df, epoch_id):
    logging.info(f"Iniciando batch para 'top_10_clientes' (Epoch ID: {epoch_id})")
    try:
        if df.rdd.isEmpty():
            logging.info("write_top_10_clientes: DataFrame está vazio.")
            return

        r = get_redis_connection()
        records = df.orderBy(desc("TotalGasto")).toJSON().map(lambda x: json.loads(x)).collect()
        
        logging.info(f"write_top_10_clientes: Escrevendo {len(records)} clientes no top 10.")
        r.delete("analysis:top_10_clientes") 
        with r.pipeline() as pipe:
            for record in records:
                pipe.rpush("analysis:top_10_clientes", json.dumps({"ClienteID": record["ClienteID"], "TotalGasto": record["TotalGasto"]}))
            pipe.execute()
        logging.info("write_top_10_clientes: Escrita concluída.")
    except Exception as e:
        logging.error(f"write_top_10_clientes: Falha ao escrever para o Redis: {e}", exc_info=True)

def write_age_distribution(df, epoch_id):
    logging.info(f"Iniciando batch para 'age_distribution' (Epoch ID: {epoch_id})")
    write_to_redis_generic(df, epoch_id, "analysis:age_distribution", "FaixaEtaria", "count")

def write_risk_alerts(df, epoch_id):
    logging.info(f"Iniciando batch para 'risk_alerts' (Epoch ID: {epoch_id})")
    try:
        if df.rdd.isEmpty():
            logging.info("write_risk_alerts: DataFrame está vazio.")
            return
        
        r = get_redis_connection()
        alerts = df.toJSON().map(lambda x: json.loads(x)).collect()
        
        logging.info(f"write_risk_alerts: Adicionando {len(alerts)} alertas de risco.")
        with r.pipeline() as pipe:
            for alert in alerts:
                pipe.sadd("analysis:risk_alerts", json.dumps(alert))
            pipe.execute()
        logging.info("write_risk_alerts: Escrita concluída.")
    except Exception as e:
        logging.error(f"write_risk_alerts: Falha ao escrever para o Redis: {e}", exc_info=True)

def process_streams(spark, schemas):
    """Lê dos tópicos Kafka e processa as análises, salvando no Redis e RDS."""
    transaction_schema, score_schema, client_schema = schemas
    
    # -------- Leitura dos Tópicos Kafka --------
    trans_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("subscribe", "transacoes").load()
    score_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("subscribe", "scores").load()
    client_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS).option("subscribe", "clientes").load()

    # -------- Parse e Preparação dos Dados --------
    parsed_trans_df = trans_df.select(from_json(col("value").cast("string"), transaction_schema).alias("data")).select("data.*") \
        .withColumn("timestamp", to_timestamp(col("Data")))

    parsed_score_df = score_df.select(from_json(col("value").cast("string"), score_schema).alias("data")).select("data.*") \
        .withColumn("timestamp", to_timestamp(col("AtualizadoEm")))

    parsed_client_df = client_df.select(from_json(col("value").cast("string"), client_schema).alias("data")).select("data.*") \
        .withColumn("timestamp", current_timestamp()) \
        .withColumn("DataNasc", to_date(col("DataNasc")))
    
    # 1. Distribuição de Clientes por Faixa de Score
    score_distribution = parsed_score_df.withColumn("FaixaScore",
        when(col("Score") < 300, "Baixo")
        .when((col("Score") >= 300) & (col("Score") < 700), "Médio")
        .when(col("Score") >= 700, "Alto")
        .otherwise("Excelente")
    ).groupBy("FaixaScore").count().orderBy("FaixaScore")

    # 2. Ticket Médio por Transação (janela de 10 minutos)
    ticket_medio = parsed_trans_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(window("timestamp", "10 minutes", "5 minutes")) \
        .agg(avg("Valor").alias("TicketMedio"))

    # 3. Volume de Transações por Moeda
    transactions_by_currency = parsed_trans_df.groupBy("Moeda") \
        .agg(_sum("Valor").alias("VolumeTotal"), count("ID").alias("Quantidade"))

    # 4. Top 10 Clientes por Valor Total Gasto (última hora)
    top_clientes = parsed_trans_df \
        .withWatermark("timestamp", "1 hour") \
        .groupBy(window("timestamp", "1 hour"), "ClienteID") \
        .agg(_sum("Valor").alias("TotalGasto")) \
        .select("window", "ClienteID", "TotalGasto") \
        .orderBy(desc("TotalGasto")).limit(10)

    # 5. Distribuição de Clientes por Faixa Etária
    faixa_etaria = parsed_client_df \
        .withColumn("Idade", (year(current_timestamp()) - year(col("DataNasc")))) \
        .withColumn("FaixaEtaria",
            when(col("Idade") < 25, "18-24")
            .when((col("Idade") >= 25) & (col("Idade") < 35), "25-34")
            .when((col("Idade") >= 35) & (col("Idade") < 45), "35-44")
            .when((col("Idade") >= 45) & (col("Idade") < 55), "45-54")
            .otherwise("55+")
        ).groupBy("FaixaEtaria").count().orderBy("FaixaEtaria")

    # 6. Alertas de Risco (Score baixo e limite de crédito alto)
    alertas_risco = parsed_score_df \
        .filter((col("Score") < 400) & (col("LimiteCredito") > 15000)) \
        .select("CPF", "Score", "LimiteCredito")

    # 7. Evolução do Volume de Transações (diário) -> RDS
    evolucao_volume_diario = parsed_trans_df \
        .withWatermark("timestamp", "1 day") \
        .groupBy(window("timestamp", "1 day")) \
        .agg(_sum("Valor").alias("total_volume")) \
        .withColumn("date", col("window.start").cast(DateType())) \
        .withColumn("total_volume", col("total_volume").cast(DoubleType())) \
        .select("date", "total_volume")

    # 8. Taxa de Aquisição de Novos Clientes (diário) -> RDS
    novos_clientes_diario = parsed_client_df \
        .withWatermark("timestamp", "1 day") \
        .groupBy(window("timestamp", "1 day")) \
        .agg(count("ID").alias("new_clients_count")) \
        .withColumn("date", col("window.start").cast(DateType())) \
        .withColumn("new_clients_count", col("new_clients_count").cast(IntegerType())) \
        .select("date", "new_clients_count")

    # 9. Tendência do Score Médio dos Clientes (diário) -> RDS
    score_medio_diario = parsed_score_df \
        .withWatermark("timestamp", "1 day") \
        .groupBy(window("timestamp", "1 day")) \
        .agg(avg("Score").alias("average_score")) \
        .withColumn("date", col("window.start").cast(DateType())) \
        .withColumn("average_score", col("average_score").cast(DoubleType())) \
        .select("date", "average_score")

    # -------- Queries de Saída para o Redis (Tempo Real) --------
    score_distribution.writeStream.outputMode("complete").foreachBatch(write_score_distribution).start()
    ticket_medio.writeStream.outputMode("update").foreachBatch(write_ticket_medio).start()
    transactions_by_currency.writeStream.outputMode("complete").foreachBatch(write_volume_by_currency).start()
    top_clientes.writeStream.outputMode("complete").foreachBatch(write_top_10_clientes).start()
    faixa_etaria.writeStream.outputMode("complete").foreachBatch(write_age_distribution).start()
    alertas_risco.writeStream.outputMode("append").foreachBatch(write_risk_alerts).start()
    
    # -------- Queries de Saída para o RDS (Histórico) --------
    if DB_HOST:
        logging.info("Configurando streams de escrita para o RDS...")
        evolucao_volume_diario.writeStream \
            .outputMode("update") \
            .foreachBatch(lambda df, epoch_id: write_to_rds(spark, df, epoch_id, "daily_transaction_volume", "date", "total_volume")) \
            .start()

        novos_clientes_diario.writeStream \
            .outputMode("update") \
            .foreachBatch(lambda df, epoch_id: write_to_rds(spark, df, epoch_id, "daily_new_clients", "date", "new_clients_count")) \
            .start()
            
        score_medio_diario.writeStream \
            .outputMode("update") \
            .foreachBatch(lambda df, epoch_id: write_to_rds(spark, df, epoch_id, "daily_average_score", "date", "average_score")) \
            .start()
    else:
        logging.warning("Variáveis de ambiente do RDS não configuradas. As streams históricas não serão iniciadas.")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logging.info("Definindo schemas...")
    schemas = define_schemas()
    
    logging.info("Iniciando processamento dos streams...")
    process_streams(spark, schemas)
    
    logging.info("Streaming queries iniciadas, enviando dados para o Redis e RDS. Pressione Ctrl+C para parar.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main() 