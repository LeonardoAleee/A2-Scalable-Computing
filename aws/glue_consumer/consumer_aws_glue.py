import sys
import logging
import psycopg2
from psycopg2.extras import execute_values
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import coalesce, from_json, col, sum as _sum, when, count, avg, window, to_date, current_timestamp, desc, year, to_timestamp, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.window import Window

# ------------------------------------------------------------------
# 1. CONFIGURAÇÃO DE LOGS
# ------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', stream=sys.stdout)

# ------------------------------------------------------------------
# 2. DEFINIÇÃO DOS SCHEMAS
# ------------------------------------------------------------------
def define_schemas():
    """Define os schemas para os dados do Kafka."""
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

# ------------------------------------------------------------------
# 3. FUNÇÕES DE ESCRITA NO POSTGRESQL
# ------------------------------------------------------------------
def write_truncate(df, epoch_id, table_name, db_args):
    db_url = f"jdbc:postgresql://{db_args['db_host']}:{db_args['db_port']}/{db_args['db_name']}"
    db_properties = { "user": db_args['db_user'], "password": db_args['db_password'], "driver": "org.postgresql.Driver" }
    if df.rdd.isEmpty():
        return
    try:
        df.write.mode("overwrite").format("jdbc").option("url", db_url).option("dbtable", table_name).option("truncate", "true").option("user", db_properties["user"]).option("password", db_properties["password"]).option("driver", db_properties["driver"]).save()
    except Exception as e:
        raise e

def write_postgres_upsert(df, epoch_id, table_name, pk_column, db_args):
    data = df.collect()
    if not data:
        return
    conn = None
    try:
        conn = psycopg2.connect(host=db_args['db_host'], dbname=db_args['db_name'], user=db_args['db_user'], password=db_args['db_password'], port=db_args['db_port'])
        cursor = conn.cursor()
        cols = df.columns
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        update_cols = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk_column])
        sql = f"""INSERT INTO {table_name} ({cols_sql}) VALUES %s ON CONFLICT ({pk_column}) DO UPDATE SET {update_cols};"""
        values = [tuple(row) for row in data]
        execute_values(cursor, sql, values)
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()

def write_postgres_increment(df, epoch_id, table_name, pk_column, increment_cols, db_args):
    data = df.collect()
    if not data:
        return
    conn = None
    try:
        conn = psycopg2.connect(host=db_args['db_host'], dbname=db_args['db_name'], user=db_args['db_user'], password=db_args['db_password'], port=db_args['db_port'])
        cursor = conn.cursor()
        all_cols = df.columns
        all_cols_sql = ", ".join([f'"{c}"' for c in all_cols])
        
        update_clause = ", ".join([f'"{c}" = {table_name}."{c}" + EXCLUDED."{c}"' for c in increment_cols])
        
        other_cols = [c for c in all_cols if c not in increment_cols and c != pk_column]
        if other_cols:
            update_clause += ", " + ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in other_cols])
        
        sql = f"""INSERT INTO {table_name} ({all_cols_sql}) VALUES %s ON CONFLICT ({pk_column}) DO UPDATE SET {update_clause};"""
        
        values = [tuple(row) for row in data]
        execute_values(cursor, sql, values)
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        raise e
    finally:
        if conn: conn.close()

# --- FUNÇÃO DE LOG DE CONSUMO ---
def log_message_consumption(df, epoch_id):
    """Loga o ID e o timestamp de consumo de cada mensagem da transação."""
    if df.rdd.isEmpty():
        return
    
    consumed_df = df.withColumn("consume_timestamp", current_timestamp())
    
    for row in consumed_df.select("ID", "consume_timestamp").collect():
        logging.info(f"CONSUME_MSG_LOG: id={row['ID']}, timestamp={row['consume_timestamp'].isoformat()}")

# --- FUNÇÕES DE PROCESSAMENTO DE MICRO-LOTE ---
def process_transactions_batch(df, epoch_id, db_args):
    if df.count() == 0:
        return
    df.cache()

    ticket_medio = df.agg(coalesce(avg("Valor"), lit(0.0)).alias("ticket_medio")).withColumn("id", lit("singleton"))
    write_postgres_upsert(ticket_medio, epoch_id, "analysis_ticket_medio", "id", db_args)

    volume_by_currency = df.groupBy("Moeda").agg(_sum("Valor").alias("volume_total"), count("ID").alias("quantidade")).withColumnRenamed("Moeda", "moeda")
    write_postgres_increment(volume_by_currency, epoch_id, "analysis_volume_by_currency", "moeda", ["volume_total", "quantidade"], db_args)

    cliente_gastos = df.groupBy("ClienteID").agg(_sum("Valor").alias("TotalGasto"))
    windowSpec = Window.orderBy(desc("TotalGasto"))
    top_10_df = cliente_gastos.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") <= 10).select(col("rank"), col("ClienteID").alias("cliente_id"), col("TotalGasto").alias("total_gasto"))
    write_truncate(top_10_df, epoch_id, "analysis_top_10_clientes", db_args)

    daily_volume = df.groupBy(to_date(col("timestamp")).alias("date")).agg(_sum("Valor").alias("total_volume"))
    write_postgres_increment(daily_volume, epoch_id, "daily_transaction_volume", "date", ["total_volume"], db_args)
    
    log_message_consumption(df, epoch_id)

    df.unpersist()

def process_scores_batch(df, epoch_id, db_args):
    if df.count() == 0:
        return
    df.cache()

    score_distribution = df.withColumn("faixa_score", when(col("Score") < 300, "Baixo").when((col("Score") >= 300) & (col("Score") < 700), "Médio").when(col("Score") >= 700, "Alto").otherwise("Excelente")).groupBy("faixa_score").agg(count("*").alias("count"))
    write_postgres_increment(score_distribution, epoch_id, "analysis_score_distribution", "faixa_score", ["count"], db_args)

    risk_alerts = df.filter((col("Score") < 400) & (col("LimiteCredito") > 15000)) \
                    .select(col("CPF").alias("cpf"), col("Score").alias("score"), col("LimiteCredito").alias("limite_credito")) \
                    .dropDuplicates(["cpf"]) 
    write_postgres_upsert(risk_alerts, epoch_id, "analysis_risk_alerts", "cpf", db_args)
    
    daily_average_score = df.groupBy(to_date(col("timestamp")).alias("date")).agg(avg("Score").alias("average_score"))
    write_postgres_upsert(daily_average_score, epoch_id, "daily_average_score", "date", db_args)
    
    df.unpersist()

def process_clients_batch(df, epoch_id, db_args):
    if df.count() == 0:
        return
    df.cache()
    
    age_distribution = df.withColumn("Idade", (year(current_timestamp()) - year(col("DataNasc")))).withColumn("faixa_etaria", when(col("Idade") < 25, "18-24").when((col("Idade") >= 25) & (col("Idade") < 35), "25-34").when((col("Idade") >= 35) & (col("Idade") < 45), "35-44").when((col("Idade") >= 45) & (col("Idade") < 55), "45-54").when(col("Idade") >= 55, "55+").otherwise("N/A")).groupBy("faixa_etaria").agg(count("*").alias("count"))
    write_postgres_increment(age_distribution, epoch_id, "analysis_age_distribution", "faixa_etaria", ["count"], db_args)
    
    daily_new_clients = df.groupBy(to_date(col("timestamp")).alias("date")).agg(count("ID").alias("new_clients_count"))
    write_postgres_increment(daily_new_clients, epoch_id, "daily_new_clients", "date", ["new_clients_count"], db_args)
    
    df.unpersist()


# ------------------------------------------------------------------
# 4. SCRIPT PRINCIPAL DO JOB GLUE
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'kafka_bootstrap_servers',
    'kafka_topic_transacoes',
    'kafka_topic_scores',
    'kafka_topic_clientes',
    'db_host',
    'db_port',
    'db_name',
    'db_user',
    'db_password',
    'checkpoint_s3_path'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.sparkContext.setLogLevel("ERROR")

transaction_schema, score_schema, client_schema = define_schemas()

def read_kafka_topic_spark(spark_session, bootstrap_servers, topic_name):
    """Lê um tópico do Kafka usando o método spark.readStream."""
    try:
        df = spark_session.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .load()
        return df
    except Exception as e:
        raise e

bootstrap_servers = args['kafka_bootstrap_servers']
trans_df_raw = read_kafka_topic_spark(spark, bootstrap_servers, args['kafka_topic_transacoes'])
score_df_raw = read_kafka_topic_spark(spark, bootstrap_servers, args['kafka_topic_scores'])
client_df_raw = read_kafka_topic_spark(spark, bootstrap_servers, args['kafka_topic_clientes'])

parsed_trans_df = trans_df_raw.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), transaction_schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("Data")))
parsed_score_df = score_df_raw.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), score_schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("AtualizadoEm")))
parsed_client_df = client_df_raw.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), client_schema).alias("data")).select("data.*").withColumn("DataNasc", to_date(col("DataNasc"), "yyyy-MM-dd")).withColumn("timestamp", current_timestamp())

# --- ESTRUTURA FOREACHBATCH ---

query_transacoes = parsed_trans_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: process_transactions_batch(df, epoch_id, args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/transacoes/") \
    .start()

query_scores = parsed_score_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: process_scores_batch(df, epoch_id, args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/scores/") \
    .start()

query_clientes = parsed_client_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: process_clients_batch(df, epoch_id, args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/clientes/") \
    .start()


spark.streams.awaitAnyTermination()

job.commit()