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
# 1. CONFIGURAÇÃO DE LOGS DETALHADOS
# ------------------------------------------------------------------
logger = logging.getLogger("GlueKafkaToPostgres")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# ------------------------------------------------------------------
# 2. DEFINIÇÃO DOS SCHEMAS
# ------------------------------------------------------------------
def define_schemas():
    """Define os schemas para os dados do Kafka."""
    logger.info("Definindo os schemas para os tópicos do Kafka.")
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
    """Escreve um DataFrame em uma tabela usando TRUNCATE e INSERT."""
    logger.info(f"TRUNCATE/WRITE: Iniciando batch para a tabela '{table_name}' (Epoch ID: {epoch_id}).")
    
    # A URL e as propriedades JDBC são construídas com os argumentos do Job
    db_url = f"jdbc:postgresql://{db_args['db_host']}:{db_args['db_port']}/{db_args['db_name']}"
    db_properties = {
        "user": db_args['db_user'],
        "password": db_args['db_password'],
        "driver": "org.postgresql.Driver"
    }
    
    if df.rdd.isEmpty():
        logger.warning(f"TRUNCATE/WRITE: DataFrame para '{table_name}' está vazio. A tabela será truncada mesmo assim.")
    
    try:
        df.write.format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", table_name) \
            .option("truncate", "true") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("driver", db_properties["driver"]) \
            .mode("overwrite") \
            .save()
        logger.info(f"TRUNCATE/WRITE: Batch para a tabela '{table_name}' escrito com sucesso.")
    except Exception as e:
        logger.error(f"TRUNCATE/WRITE: Erro ao escrever na tabela '{table_name}'. Erro: {e}")
        raise e

def write_postgres_upsert(df, epoch_id, table_name, pk_column, db_args):
    """Realiza um UPSERT (INSERT ON CONFLICT) usando psycopg2."""
    logger.info(f"UPSERT: Iniciando batch para '{table_name}' (Epoch ID: {epoch_id}).")
    
    data = df.collect()
    if not data:
        logger.info(f"UPSERT: DataFrame para '{table_name}' está vazio. Pulando.")
        return

    conn = None
    try:
        logger.info(f"UPSERT: Conectando ao banco de dados em {db_args['db_host']}...")
        conn = psycopg2.connect(
            host=db_args['db_host'],
            dbname=db_args['db_name'],
            user=db_args['db_user'],
            password=db_args['db_password'],
            port=db_args['db_port']
        )
        cursor = conn.cursor()
        logger.info("UPSERT: Conexão com PostgreSQL estabelecida.")
        
        cols = df.columns
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        update_cols = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk_column])
        
        sql = f"""
            INSERT INTO {table_name} ({cols_sql}) VALUES %s
            ON CONFLICT ({pk_column}) DO UPDATE SET {update_cols};
        """
        
        values = [tuple(row) for row in data]
        execute_values(cursor, sql, values)
        conn.commit()
        logger.info(f"UPSERT: {len(data)} linhas escritas com sucesso em '{table_name}'.")

    except Exception as e:
        logger.error(f"UPSERT: Erro ao escrever na tabela '{table_name}': {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()
            logger.info("UPSERT: Conexão com PostgreSQL fechada.")


def write_postgres_increment(df, epoch_id, table_name, pk_column, increment_cols, db_args):
    """Realiza um UPSERT que INCREMENTA os valores de colunas especificadas."""
    logger.info(f"INCREMENT: Iniciando batch para '{table_name}' (Epoch ID: {epoch_id}).")
    
    data = df.collect()
    if not data:
        logger.info(f"INCREMENT: DataFrame para '{table_name}' está vazio. Pulando.")
        return

    conn = None
    try:
        logger.info(f"INCREMENT: Conectando ao banco de dados em {db_args['db_host']}...")
        conn = psycopg2.connect(
            host=db_args['db_host'],
            dbname=db_args['db_name'],
            user=db_args['db_user'],
            password=db_args['db_password'],
            port=db_args['db_port']
        )
        cursor = conn.cursor()
        logger.info("INCREMENT: Conexão com PostgreSQL estabelecida.")

        all_cols = df.columns
        all_cols_sql = ", ".join([f'"{c}"' for c in all_cols])
        update_clause = ", ".join([f'"{c}" = {table_name}."{c}" + EXCLUDED."{c}"' for c in increment_cols])
        
        # Lógica para colunas que não são de incremento (caso existam)
        non_increment_cols = [c for c in all_cols if c not in increment_cols and c != pk_column]
        if non_increment_cols:
             update_clause += ", " + ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_increment_cols])
        
        sql = f"""
            INSERT INTO {table_name} ({all_cols_sql}) VALUES %s
            ON CONFLICT ({pk_column}) DO UPDATE SET {update_clause};
        """
        
        values = [tuple(row) for row in data]
        execute_values(cursor, sql, values)
        conn.commit()
        logger.info(f"INCREMENT: {len(data)} linhas escritas com sucesso em '{table_name}'.")

    except Exception as e:
        logger.error(f"INCREMENT: Erro ao escrever na tabela '{table_name}': {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()
            logger.info("INCREMENT: Conexão com PostgreSQL fechada.")

def process_and_write_top_10(df, epoch_id, db_args):
    """Aplica o ranking no micro-batch e escreve o resultado usando TRUNCATE/WRITE."""
    logger.info(f"TOP 10: Processando batch (Epoch ID: {epoch_id}).")
    windowSpec = Window.orderBy(desc("TotalGasto"))
    top_10_df = df.withColumn("rank", row_number().over(windowSpec)) \
                  .filter(col("rank") <= 10) \
                  .select(col("rank"), col("ClienteID").alias("cliente_id"), col("TotalGasto").alias("total_gasto"))
    
    write_truncate(top_10_df, epoch_id, "analysis_top_10_clientes", db_args)

# ------------------------------------------------------------------
# 4. SCRIPT PRINCIPAL DO JOB GLUE
# ------------------------------------------------------------------
logger.info("Iniciando o Job Glue de streaming Kafka -> Postgres.")

# Obtenção dos parâmetros do Job
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
    'checkpoint_s3_path' # Caminho base para os checkpoints no S3
])

# Inicialização do GlueContext e SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.sparkContext.setLogLevel("ERROR") # Reduz a verbosidade do Spark, mantendo nossos logs

# Definição dos Schemas
transaction_schema, score_schema, client_schema = define_schemas()

# Função para ler um tópico Kafka
def read_kafka_topic(topic_name, connection_name):
    logger.info(f"Configurando stream para o tópico '{topic_name}' em '{args['kafka_bootstrap_servers']}'.")
    kafka_options = {
        "connection.name": connection_name,
        "connection.type": "kafka",
        "kafka.bootstrap.servers": args['kafka_bootstrap_servers'],
        "subscribe": topic_name,
        "startingOffsets": "earliest" # ou "latest"
    }
    try:
        stream_dyf = glueContext.create_data_frame_from_options(
            connection_options=kafka_options,
            transformation_ctx=connection_name
        )
        logger.info(f"Stream para '{topic_name}' configurado com sucesso.")
        return stream_dyf.toDF()
    except Exception as e:
        logger.error(f"Falha CRÍTICA ao configurar o stream para o tópico '{topic_name}'. Erro: {e}")
        raise e

# Leitura dos Tópicos Kafka
trans_df = read_kafka_topic(args['kafka_topic_transacoes'], "transacoes_stream")
score_df = read_kafka_topic(args['kafka_topic_scores'], "scores_stream")
client_df = read_kafka_topic(args['kafka_topic_clientes'], "clientes_stream")

# Parse e Preparação dos Dados
logger.info("Aplicando parsing e transformações iniciais nos DataFrames.")
parsed_trans_df = trans_df.select(from_json(col("value").cast("string"), transaction_schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("Data")))
parsed_score_df = score_df.select(from_json(col("value").cast("string"), score_schema).alias("data")).select("data.*").withColumn("timestamp", to_timestamp(col("AtualizadoEm")))
parsed_client_df = client_df.select(from_json(col("value").cast("string"), client_schema).alias("data")).select("data.*").withColumn("timestamp", current_timestamp()).withColumn("DataNasc", to_date(col("DataNasc"), "yyyy-MM-dd"))

# --- Definição e Escrita das Análises ---

# 1. Distribuição por Score (INCREMENT)
logger.info("Configurando stream de análise: Distribuição por Score.")
score_distribution = parsed_score_df.groupBy("Score").agg(count("*").alias("count")) \
    .withColumn("faixa_score",
        when(col("Score") < 300, "Baixo")
        .when((col("Score") >= 300) & (col("Score") < 700), "Médio")
        .when(col("Score") >= 700, "Alto")
        .otherwise("Excelente")
    ).groupBy("faixa_score").agg(_sum("count").alias("count"))

score_distribution.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch: write_postgres_increment(df, epoch, "analysis_score_distribution", "faixa_score", ["count"], args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/score_distribution/") \
    .start()

# 2. Ticket Médio (UPSERT)
logger.info("Configurando stream de análise: Ticket Médio.")
ticket_medio = parsed_trans_df.agg(coalesce(avg("Valor"), lit(0.0)).alias("ticket_medio")).withColumn("id", lit("singleton"))

ticket_medio.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, "analysis_ticket_medio", "id", args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/ticket_medio/") \
    .start()
    
# 3. Volume por Moeda (INCREMENT)
logger.info("Configurando stream de análise: Volume por Moeda.")
volume_by_currency = parsed_trans_df.groupBy("Moeda").agg(
    _sum("Valor").alias("volume_total"),
    count("ID").alias("quantidade")
).withColumnRenamed("Moeda", "moeda")

volume_by_currency.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch: write_postgres_increment(df, epoch, "analysis_volume_by_currency", "moeda", ["volume_total", "quantidade"], args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/volume_by_currency/") \
    .start()

# 4. Top 10 Clientes (TRUNCATE)
logger.info("Configurando stream de análise: Top 10 Clientes.")
cliente_gastos = parsed_trans_df.groupBy("ClienteID").agg(_sum("Valor").alias("TotalGasto"))

cliente_gastos.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch: process_and_write_top_10(df, epoch, args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/top_10_clientes/") \
    .start()

# 5. Distribuição por Idade (INCREMENT)
logger.info("Configurando stream de análise: Distribuição por Idade.")
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

age_distribution.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch: write_postgres_increment(df, epoch, "analysis_age_distribution", "faixa_etaria", ["count"], args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/age_distribution/") \
    .start()

# 6. Alertas de Risco (TRUNCATE)
logger.info("Configurando stream de análise: Alertas de Risco.")
risk_alerts = parsed_score_df \
    .filter((col("Score") < 400) & (col("LimiteCredito") > 15000)) \
    .select(
        col("CPF").alias("cpf"),
        col("Score").alias("score"),
        col("LimiteCredito").alias("limite_credito")
    ).distinct()

risk_alerts.writeStream \
    .outputMode("complete") \
    .foreachBatch(lambda df, epoch: write_truncate(df, epoch, "analysis_risk_alerts", args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/risk_alerts/") \
    .start()

# 7. Análises com Janela de Tempo e Watermark (UPSERT)
logger.info("Configurando streams de análise com janela de tempo (Diárias).")
daily_volume = parsed_trans_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "1 day")).agg(_sum("Valor").alias("total_volume")) \
    .select(col("window.start").cast(DateType()).alias("date"), "total_volume")

daily_new_clients = parsed_client_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "1 day")).agg(count("ID").alias("new_clients_count")) \
    .select(col("window.start").cast(DateType()).alias("date"), "new_clients_count")

daily_average_score = parsed_score_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "1 day")).agg(avg("Score").alias("average_score")) \
    .select(col("window.start").cast(DateType()).alias("date"), "average_score")

daily_volume.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, "daily_transaction_volume", "date", args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/daily_volume/") \
    .start()

daily_new_clients.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, "daily_new_clients", "date", args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/daily_new_clients/") \
    .start()

daily_average_score.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch: write_postgres_upsert(df, epoch, "daily_average_score", "date", args)) \
    .option("checkpointLocation", f"{args['checkpoint_s3_path']}/daily_average_score/") \
    .start()

# Aguarda a finalização de qualquer um dos streams para encerrar o job (o Glue gerencia isso)
logger.info("Todos os streams foram configurados. O Job Glue irá aguardar a terminação.")
spark.streams.awaitAnyTermination()

job.commit()