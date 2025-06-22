import time
import boto3
import botocore
import psycopg2
import pandas as pd
from datetime import datetime
from boto3.session import Session


REGION = "us-east-1"
CLUSTER = "lab-cluster-ce"
SERVICE = "generator-task-def-service-8iyq3e6b"
JOB_NAME = "pyspark-kafka-consumer"
WAIT_SECS = 10
PG_CONN = (
    "host=database-projeto-ce.c2uj2bn9auip.us-east-1.rds.amazonaws.com "
    "dbname=postgres user=postgres password=ComputacaoEscalavel_6_"
)



ecs   = session.client("ecs", region_name=REGION)
glue  = session.client("glue", region_name=REGION)
logs  = session.client("logs", region_name=REGION)
rds   = psycopg2.connect(PG_CONN)


with rds.cursor() as cur:
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'daily_transaction_volume'
          AND data_type IN ('timestamp without time zone', 'timestamp with time zone')
    """)
    timestamp_cols = [row[0] for row in cur.fetchall()]

timestamp_col = timestamp_cols[0] if timestamp_cols else None
if not timestamp_col:
    print("[⚠] Nenhuma coluna TIMESTAMP encontrada na tabela 'daily_transaction_volume'. Métrica de registros recentes será pulada.")


def safe_start(glue_client, job_name, args, max_retries=5):
    backoff = 10
    for attempt in range(1, max_retries + 1):
        try:
            return glue_client.start_job_run(JobName=job_name, Arguments=args)
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "ConcurrentRunsExceededException":
                print(f"[ATENÇÃO] Run ativa. Retry em {backoff}s (tentativa {attempt}/{max_retries})")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise
    raise RuntimeError("Não foi possível iniciar o Glue Job após múltiplas tentativas")


replicas_list = [5]
results = []

print("\n[GLUE] Iniciando Glue Job Streaming com configuração inicial...")
response = safe_start(glue, JOB_NAME, {
    "--NumberOfWorkers": "2",
    "--WorkerType": "G.1X"
})
job_run_id = response["JobRunId"]
print(f"[GLUE] Job iniciado: {job_run_id}")

for count in replicas_list:
    print(f"\n[ESCALANDO ECS PARA {count} RÉPLICAS]")
    ecs.update_service(cluster=CLUSTER, service=SERVICE, desiredCount=count)

    glue_workers = min(count * 2, 10)
    print(f"[GLUE] (Simulado) Ajuste desejado: {glue_workers} workers")

    print(f"Aguardando {WAIT_SECS} segundos para estabilização...")
    t0 = time.time()
    time.sleep(WAIT_SECS)

    with rds.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM daily_transaction_volume")
        total_rds = cur.fetchone()[0]

        if timestamp_col:
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM daily_transaction_volume 
                WHERE {timestamp_col} >= NOW() - interval '2 minutes'
            """)
            recent_rows = cur.fetchone()[0]
        else:
            recent_rows = None

    t1 = time.time()
    latency_secs = round(t1 - t0, 2)

    results.append({
        "replicas": count,
        "glue_workers": glue_workers,
        "rds_rows": total_rds,
        "recent_rds_last_2min": recent_rows,
        "latency_secs": latency_secs,
        "timestamp": datetime.utcnow().isoformat()
    })

print("\n[FINALIZANDO ECS]")
ecs.update_service(cluster=CLUSTER, service=SERVICE, desiredCount=0)

status = glue.get_job_run(JobName=JOB_NAME, RunId=job_run_id)["JobRun"]["JobRunState"]
print(f"[GLUE] Status final do job: {status}")


def calculate_log_latency(log_group, job_run_id):
    
    streams = logs.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=job_run_id
    )["logStreams"]
    if not streams:
        print(f"[⚠] Nenhum log stream encontrado em {log_group} para {job_run_id}")
        return None

    min_ts, max_ts = None, None
    for s in streams:
        events = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=s["logStreamName"],
            startFromHead=True
        )["events"]
        for ev in events:
            ts = ev["timestamp"]
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

    if min_ts and max_ts:
        
        return ((max_ts - min_ts) / 1000.0)-10
    return None

for lg in ["/aws-glue/jobs/output", "/aws-glue/jobs/error", "/aws-glue/jobs/logs-v2"]:
    lat = calculate_log_latency(lg, job_run_id)
    if lat is not None:
        print(f"Latência em {lg}: {lat:.2f}s")


df = pd.DataFrame(results)
df.to_csv("carga_resultados.csv", index=False)
print("\n[✔] Teste de carga finalizado. Resultados salvos em 'carga_resultados.csv'")
print(df)
