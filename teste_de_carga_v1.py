import time
import boto3
import botocore
import psycopg2
import pandas as pd
from datetime import datetime
from boto3.session import Session


REGION       = "us-east-1"
CLUSTER      = "lab-cluster-ce"
SERVICE      = "generator-task-def-service-8iyq3e6b"
JOB_NAME     = "pyspark-kafka-consumer"
WAIT_SECS    = 10
PG_CONN      = (
    "host=database-projeto-ce.c2uj2bn9auip.us-east-1.rds.amazonaws.com "
    "dbname=postgres user=postgres password=ComputacaoEscalavel_6_"
)

print("[INIT] Autenticando AWS session e conectando RDS...")
session = Session(
    aws_access_key_id="ASIARTTKLU4PU4KCZ66T",
    aws_secret_access_key="Ar3DOTN/VpbZHbFJ1Lo+e6QUnbv9m1AV1VEYg+uj",
    aws_session_token="IQoJb3JpZ2luX2VjEPb..."
)
ecs  = session.client("ecs",   region_name=REGION)
glue = session.client("glue",  region_name=REGION)
logs = session.client("logs",  region_name=REGION)
rds  = psycopg2.connect(PG_CONN)
print("[INIT] OK.")


print("[CHECK] Procurando coluna TIMESTAMP em daily_transaction_volume...")
with rds.cursor() as cur:
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'daily_transaction_volume'
          AND data_type IN (
            'timestamp without time zone',
            'timestamp with time zone'
          )
    """)
    cols = [row[0] for row in cur.fetchall()]
timestamp_col = cols[0] if cols else None
print(f"[CHECK] Coluna timestamp: {timestamp_col}")


def safe_start(glue_client, job_name, args, max_retries=5):
    backoff = 10
    for attempt in range(1, max_retries+1):
        print(f"[GLUE] Tentar iniciar run (tentativa {attempt})...")
        try:
            resp = glue_client.start_job_run(JobName=job_name, Arguments=args)
            run_id = resp["JobRunId"]
            print(f"[GLUE] Job iniciado: {run_id}")
            return run_id
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConcurrentRunsExceededException":
                print(f"[GLUE] ConcurrentRunsExceeded → retry em {backoff}s")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise
    raise RuntimeError("Não foi possível iniciar Glue Job após várias tentativas")


def wait_stopped(run_id, timeout=120):
    start = time.time()
    while True:
        state = glue.get_job_run(JobName=JOB_NAME, RunId=run_id)["JobRun"]["JobRunState"]
        print(f"[GLUE] run {run_id} estado: {state}")
        if state == "STOPPED":
            print(f"[GLUE] run {run_id} parado.")
            return
        if time.time() - start > timeout:
            print(f"[GLUE] Timeout de {timeout}s aguardando STOPPED.")
            return
        time.sleep(5)


def calc_latency(log_group, prefix):
    print(f"[CW] fetch logs em {log_group} (prefix {prefix})")
    streams = logs.describe_log_streams(logGroupName=log_group, logStreamNamePrefix=prefix)["logStreams"]
    if not streams:
        print(f"[CW] nenhum stream em {log_group}")
        return None
    ts_min = ts_max = None
    for s in streams:
        events = logs.get_log_events(logGroupName=log_group, logStreamName=s["logStreamName"], startFromHead=True)["events"]
        for ev in events:
            ts = ev["timestamp"]
            ts_min = ts if ts_min is None or ts < ts_min else ts_min
            ts_max = ts if ts_max is None or ts > ts_max else ts_max
    latency = (ts_max - ts_min)/1000 if ts_min and ts_max else None
    print(f"[CW] latência em {log_group}: {latency}s")
    return latency


replicas_list = [1, 2, 4, 6, 8]
results = []

for count in replicas_list:
    print(f"\n[ITER] {count} réplica(s)")

    
    runs = glue.get_job_runs(JobName=JOB_NAME, MaxResults=10)["JobRuns"]
    to_stop = [r["Id"] for r in runs if r["JobRunState"] in ("RUNNING","STARTING","STOPPING")]
    if to_stop:
        print(f"[GLUE] stoppando runs: {to_stop}")
        glue.batch_stop_job_run(JobName=JOB_NAME, JobRunIds=to_stop)
        for rid in to_stop:
            wait_stopped(rid)
    else:
        print("[GLUE] sem runs ativas")

    
    print(f"[ECS] desiredCount → {count}")
    ecs.update_service(cluster=CLUSTER, service=SERVICE, desiredCount=count)

    
    workers = min(count*2, 10)
    run_id = safe_start(glue, JOB_NAME, {"--NumberOfWorkers": str(workers), "--WorkerType": "G.1X"})

    
    print(f"[WAIT] {WAIT_SECS}s")
    t0 = time.time()
    time.sleep(WAIT_SECS)
    t1 = time.time()
    print(f"[WAIT] {round(t1-t0,2)}s decorridos")

    
    with rds.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM daily_transaction_volume")
        total = cur.fetchone()[0]
        recent = None
        if timestamp_col:
            cur.execute(f"SELECT COUNT(*) FROM daily_transaction_volume WHERE {timestamp_col} >= NOW()-interval '2 minutes'")
            recent = cur.fetchone()[0]
    print(f"[RDS] total={total}, recent_2min={recent}")

    
    print(f"[GLUE] stopping run {run_id}")
    glue.batch_stop_job_run(JobName=JOB_NAME, JobRunIds=[run_id])
    wait_stopped(run_id)

    
    lat_o = calc_latency("/aws-glue/jobs/output", run_id)
    lat_e = calc_latency("/aws-glue/jobs/error",  run_id)
    lat_l = calc_latency("/aws-glue/jobs/logs-v2", run_id)
    lat_g = calc_latency("/ecs/generator-task-def", run_id)

    
    results.append({
        "replicas":             count,
        "glue_workers":         workers,
        "rds_rows":             total,
        "recent_rds_2min":      recent,
        "sleep_latency":        round(t1-t0,2),
        "job_run_id":           run_id,
        "output_cw":            lat_o,
        "error_cw":             lat_e,
        "logs-v2_cw":           lat_l,
        "generator-task-def_cw":lat_g
    })


print("\n[ECS] reset desiredCount → 0")
ecs.update_service(cluster=CLUSTER, service=SERVICE, desiredCount=0)


print("[RESULT] salvando resultados em carga_resultados.csv")
df = pd.DataFrame(results)
df.to_csv("carga_resultados.csv", index=False)
print(df)
