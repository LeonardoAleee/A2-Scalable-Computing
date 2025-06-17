import streamlit as st
import redis
import pandas as pd
import json
import time
import plotly.express as px
import os
import psycopg2
import logging
import sys

# --- Configuração de Logging Robusta ---
# Cria um logger específico para o dashboard, imune a configurações do Streamlit
logger = logging.getLogger('dashboard_logger')
logger.setLevel(logging.INFO)
# Evita adicionar handlers duplicados a cada atualização do Streamlit
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - Dashboard - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Configuração da Página ---
st.set_page_config(
    page_title="Dashboard de Análise de Risco",
    page_icon="✅",
    layout="wide"
)

# --- Conexões com Serviços ---
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

@st.cache_resource
def get_redis_connection():
    logger.info(f"Tentando conectar ao Redis em: {REDIS_HOST}")
    try:
        conn = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        conn.ping() # Verifica se a conexão é válida
        logger.info("Conexão com Redis bem-sucedida.")
        return conn
    except Exception as e:
        logger.error(f"Falha ao conectar ao Redis: {e}", exc_info=True)
        st.error(f"Não foi possível conectar ao Redis em {REDIS_HOST}. Verifique as variáveis de ambiente e a conectividade de rede.")
        return None

@st.cache_resource
def get_db_connection():
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        return None
    try:
        logger.info(f"Tentando conectar ao RDS em: {DB_HOST}")
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.info("Conexão com RDS bem-sucedida.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Não foi possível conectar ao banco de dados: {e}", exc_info=True)
        st.error(f"Não foi possível conectar ao banco de dados: {e}")
        return None

r = get_redis_connection()
# Interrompe a execução se a conexão com o Redis (crítica) falhar.
if r is None:
    st.stop()
    
db_conn = get_db_connection()

# --- Funções para buscar dados ---

# Funções Redis
def get_hash_data(key_name):
    logger.info(f"Buscando dados HASH da chave: {key_name}")
    data = r.hgetall(key_name)
    logger.info(f"Encontrados {len(data)} itens para a chave {key_name}.")
    return data

def get_zset_data(key_name):
    logger.info(f"Buscando dados ZSET da chave: {key_name}")
    data = r.zrange(key_name, 0, -1, withscores=True)
    logger.info(f"Encontrados {len(data)} itens para a chave {key_name}.")
    return data

def get_list_data(key_name):
    logger.info(f"Buscando dados LIST da chave: {key_name}")
    data = r.lrange(key_name, 0, -1)
    logger.info(f"Encontrados {len(data)} itens para a chave {key_name}.")
    return data

def get_set_data(key_name):
    logger.info(f"Buscando dados SET da chave: {key_name}")
    data = r.smembers(key_name)
    logger.info(f"Encontrados {len(data)} itens para a chave {key_name}.")
    return data

# Funções RDS
@st.cache_data(ttl=60) # Cache por 60 segundos
def get_historical_data(_conn, query):
    if _conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Erro ao buscar dados do RDS: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_total_clients_from_db(_conn):
    logger.info("Buscando contagem total de clientes do RDS.")
    if _conn is None:
        logger.warning("Conexão com o banco de dados não disponível para buscar total de clientes.")
        return 0
    df = get_historical_data(_conn, "SELECT SUM(new_clients_count) as total FROM daily_new_clients;")
    if not df.empty and df['total'][0] is not None:
        return int(df['total'][0])
    logger.warning("Não foi possível obter a contagem total de clientes do RDS.")
    return 0

# --- Layout do Dashboard ---
st.title("🚀 Dashboard de Análise de Risco em Tempo Real e Histórico")
placeholder = st.empty()
iteration_counter = 0

# --- Loop de atualização ---
while True:
    with placeholder.container():
        logger.info(f"Atualizando dashboard... Iteração: {iteration_counter}")
        # --- Linha 1: Métricas Globais ---
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        # KPI 1: Volume Total por Moeda (exemplo com BRL)
        volume_currency = get_hash_data("analysis:volume_by_currency")
        total_brl = json.loads(volume_currency.get("BRL", "{}")).get("VolumeTotal", 0)
        kpi1.metric(label="Volume Total (BRL)", value=f"R$ {total_brl:,.2f}")

        # KPI 2: Ticket Médio (BRL)
        ticket_medio = float(r.get("analysis:ticket_medio") or 0)
        kpi2.metric(label="Ticket Médio (BRL)", value=f"R$ {ticket_medio:,.2f}")

        # KPI 3: Clientes em Alerta de Risco
        risk_alerts_count = len(get_set_data("analysis:risk_alerts"))
        kpi3.metric(label="Clientes em Alerta de Risco", value=risk_alerts_count)

        # KPI 4: Total de Clientes (do RDS)
        total_clients = get_total_clients_from_db(db_conn)
        kpi4.metric(label="Total de Clientes", value=total_clients)
        
        st.markdown("---")

        # --- Linha 2: Gráficos de Distribuição ---
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Distribuição de Clientes por Score")
            score_dist_data = get_hash_data("analysis:score_distribution")
            if score_dist_data:
                df_score = pd.DataFrame(score_dist_data.items(), columns=['Faixa de Score', 'Quantidade'])
                df_score['Quantidade'] = pd.to_numeric(df_score['Quantidade'])
                fig = px.bar(df_score, x='Faixa de Score', y='Quantidade', title="", text_auto=True)
                st.plotly_chart(fig, use_container_width=True, key=f"score_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de distribuição de score...")

        with col2:
            st.subheader("Distribuição de Clientes por Faixa Etária")
            age_dist_data = get_hash_data("analysis:age_distribution")
            if age_dist_data:
                df_age = pd.DataFrame(age_dist_data.items(), columns=['Faixa Etária', 'Quantidade'])
                df_age['Quantidade'] = pd.to_numeric(df_age['Quantidade'])
                fig = px.pie(df_age, names='Faixa Etária', values='Quantidade', title="")
                st.plotly_chart(fig, use_container_width=True, key=f"age_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de distribuição de idade...")
        
        st.markdown("---")
        
        # --- Linha 3: Gráficos de Série Temporal (Histórico do RDS) ---
        st.subheader("Análises Históricas (últimos 30 dias)")
        
        col3, col4, col5 = st.columns(3)
        
        with col3:
            st.markdown("###### Volume Diário de Transações")
            daily_volume_df = get_historical_data(db_conn, "SELECT date, total_volume FROM daily_transaction_volume WHERE date > (CURRENT_DATE - INTERVAL '30 days') AND date <= CURRENT_DATE ORDER BY date ASC;")
            if not daily_volume_df.empty:
                fig = px.bar(daily_volume_df, x='date', y='total_volume', labels={'date': 'Data', 'total_volume': 'Volume (R$)'})
                fig.update_layout(margin=dict(l=20, r=20, t=0, b=0), height=300)
                st.plotly_chart(fig, use_container_width=True, key=f"volume_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de volume diário...")

        with col4:
            st.markdown("###### Score Médio Diário")
            daily_score_df = get_historical_data(db_conn, "SELECT date, average_score FROM daily_average_score WHERE date > (CURRENT_DATE - INTERVAL '30 days') AND date <= CURRENT_DATE ORDER BY date ASC;")
            if not daily_score_df.empty:
                fig = px.line(daily_score_df, x='date', y='average_score', markers=True, labels={'date': 'Data', 'average_score': 'Score Médio'})
                fig.update_layout(margin=dict(l=20, r=20, t=0, b=0), height=300)
                st.plotly_chart(fig, use_container_width=True, key=f"score_chart_hist_{iteration_counter}")
            else:
                st.warning("Aguardando dados de score médio diário...")

        with col5:
            st.markdown("###### Novos Clientes por Dia")
            daily_clients_df = get_historical_data(db_conn, "SELECT date, new_clients_count FROM daily_new_clients WHERE date > (CURRENT_DATE - INTERVAL '30 days') AND date <= CURRENT_DATE ORDER BY date ASC;")
            if not daily_clients_df.empty:
                fig = px.bar(daily_clients_df, x='date', y='new_clients_count', labels={'date': 'Data', 'new_clients_count': 'Novos Clientes'})
                fig.update_layout(margin=dict(l=20, r=20, t=0, b=0), height=300)
                st.plotly_chart(fig, use_container_width=True, key=f"clients_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de novos clientes...")

        st.markdown("---")

        # --- Linha 4: Tabelas de Detalhes (Tempo Real) ---
        col6, col7 = st.columns(2)

        with col6:
            st.subheader("Top 10 Clientes por Gastos (Última Hora)")
            top_clients_data = get_list_data("analysis:top_10_clientes")
            if top_clients_data:
                df_top_clients = pd.DataFrame([json.loads(item) for item in top_clients_data])
                st.dataframe(df_top_clients, use_container_width=True, height=360, key=f"top_clients_df_{iteration_counter}")
            else:
                st.warning("Aguardando dados de top clientes...")
                
        with col7:
            st.subheader("Alertas de Risco Ativos")
            risk_alerts_data = get_set_data("analysis:risk_alerts")
            if risk_alerts_data:
                df_alerts = pd.DataFrame([json.loads(item) for item in risk_alerts_data])
                st.dataframe(df_alerts, use_container_width=True, height=360, key=f"alerts_df_{iteration_counter}")
            else:
                st.warning("Nenhum alerta de risco no momento.")

    iteration_counter += 1
    time.sleep(5)