import streamlit as st
import pandas as pd
import time
import plotly.express as px
import os
import psycopg2
import sys
from datetime import datetime

# --- Função de Logging ---
def log_message(level, message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    print(f"{timestamp} - {level} - DashboardDB - {message}", file=sys.stderr, flush=True)

# --- Configuração da Página ---
st.set_page_config(
    page_title="Dashboard de Análise de Risco",
    layout="wide"
)

# --- Conexão com Banco de Dados ---
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres") 
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

@st.cache_resource
def get_db_connection():
    log_message("INFO", f"Tentando conectar ao banco de dados em: {DB_HOST}")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        log_message("INFO", "Conexão com banco de dados bem-sucedida.")
        return conn
    except psycopg2.OperationalError as e:
        log_message("ERROR", f"Não foi possível conectar ao banco de dados: {e}")
        st.error(f"Não foi possível conectar ao banco de dados: {e}. Verifique as variáveis de ambiente e a conectividade.")
        return None

db_conn = get_db_connection()
if db_conn is None:
    st.stop()

# --- Funções para buscar dados do RDS ---
@st.cache_data(ttl=10) 
def get_data_from_db(_conn, query):
    if _conn is None:
        log_message("WARNING", "Conexão com o DB indisponível para a query.")
        return pd.DataFrame()
    try:
        log_message("INFO", f"Executando query: {query}")
        df = pd.read_sql_query(query, _conn)
        log_message("INFO", f"Query retornou {len(df)} linhas.")
        return df
    except Exception as e:
        log_message("ERROR", f"Erro ao buscar dados do DB: {e}")
        return pd.DataFrame()

# --- Layout do Dashboard ---
st.title("Dashboard de Análise de Risco")
placeholder = st.empty()
iteration_counter = 0

# --- Loop de atualização ---
while True:
    with placeholder.container():
        log_message("INFO", f"Atualizando dashboard... Iteração: {iteration_counter}")
        
        # --- Linha 1: Métricas Globais ---
        kpi2, kpi3, kpi4 = st.columns(3)

        # KPI 1: Ticket Médio
        df_ticket = get_data_from_db(db_conn, "SELECT ticket_medio FROM analysis_ticket_medio WHERE id = 'singleton';")
        ticket_medio = df_ticket['ticket_medio'].iloc[0] if not df_ticket.empty else 0
        kpi2.metric(label="Ticket Médio (BRL)", value=f"R$ {ticket_medio:,.2f}")

        # KPI 2: Clientes em Alerta de Risco
        df_alerts_count = get_data_from_db(db_conn, "SELECT count(*) as total FROM analysis_risk_alerts;")
        risk_alerts_count = df_alerts_count['total'].iloc[0] if not df_alerts_count.empty else 0
        kpi3.metric(label="Clientes em Alerta de Risco", value=risk_alerts_count)

        # KPI 3: Total de Clientes (do RDS)
        df_total_clients = get_data_from_db(db_conn, "SELECT SUM(new_clients_count) as total FROM daily_new_clients;")
        total_clients = df_total_clients['total'].iloc[0] if not df_total_clients.empty and df_total_clients['total'].iloc[0] is not None else 0
        kpi4.metric(label="Total de Clientes", value=int(total_clients))
        
        st.markdown("---")

        # --- Linha 2: Gráficos de Distribuição ---
        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"Distribuição de Clientes por Score")
            df_score = get_data_from_db(db_conn, "SELECT faixa_score, count FROM analysis_score_distribution;")
            if not df_score.empty:
                fig = px.bar(df_score, x='faixa_score', y='count', title="", text_auto=True, labels={'faixa_score': 'Faixa de Score', 'count': 'Quantidade'})
                st.plotly_chart(fig, use_container_width=True, key=f"score_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de distribuição de score...")

        with col2:
            st.subheader(f"Distribuição de Clientes por Faixa Etária")
            df_age = get_data_from_db(db_conn, "SELECT faixa_etaria, count FROM analysis_age_distribution;")
            if not df_age.empty:
                fig = px.pie(df_age, names='faixa_etaria', values='count', title="", labels={'faixa_etaria': 'Faixa Etária', 'count': 'Quantidade'})
                st.plotly_chart(fig, use_container_width=True, key=f"age_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de distribuição de idade...")
        
        st.markdown("---")
        
        # --- Linha 3: Gráficos de Série Temporal (Histórico) ---
        st.subheader("Análises Históricas (últimos 30 dias)")
        
        col3, col4, col5 = st.columns(3)
        
        with col3:
            st.markdown("###### Volume Diário de Transações")
            daily_volume_df = get_data_from_db(db_conn, "SELECT date, total_volume FROM daily_transaction_volume WHERE date > (CURRENT_DATE - INTERVAL '30 days') ORDER BY date ASC;")
            if not daily_volume_df.empty:
                fig = px.bar(daily_volume_df, x='date', y='total_volume', labels={'date': 'Data', 'total_volume': 'Volume (R$)'})
                st.plotly_chart(fig, use_container_width=True, key=f"volume_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de volume diário...")

        with col4:
            st.markdown("###### Score Médio Diário")
            daily_score_df = get_data_from_db(db_conn, "SELECT date, average_score FROM daily_average_score WHERE date > (CURRENT_DATE - INTERVAL '30 days') ORDER BY date ASC;")
            if not daily_score_df.empty:
                fig = px.line(daily_score_df, x='date', y='average_score', markers=True, labels={'date': 'Data', 'average_score': 'Score Médio'})
                st.plotly_chart(fig, use_container_width=True, key=f"score_chart_hist_{iteration_counter}")
            else:
                st.warning("Aguardando dados de score médio diário...")

        with col5:
            st.markdown("###### Novos Clientes por Dia")
            daily_clients_df = get_data_from_db(db_conn, "SELECT date, new_clients_count FROM daily_new_clients WHERE date > (CURRENT_DATE - INTERVAL '30 days') ORDER BY date ASC;")
            if not daily_clients_df.empty:
                fig = px.bar(daily_clients_df, x='date', y='new_clients_count', labels={'date': 'Data', 'new_clients_count': 'Novos Clientes'})
                st.plotly_chart(fig, use_container_width=True, key=f"clients_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de novos clientes...")

        st.markdown("---")

        # --- Linha 4: Tabelas de Detalhes ---
        col6, col7 = st.columns(2)

        with col6:
            st.subheader(f"Top 10 Clientes por Gastos")
            df_top_clients = get_data_from_db(db_conn, "SELECT rank, cliente_id, total_gasto FROM analysis_top_10_clientes ORDER BY rank ASC;")
            if not df_top_clients.empty:
                st.dataframe(df_top_clients, use_container_width=True, height=360, key=f"top_clients_df_{iteration_counter}")
            else:
                st.warning("Aguardando dados de top clientes...")
                
        with col7:
            st.subheader("Alertas de Risco Ativos")
            df_alerts = get_data_from_db(db_conn, "SELECT cpf, score, limite_credito FROM analysis_risk_alerts;")
            if not df_alerts.empty:
                st.dataframe(df_alerts, use_container_width=True, height=360, key=f"alerts_df_{iteration_counter}")
            else:
                st.warning("Nenhum alerta de risco no momento.")

    iteration_counter += 1
    time.sleep(10) 