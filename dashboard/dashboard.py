import streamlit as st
import redis
import pandas as pd
import json
import time
import plotly.express as px

# --- Configuração da Página ---
st.set_page_config(
    page_title="Dashboard de Análise de Risco",
    page_icon="✅",
    layout="wide"
)

# --- Conexão com Redis ---
@st.cache_resource
def get_redis_connection():
    return redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

r = get_redis_connection()

# --- Funções para buscar dados do Redis ---
def get_hash_data(key_name):
    return r.hgetall(key_name)

def get_zset_data(key_name):
    return r.zrange(key_name, 0, -1, withscores=True)

def get_list_data(key_name):
    return r.lrange(key_name, 0, -1)

def get_set_data(key_name):
    return r.smembers(key_name)

# --- Layout do Dashboard ---
st.title("🚀 Dashboard de Análise de Risco em Tempo Real")
placeholder = st.empty()
iteration_counter = 0

# --- Loop de atualização ---
while True:
    with placeholder.container():
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

        # KPI 4: Total de Clientes
        total_clients = int(r.get("analysis:total_clients") or 0)
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
        
        # --- Linha 3: Gráficos de Série Temporal ---
        col3, col4 = st.columns(2)
        
        with col3:
            st.subheader("Evolução Diária do Volume de Transações")
            daily_volume_data = get_zset_data("analysis:daily_volume")
            if daily_volume_data:
                df_volume = pd.DataFrame(daily_volume_data, columns=['Data', 'Volume'])
                df_volume['Data'] = pd.to_datetime(df_volume['Data'])
                fig = px.bar(df_volume, x='Data', y='Volume', title="")
                st.plotly_chart(fig, use_container_width=True, key=f"volume_chart_{iteration_counter}")
            else:
                st.warning("Aguardando dados de volume diário...")

        with col4:
            st.subheader("Top 10 Clientes por Gastos (Última Hora)")
            top_clients_data = get_list_data("analysis:top_10_clientes")
            if top_clients_data:
                df_top_clients = pd.DataFrame([json.loads(item) for item in top_clients_data])
                st.dataframe(df_top_clients, use_container_width=True, height=360)
            else:
                st.warning("Aguardando dados de top clientes...")
                
        st.markdown("---")
        
        # --- Linha 4: Tabelas de Detalhes ---
        st.subheader("Alertas de Risco Ativos")
        risk_alerts_data = get_set_data("analysis:risk_alerts")
        if risk_alerts_data:
            df_alerts = pd.DataFrame([json.loads(item) for item in risk_alerts_data])
            st.dataframe(df_alerts, use_container_width=True)
        else:
            st.warning("Nenhum alerta de risco no momento.")

    iteration_counter += 1
    time.sleep(5)