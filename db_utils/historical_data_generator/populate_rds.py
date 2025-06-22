import os
import psycopg
import random
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# --- Configurações do Banco de Dados (lidas do ambiente) ---
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

# --- Configurações da Geração de Dados ---
fake = Faker('pt_BR')
DAYS_TO_GENERATE = 365

def get_db_connection():
    conn = psycopg.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_transaction_volume (
            date DATE PRIMARY KEY,
            total_volume NUMERIC(15, 2) NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_average_score (
            date DATE PRIMARY KEY,
            average_score NUMERIC(7, 2) NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_new_clients (
            date DATE PRIMARY KEY,
            new_clients_count INTEGER NOT NULL
        );
        """)
        conn.commit()
    print("Tabelas verificadas/criadas com sucesso.")

def populate_historical_data(conn):
    today = datetime.now()
    
    with conn.cursor() as cur:
        # 1. Volume de Transações Diário
        print("Gerando dados para 'daily_transaction_volume'...")
        for i in range(DAYS_TO_GENERATE):
            date = today - timedelta(days=i)
            volume = random.uniform(50000, 200000) * (1 - (i / (DAYS_TO_GENERATE * 1.5))) # Tendência de queda para o passado
            cur.execute(
                "INSERT INTO daily_transaction_volume (date, total_volume) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING;",
                (date.date(), volume)
            )

        # 2. Score Médio Diário
        print("Gerando dados para 'daily_average_score'...")
        for i in range(DAYS_TO_GENERATE):
            date = today - timedelta(days=i)
            # Score médio oscila entre 650 e 750
            score = random.uniform(650, 750) + random.uniform(-20, 20)
            cur.execute(
                "INSERT INTO daily_average_score (date, average_score) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING;",
                (date.date(), score)
            )

        # 3. Contagem de Novos Clientes Diário
        print("Gerando dados para 'daily_new_clients'...")
        for i in range(DAYS_TO_GENERATE):
            date = today - timedelta(days=i)
            new_clients = random.randint(5, 50)
            cur.execute(
                "INSERT INTO daily_new_clients (date, new_clients_count) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING;",
                (date.date(), new_clients)
            )
            
        conn.commit()
    print("Dados históricos inseridos com sucesso.")

def main():
    """Função principal para executar o script."""
    if not all([DB_HOST, DB_USER, DB_PASSWORD]):
        print("Erro: As variáveis de ambiente DB_HOST, DB_USER, e DB_PASSWORD devem ser definidas.")
        return

    print("Iniciando script de população de dados históricos...")
    try:
        conn = get_db_connection()
        create_tables(conn)
        populate_historical_data(conn)
        conn.close()
        print("Script concluído.")
    except psycopg.OperationalError as e:
        print(f"Erro de conexão com o banco de dados: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    main() 