import psycopg2
import os
import sys
import logging
from time import sleep

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - CreateTables - %(message)s',
    stream=sys.stdout
)

# --- Configuração do Banco de Dados a partir de Variáveis de Ambiente ---
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT")

def get_db_connection():
    """Tenta estabelecer uma conexão com o banco de dados com múltiplas tentativas."""
    retry_count = 0
    while retry_count < 5:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT
            )
            logging.info("Conexão com o banco de dados bem-sucedida.")
            return conn
        except psycopg2.OperationalError as e:
            logging.warning(f"Não foi possível conectar ao banco de dados: {e}. Tentando novamente em 5 segundos...")
            retry_count += 1
            sleep(5)
    logging.error("Não foi possível estabelecer conexão com o banco de dados após várias tentativas.")
    sys.exit(1)


def create_tables(conn):
    
    commands = [
        # --- Tabelas para Análises em "Tempo Real" ---
        """
        CREATE TABLE IF NOT EXISTS analysis_score_distribution (
            faixa_score VARCHAR(50) PRIMARY KEY,
            count BIGINT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analysis_ticket_medio (
            id VARCHAR(20) PRIMARY KEY DEFAULT 'singleton',
            ticket_medio DOUBLE PRECISION NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analysis_volume_by_currency (
            moeda VARCHAR(10) PRIMARY KEY,
            volume_total DOUBLE PRECISION NOT NULL,
            quantidade BIGINT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analysis_top_10_clientes (
            rank INT PRIMARY KEY,
            cliente_id VARCHAR(255) NOT NULL,
            total_gasto DOUBLE PRECISION NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analysis_age_distribution (
            faixa_etaria VARCHAR(50) PRIMARY KEY,
            count BIGINT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS analysis_risk_alerts (
            cpf VARCHAR(20) PRIMARY KEY,
            score INT NOT NULL,
            limite_credito DOUBLE PRECISION NOT NULL
        );
        """,
        # --- Tabelas para Análises Históricas ---
        """
        CREATE TABLE IF NOT EXISTS daily_transaction_volume (
            date DATE PRIMARY KEY,
            total_volume DOUBLE PRECISION NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_new_clients (
            date DATE PRIMARY KEY,
            new_clients_count INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_average_score (
            date DATE PRIMARY KEY,
            average_score DOUBLE PRECISION NOT NULL
        );
        """
    ]
    
    try:
        with conn.cursor() as cur:
            for command in commands:
                logging.info(f"Executando comando: {command.strip().splitlines()[1]}")
                cur.execute(command)
        conn.commit()
        logging.info("Todas as tabelas foram criadas ou já existiam com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erro ao criar tabelas: {error}")
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

if __name__ == '__main__':
    logging.info("Iniciando script de criação de tabelas.")
    connection = get_db_connection()
    if connection:
        create_tables(connection) 