import json
import time
import random
from faker import Faker
from kafka import KafkaProducer
import logging
import sys
import os
import socket

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Adicionando uma semente única para cada instância do gerador
# Isso garante que múltiplos geradores produzam dados diferentes.
# A semente é baseada no hostname do contêiner e no ID do processo.
try:
    seed_value = f"{socket.gethostname()}-{os.getpid()}"
    Faker.seed(seed_value)
    random.seed(seed_value)
    logging.info(f"Gerador inicializado com a semente: {seed_value}")
except Exception as e:
    logging.warning(f"Não foi possível definir uma semente determinística: {e}. Usando aleatoriedade padrão.")

fake = Faker('pt_BR')

def create_kafka_producer(bootstrap_servers):
    """
    Cria um produtor Kafka, com múltiplas tentativas de conexão.
    """
    producer = None
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logging.info("Conexão com o Kafka estabelecida com sucesso.")
            return producer
        except Exception as e:
            logging.warning(f"Falha ao conectar ao Kafka: {e}. Tentando novamente em 5 segundos... (Tentativa {i+1}/10)")
            time.sleep(5)
    raise Exception("Não foi possível conectar ao Kafka após várias tentativas.")


def generate_data(producer):
    """
    Gera e envia dados para os tópicos Kafka em um loop infinito.
    """
    client_topic = 'clientes'
    score_topic = 'scores'
    transaction_topic = 'transacoes'

    logging.info(f"Iniciando a geração de dados para os tópicos: {client_topic}, {score_topic}, {transaction_topic}")

    while True:
        try:
            # Gera dados de cliente e score para o mesmo CPF
            cpf = fake.cpf()
            client_id = str(fake.uuid4())
            
            client_data = {
                'ID': client_id,
                'Nome': fake.name(),
                'CPF': cpf,
                'DataNasc': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
                'Endereco': fake.address()
            }
            
            score_data = {
                'CPF': cpf,
                'Score': random.randint(0, 1000),
                'RendaMensal': round(random.uniform(1200, 15000), 2),
                'LimiteCredito': round(random.uniform(500, 50000), 2),
                'AtualizadoEm': fake.iso8601()
            }

            # Envia dados de cliente e score
            producer.send(client_topic, key=client_id, value=client_data)
            producer.send(score_topic, key=cpf, value=score_data)
            logging.info(f"Enviado Cliente: {client_id} e Score para CPF: {cpf}")

            # Gera um número aleatório de transações para o cliente
            for _ in range(random.randint(1, 5)):
                transaction_id = str(fake.uuid4())
                transaction_data = {
                    'ID': transaction_id,
                    'ClienteID': client_id, # Chave estrangeira para o cliente
                    'Data': fake.iso8601(),
                    'Valor': round(random.uniform(10, 2000), 2),
                    'Moeda': random.choice(['BRL', 'USD', 'EUR'])
                }
                producer.send(transaction_topic, key=transaction_id, value=transaction_data)
                logging.info(f"Enviada Transação: {transaction_id} para Cliente: {client_id}")

            producer.flush()
            logging.info("--- Lote de dados enviado ---")
            time.sleep(random.uniform(0.5, 2.0))

        except Exception as e:
            logging.error(f"Ocorreu um erro no loop de geração de dados: {e}")
            time.sleep(10)


if __name__ == "__main__":
    KAFKA_BROKER_URL = 'kafka:9092'
    
    try:
        producer = create_kafka_producer(KAFKA_BROKER_URL)
        generate_data(producer)
    except Exception as e:
        logging.critical(f"Falha crítica ao iniciar o gerador: {e}")
        sys.exit(1) 