import json
import time
import random
from datetime import datetime
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

    while True:
        try:
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
                'LimiteCredito': round(random.uniform(1000, 30000), 2),
                'AtualizadoEm': datetime.utcnow().isoformat()
            }

            producer.send(client_topic, key=client_id, value=client_data)
            producer.send(score_topic, key=cpf, value=score_data)

            # Gera um número aleatório de transações para o cliente
            for _ in range(random.randint(1, 5)):
                transaction_id = str(fake.uuid4())
                send_timestamp = datetime.utcnow()
                transaction_data = {
                    'ID': transaction_id,
                    'ClienteID': client_id,
                    'Data': send_timestamp.isoformat(),
                    'Valor': round(random.uniform(10, 2000), 2),
                    'Moeda': random.choice(['BRL', 'USD', 'EUR'])
                }
                producer.send(transaction_topic, key=transaction_id, value=transaction_data)
                logging.info(f"SEND_MSG_LOG: id={transaction_id}, timestamp={send_timestamp.isoformat()}")

            producer.flush()

            logging.info(f"--- Lote de dados com {_+1} transações enviado para o Kafka ---")
            time.sleep(random.uniform(0.5, 2.0))

        except Exception as e:
            logging.error(f"Ocorreu um erro no loop de geração de dados: {e}")
            time.sleep(10)


if __name__ == "__main__":
    KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
    
    try:
        producer = create_kafka_producer(KAFKA_BROKER_URL)
        generate_data(producer)
    except Exception as e:
        logging.critical(f"Falha crítica ao iniciar o gerador: {e}")
        sys.exit(1) 