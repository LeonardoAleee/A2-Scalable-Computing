# Use uma imagem base Python
FROM python:3.9-slim

# Instala o Java (dependência do PySpark)
RUN apt-get update && apt-get install -y default-jdk && apt-get clean

# Define o diretório de trabalho
WORKDIR /app

# Copia e instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o script do consumidor para o contêiner
# Renomeamos para 'consumer.py' para simplicidade no comando CMD
COPY consumer_2.py /app/consumer.py

# Comando para executar o script Python que inicia a aplicação Spark
CMD ["python", "consumer.py"] 