FROM python:3.11-slim

# Variáveis de ambiente
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Instala dependências do sistema: Java (para Spark) e procps (para o comando 'ps')
# O uso de 'USER root' é temporário para permitir a instalação.
USER root
RUN apt-get update && apt-get install -y default-jdk procps && \
    rm -rf /var/lib/apt/lists/*

# Configura o JAVA_HOME, essencial para o Spark encontrar o Java.
ENV JAVA_HOME /usr/lib/jvm/default-java

# Define o diretório de trabalho para a aplicação.
WORKDIR /app

# Copia e instala as dependências do Python.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos da aplicação.
COPY . .

# Comando para rodar a aplicação.
# O Spark é executado como o usuário padrão do contêiner a partir daqui.
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "consumer.py"] 