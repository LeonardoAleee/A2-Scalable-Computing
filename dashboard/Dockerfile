# Define a imagem base
FROM python:3.9-slim

# Define o diretório de trabalho
WORKDIR /app

# Define a porta que o Streamlit irá usar
EXPOSE 8501

# Copia e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o script do dashboard
COPY dashboard_2.py .

# Define o comando para iniciar o aplicativo Streamlit, apontando para o script correto
CMD ["streamlit", "run", "dashboard_2.py"] 