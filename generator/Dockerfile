# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Definir o diretório de trabalho no contêiner
WORKDIR /app

# Copiar os arquivos de dependências
COPY requirements.txt ./

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código da aplicação
COPY . .

# Comando para rodar a aplicação
CMD ["python", "generator.py"] 