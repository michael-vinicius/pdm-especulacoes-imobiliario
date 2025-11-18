# Use uma imagem base oficial do Python (VERSÃO 3.11 para o numpy)
FROM python:3.11-slim

# Defina o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copie o arquivo de dependências
COPY requirements.txt .

# Instale as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copie o resto do seu código
# Isso irá copiar a pasta 'ELT' para dentro de '/app'
COPY . .

# A variável de ambiente PORT é fornecida automaticamente pelo Cloud Run.
ENV PORT=8080

# [CORRIGIDO] Comando para iniciar o SERVIDOR WEB.
# Corrigimos de "main:app" para "ELT.Dataframe_populate:app"
#
# Isso diz ao Gunicorn:
# 1. Olhe na pasta 'ELT' (que agora é um módulo Python)
# 2. Encontre o arquivo 'Dataframe_populate'
# 3. Encontre a variável 'app' dentro dele
CMD ["gunicorn", "-b", "0.0.0.0:${PORT}", "ELT.Dataframe_populate:app"]