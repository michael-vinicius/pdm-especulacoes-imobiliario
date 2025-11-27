# 1. Imagem base (Python leve)
FROM python:3.10-slim

# 2. Define pasta de trabalho dentro do container
WORKDIR /app

# 3. Instala dependências do sistema (necessário para alguns pacotes)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 4. Copia e instala as bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todo o seu código para dentro da imagem
COPY . .

# 6. Dá permissão para executar o script de inicialização
RUN chmod +x start.sh

# 7. Comando para ligar a API
CMD ["./start.sh"]