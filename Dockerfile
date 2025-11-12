# 1. Imagem Base: 
FROM python:3.11-slim

# 2. Define o diretório de trabalho dentro do container
WORKDIR /app

# 3. Define variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 4. Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia o código da aplicação
# Copia a pasta local 'app' para a pasta '/app/app' no container
COPY ./app ./app

# 6. Expõe a porta que a API vai rodar
EXPOSE 8000

# 7. Comando para iniciar a API
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]