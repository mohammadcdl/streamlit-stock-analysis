# Utiliser une image Python avec Java préinstallé
FROM openjdk:11-slim

# Installer Python et Pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code de l’application
COPY . /app
WORKDIR /app

# Exposer le bon port
ENV PORT=10000
EXPOSE 10000

# Lancer Streamlit
CMD streamlit run app.py --server.port 10000 --server.enableCORS false
