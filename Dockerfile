# Choix de l'image Python (Debian Bookworm en version slim)
FROM python:3.10-slim

# 1) Installer Java 17 dans le conteneur
RUN apt-get update && apt-get install -y openjdk-17-jdk-headless && rm -rf /var/lib/apt/lists/*

# 2) Définir JAVA_HOME et ajouter /bin à ton PATH
#    (Le chemin ci-dessous est souvent correct sous Debian Bookworm,
#     mais tu peux essayer "/usr/lib/jvm/java-17-openjdk-headless" ou "/usr/lib/jvm/default-java" si besoin.)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# 3) Créer un répertoire de travail
WORKDIR /app

# 4) Installer les bibliothèques Python (PySpark, Streamlit, etc.)
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# 5) Copier le reste du code (dont app.py) dans /app
COPY . /app

# 6) Lancer Streamlit en écoutant sur le port imposé par Render ($PORT) et l'adresse 0.0.0.0
CMD ["streamlit", "run", "app.py", "--server.port=$PORT", "--server.address=0.0.0.0"]
