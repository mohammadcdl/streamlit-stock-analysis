# Choisir une image Python légère
FROM python:3.10-slim

# Installer Java (pour Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk-headless && rm -rf /var/lib/apt/lists/*

# Créer un dossier de travail
WORKDIR /app

# Copier tes dépendances Python
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code dans l'image
COPY . /app

# Définir la commande de démarrage
# Render va injecter la variable $PORT pour indiquer sur quel port il faut écouter.
CMD ["streamlit", "run", "app.py", "--server.port=$PORT", "--server.address=0.0.0.0"]
