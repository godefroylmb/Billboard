French Below


# Billboard Charts Data Pipeline

An automated data pipeline that scrapes Billboard chart data using Apache Airflow, stores it in PostgreSQL, and exports it to CSV files.

## 📊 Overview

This project automates the collection of Billboard music charts data on a weekly basis. It scrapes five different chart types dating back to their inception, storing historical and current data in a structured database.

### Supported Charts

| Chart Name | Start Date |
|------------|------------|
| **Hot 100** | 1958-07-30 |
| **Global 200** | 1963-08-07 |
| **Radio Songs** | 1990-10-24 |
| **Streaming Songs** | 2013-01-16 |
| **Digital Song Sales** | 2004-10-20 |

## 🏗️ Architecture

- **Orchestration**: Apache Airflow
- **Database**: PostgreSQL
- **Scraping**: aiohttp + BeautifulSoup4 (async)
- **Containerization**: Docker Compose
- **Schedule**: Weekly on Wednesdays at 3:00 AM UTC

## 🚀 Quick Start

### Prerequisites

- Docker
- Docker Compose

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd "Billboard - Rework"
```

2. **Start the services**
```bash
docker-compose up -d
```

3. **Access Airflow UI**
```
URL: http://localhost:8080
Username: #usernamedockerompose
Password: #passworddockerompose
```

4. **Trigger the DAG**
   - Navigate to the `billboard_dag` in the Airflow UI
   - Enable the DAG using the toggle switch
   - Trigger a manual run or wait for the scheduled execution

## 📂 Project Structure

```
.
├── docker-compose.yml          # Docker orchestration configuration
├── readme.md                   # This file
├── dags/
│   ├── billboard_dag.py       # Main Airflow DAG
│   └── billboard/
│       ├── billboard.ini      # Charts configuration
│       ├── create_tables.sql  # Database schema
│       └── scripts/
│           ├── config.py      # Configuration loader
│           ├── db_manager.py  # Database operations
│           └── scraper.py     # Web scraping logic
├── logs/                      # Airflow logs
└── plugins/                   # Airflow plugins
```

## 🔄 Pipeline Workflow

The DAG consists of three main task groups:

### 1. **Scrape** (`scrap_group`)
- Fetches data from Billboard.com for each chart
- Respects individual chart start dates
- Uses async HTTP requests for efficiency
- Extracts: rank, song name, artist, last week position, peak position, weeks in chart, image URL

### 2. **Insert** (`insert_group`)
- Inserts scraped data into PostgreSQL
- Uses `ON CONFLICT DO NOTHING` to avoid duplicates
- Each chart has its own table in the `billboard` schema

### 3. **Export** (`export_group`)
- Exports data to CSV files
- Prepares data for external use (e.g., Kaggle)

### 4. **Upload** (Planned)
- Upload to Kaggle datasets (to be implemented)

## 🗄️ Database Schema

Each chart table contains:
- `chart_date` (DATE): The chart date
- `song_name` (VARCHAR): Song title
- `artist` (VARCHAR): Artist name
- `rank` (INTEGER): Position on the chart
- `last_week` (VARCHAR): Previous week position
- `peak_position` (VARCHAR): Highest position reached
- `weeks_in_chart` (VARCHAR): Number of weeks on chart
- `image_url` (TEXT): Cover art URL

**Primary Key**: `(chart_date, rank)`

## ⚙️ Configuration

### Chart Configuration (`billboard.ini`)

Add or modify charts by editing the configuration file:

```ini
[chart_name]
full_url = https://www.billboard.com/charts/chart-name/
url_path = chart-name
table = table_name
start_date = YYYY-MM-DD
```

## 🔧 Development

### Database Connection

The DAG connects to PostgreSQL using:
```
Host: postgres
Port: 5432
Database: airflow
User: airflow
Password: airflow
```

## 📦 Dependencies

Python packages (included in Airflow image):
- apache-airflow==2.9.3
- aiohttp
- beautifulsoup4
- psycopg2-binary

## 🤝 Contributing

Contributions are welcome!

## 📄 License

This project is for educational and personal use.

---

# Pipeline de Données Billboard Charts

Un pipeline automatisé qui récupère les données des classements Billboard en utilisant Apache Airflow, les stocke dans PostgreSQL et les exporte en fichiers CSV.

## 📊 Aperçu

Ce projet automatise la collecte des données des classements musicaux Billboard de manière hebdomadaire. Il récupère cinq types de classements différents depuis leur création, stockant les données historiques et actuelles dans une base de données structurée.

### Classements Supportés

| Nom du Classement | Date de Début |
|-------------------|---------------|
| **Hot 100** | 1958-07-30 |
| **Global 200** | 1963-08-07 |
| **Radio Songs** | 1990-10-24 |
| **Streaming Songs** | 2013-01-16 |
| **Digital Song Sales** | 2004-10-20 |

## 🏗️ Architecture

- **Orchestration** : Apache Airflow
- **Base de données** : PostgreSQL
- **Scraping** : aiohttp + BeautifulSoup4 (async)
- **Conteneurisation** : Docker Compose
- **Planification** : Hebdomadaire le mercredi à 3h00 UTC

## 🚀 Démarrage Rapide

### Prérequis

- Docker
- Docker Compose

### Installation

1. **Cloner le dépôt**
```bash
git clone <url-du-dépôt>
cd "Billboard - Rework"
```

2. **Démarrer les services**
```bash
docker-compose up -d
```

3. **Accéder à l'interface Airflow**
```
URL: http://localhost:8080
Nom d'utilisateur: #usernamedockercompose
Mot de passe: #passworddockercompose
```

4. **Déclencher le DAG**
   - Naviguez vers `billboard_dag` dans l'interface Airflow
   - Activez le DAG avec le bouton bascule
   - Déclenchez une exécution manuelle ou attendez l'exécution planifiée

## 📂 Structure du Projet

```
.
├── docker-compose.yml          # Configuration d'orchestration Docker
├── readme.md                   # Ce fichier
├── dags/
│   ├── billboard_dag.py       # DAG principal Airflow
│   └── billboard/
│       ├── billboard.ini      # Configuration des classements
│       ├── create_tables.sql  # Schéma de base de données
│       └── scripts/
│           ├── config.py      # Chargeur de configuration
│           ├── db_manager.py  # Opérations de base de données
│           └── scraper.py     # Logique de web scraping
├── logs/                      # Logs Airflow
└── plugins/                   # Plugins Airflow
```

## 🔄 Flux du Pipeline

Le DAG se compose de trois groupes de tâches principaux :

### 1. **Scrape** (`scrap_group`)
- Récupère les données de Billboard.com pour chaque classement
- Respecte les dates de début individuelles de chaque classement
- Utilise des requêtes HTTP async pour l'efficacité
- Extrait : classement, nom de la chanson, artiste, position semaine dernière, position maximale, semaines au classement, URL de l'image

### 2. **Insert** (`insert_group`)
- Insère les données récupérées dans PostgreSQL
- Utilise `ON CONFLICT DO NOTHING` pour éviter les doublons
- Chaque classement a sa propre table dans le schéma `billboard`

### 3. **Export** (`export_group`)
- Exporte les données vers des fichiers CSV
- Prépare les données pour une utilisation externe (ex: Kaggle)

### 4. **Upload** (Prévu)
- Upload vers les datasets Kaggle (à implémenter)

## 🗄️ Schéma de Base de Données

Chaque table de classement contient :
- `chart_date` (DATE) : La date du classement
- `song_name` (VARCHAR) : Titre de la chanson
- `artist` (VARCHAR) : Nom de l'artiste
- `rank` (INTEGER) : Position dans le classement
- `last_week` (VARCHAR) : Position la semaine précédente
- `peak_position` (VARCHAR) : Position maximale atteinte
- `weeks_in_chart` (VARCHAR) : Nombre de semaines au classement
- `image_url` (TEXT) : URL de la pochette

**Clé primaire** : `(chart_date, rank)`

## ⚙️ Configuration

### Configuration des Classements (`billboard.ini`)

Ajoutez ou modifiez les classements en éditant le fichier de configuration :

```ini
[nom_classement]
full_url = https://www.billboard.com/charts/nom-classement/
url_path = nom-classement
table = nom_table
start_date = AAAA-MM-JJ
```

## 🔧 Développement

### Connexion à la Base de Données

Le DAG se connecte à PostgreSQL via :
```
Hôte: postgres
Port: 5432
Base de données: airflow
Utilisateur: airflow
Mot de passe: airflow
```

## 📦 Dépendances

Packages Python (inclus dans l'image Airflow) :
- apache-airflow==2.9.3
- aiohttp
- beautifulsoup4
- psycopg2-binary

## 🤝 Contribution

Les contributions sont les bienvenues !

## 📄 Licence

Ce projet est destiné à un usage éducatif et personnel.