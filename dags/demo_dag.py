from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# Définir un dataset (URI unique pour identifier la ressource)
my_dataset = Dataset("my_dataset_uri")

@dag(start_date=datetime(2023, 1, 1), schedule=None, catchup=False)
def mon_dag():
    @task(outlets=[my_dataset])  # Cette tâche produit le dataset
    def produire_donnees():
        # Logique pour produire/mettre à jour les données
        print("Production des données terminée !")

    @task(inlets=[my_dataset])  # Cette tâche consomme le dataset
    def consommer_donnees():
        # Logique pour consommer les données
        print("Consommation des données en cours...")

    # Instancier les tâches
    produire_donnees()  # Pas besoin de définir manuellement la dépendance !
    consommer_donnees()

mon_dag()
