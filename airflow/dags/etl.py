from scripts import extract_load

from airflow.decorators import task
from airflow import DAG
from pendulum import datetime

# Instancier votre classe dv3f
dv3f_instance = extract_load.dv3f()

with DAG(
    dag_id="ETL",
    start_date=datetime(2024,3,31,hour=10,minute=0,tz="Europe/Paris"),
    schedule=datetime.timedelta(hours=1),
    tags=["dev_test"],
):
    # Définir une fonction pour appeler la méthode get_data de dv3f
    @task
    def extract_data():
        dv3f_instance.get_data(annee=2023, scope='region', code='example_code')
    extract_task = extract_data()
    # Définir une fonction pour appeler la méthode transform_data de dv3f
    @task
    def transform_data():
        dv3f_instance.transform_data()
    transform_task = transform_data()
    # Définir une fonction pour appeler la méthode load_data de dv3f
    @task
    def load_data():
        dv3f_instance.load_data()
    load_task = load_data()

    # Définir l'ordre d'exécution des tâches
    extract_task >> transform_task >> load_task