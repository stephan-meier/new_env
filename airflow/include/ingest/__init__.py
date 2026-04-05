"""Metadatengetriebener Ingest-Layer auf Basis von dlt.

Quellen werden in YAML deklariert (airflow/config/ingest_sources/),
die DAG-Factory (airflow/dags/ingest_factory.py) generiert pro YAML
einen Airflow-DAG. Der Runner dispatcht auf quellentyp-spezifische
Builder in ingest.sources.*.

Modul liegt in airflow/include/ (nicht plugins/), weil Airflow 3 den
plugins/-Ordner rekursiv als Plugin-Klassen-Quelle scannt und eigene
Module dort zu Import-Fehlern beim Scheduler-Start fuehren.
"""
