# Airflow DAG's
For this DAG's I have used airflow 2.1.0 with Celery worker and 2 instanses of Postgres 11 as a target and source DB.
# Psql_importer_airflow.py
Get all tables and table schema from source database and dump them to target database.
# Psql_to_data_vault
Copy schema and table data from source database to stage layer of target DB. Create data_vault schema in core layer of target database, transform and load data to core layer from stage. Loading in core layer incrementally.
