from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator



DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 3),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
        dag_id="test_customer",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        tags=['data-flow'],
) as dag:

    def get_tables():
        tables = []
        PG_SOURCE = PostgresHook(postgres_conn_id='postgres_target')
        conn = PG_SOURCE.get_conn()
        cursor = conn.cursor()
        query = "select table_name from information_schema.tables" \
                " where table_schema='public' and table_type='BASE TABLE'"
        cursor.execute(query)
        print(cursor)
        for elem in cursor:
            tables.append(elem[0])
        return tables

    def get_source_schema(**kwargs):
        PG_SOURCE = PostgresHook(postgres_conn_id='postgres_target')
        PG_TARGET = PostgresHook(postgres_conn_id='postgres_source')
        conn_source = PG_SOURCE.get_conn()
        conn_target = PG_TARGET.get_conn()
        cursor_source = conn_source.cursor()
        cursor_target = conn_target.cursor()
        tables = kwargs['ti'].xcom_pull(task_ids='get_tables')
        print(tables)
        for table in tables:
            line = []
            query = f"select column_name, data_type, character_maximum_length" \
                    f" from information_schema.columns where table_name='{table}'"
            cursor_source.execute(query)
            columns = cursor_source.fetchall()
            print(columns)
            for column in columns:
                if column[2]:
                    line.append(str(column[0]) + ' ' + str(column[1]) + '(' + str(column[2]) + ')')
                else:
                    line.append(str(column[0]) + ' ' + str(column[1]))
            table_columns = ', '.join(line)
            create_query = f"create table if not exists {table} ({table_columns});"
            cursor_target.execute(create_query)
            conn_target.commit()

    def dump_tables(**kwargs):
        pg_source = PostgresHook(postgres_conn_id='postgres_target')
        conn_source = pg_source.get_conn()
        cursor_source = conn_source.cursor()
        tables = kwargs['ti'].xcom_pull(task_ids='get_tables')
        for table in tables:
            query = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table}.csv', 'w') as f:
                cursor_source.copy_expert(query, f)

    def load_tables(**kwargs):
        pg_target = PostgresHook(postgres_conn_id='postgres_source')
        conn_target = pg_target.get_conn()
        cursor_target = conn_target.cursor()
        tables = kwargs['ti'].xcom_pull(task_ids='get_tables')
        for table in tables:
            query = f"COPY {table} TO STDIN WITH DELIMITER ',' CSV HEADER;"
            with open(f'{table}.csv', 'r') as f:
                cursor_target.copy_expert(query, f)

    get_tables = PythonOperator(
        task_id='get_tables',
        python_callable=get_tables,
        provide_context=True,
        dag=dag
    )
    get_source_table_schema = PythonOperator(
        task_id='get_source_schema',
        python_callable=get_source_schema,
        provide_context=True,
        dag=dag
    )

    dump_tables = PythonOperator(
        task_id='dump_tables',
        python_callable=dump_tables,
        provide_context=True,
        dag=dag
    )

    load_tables = PythonOperator(
        task_id='load_tables',
        python_callable=load_tables,
        provide_context=True,
        dag=dag
    )

    get_tables >> get_source_table_schema >> dump_tables >> load_tables
