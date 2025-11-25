from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# -----------------------------
# Snowflake connection
# -----------------------------
def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()

@task
def create_table_and_stage_and_copy():

    conn = get_snowflake_connection()
    cur = conn.cursor()
    sqls = [
        "CREATE SCHEMA IF NOT EXISTS RAW;",
        """
            CREATE TABLE IF NOT EXISTS RAW.user_session_channel (
            userId int not NULL,
            sessionId varchar(32) primary key,
            channel varchar(32) default 'direct'
        );
        """,
        """
            CREATE TABLE IF NOT EXISTS RAW.session_timestamp(
            sessionId varchar(32) primary key,
            ts timestamp
        );
        """,
        """
            CREATE OR REPLACE STAGE raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """,
        """
            COPY INTO raw.user_session_channel
            FROM @raw.blob_stage/user_session_channel.csv
            VALIDATION_MODE = RETURN_ERRORS;
        """,
        """
            COPY INTO raw.session_timestamp
            FROM @raw.blob_stage/session_timestamp.csv
            VALIDATION_MODE = RETURN_ERRORS;
        """
    ]
    try:
        cur.execute("BEGIN;")
        for sql in sqls:
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
@task
def copy_from_stage_into_tables():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute("DELETE FROM RAW.user_session_channel")
        cur.execute("DELETE FROM RAW.session_timestamp")
        cur.execute("""
            COPY INTO RAW.session_timestamp
            FROM @RAW.blob_stage/session_timestamp.csv
            FORCE=TRUE;
        """)
        cur.execute("""
            COPY INTO RAW.user_session_channel
            FROM @RAW.blob_stage/user_session_channel.csv
            FORCE=TRUE;
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
with DAG(
    dag_id="COPY_INTO_DATABASE_FROM_AWS",
    start_date=datetime(2025,9,25),
    catchup=False,
    tags=['ELT'],
    schedule_interval="20 8 * * *",
) as dag:
    create_table_and_stage_and_copy() >> copy_from_stage_into_tables()

