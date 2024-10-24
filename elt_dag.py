from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_hw7')
    
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(cur, table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    
    try:
        cur.execute("BEGIN;")
        
        # CTAS
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # primary key uniqueness check
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
            
        # duplicate records check
        sql = f"SELECT COUNT(*) FROM {table} GROUP BY userId, sessionID, channel, ts HAVING COUNT(*) > 1;"
        cur.execute(sql)
        duplicate_records = cur.fetchall()
        if len(duplicate_records) > 0:
            print("!!!!!!!!!!!!!!")
            raise Exception(f"Duplicate records detected.")
        
        print("CTAS successfully executed. Passed checks for primary key uniqueness and duplicate records.")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise


with DAG(
    dag_id = 'elt_CTAS',
    start_date = datetime(2024,10,23),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:
    cur = return_snowflake_conn()
    table = "hw7.analytics.session_summary"
    select_sql = """SELECT u.*, s.ts
                    FROM hw7.raw_data.user_session_channel u
                    JOIN hw7.raw_data.session_timestamp s ON u.sessionId=s.sessionId"""

    run_ctas(cur, table, select_sql, primary_key='sessionId')