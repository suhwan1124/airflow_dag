from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pyarrow as pa
import io
import pandas as pd
import logging



# RDS 연결 테스트 함수
def test_rds_connection():
    try:
        # RDS 연결 코드
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        logging.info(f"RDS 연결 테스트 결과: {result}")
        
        # 샘플 데이터프레임 생성
        df = pd.DataFrame({
            'column1': [1, 2, 3],
            'column2': ['a', 'b', 'c']
        })

        # Parquet 변환 코드
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            logging.info("Parquet 변환 성공")
        except Exception as e:
            logging.error(f"Parquet 변환 실패: {e}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"RDS 연결 실패: {e}")

# Airflow DAG 정의
with DAG(
    dag_id='test_rds_connection',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # 수동으로 실행
    catchup=False
) as dag:
    
    # RDS 연결 테스트 태스크
    connection_test_task = PythonOperator(
        task_id='test_rds_connection',
        python_callable=test_rds_connection
    )