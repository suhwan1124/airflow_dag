from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pyarrow 
import pandas as pd
import boto3
import io

# RDS에서 데이터 추출 함수
def extract_from_rds():
    # Airflow의 PostgresHook을 사용해 데이터베이스에 연결
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = "SELECT * FROM accounts_profile LIMIT 1;"
    print(query)
    # 데이터베이스에서 쿼리 실행하여 데이터프레임으로 저장
    df = pd.read_sql(query, conn)
    cursor.close()
    conn.close()
    
    # Parquet 형식으로 변환하여 반환
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)  # S3에 업로드하기 위해 버퍼의 시작 위치로 이동
    return parquet_buffer.getvalue()

# S3에 데이터 업로드 함수
def load_to_s3(parquet_data):
    s3 = boto3.client('s3')
    
    # 현재 날짜를 기준으로 디렉토리 형식의 Key 생성
    date_str = datetime.now().strftime("%Y/%m/%d")
    s3_key = f"data/{date_str}/your_data.parquet"
    
    try:
        s3.put_object(
            Bucket="suhwan-datalake-s3",
            Key=s3_key,
            Body=parquet_data
        )
        print(f"Upload successful: {s3_key}")
    except Exception as e:
        print(f"S3 upload error: {e}")

# Airflow DAG 정의
with DAG(
    dag_id='rds_to_s3_etl_with_directory_structure',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # RDS에서 데이터 추출 태스크
    extract_task = PythonOperator(
        task_id='extract_from_rds',
        python_callable=extract_from_rds
    )

    # S3로 데이터 로드 태스크
    load_task = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='extract_from_rds') }}"]
    )

    # 태스크 순서 설정
    extract_task >> load_task


