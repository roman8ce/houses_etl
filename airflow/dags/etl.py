import os
import pandas as pd
import requests
import logging
import zipfile
from airflow import DAG
from airflow import settings
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable, Connection
from datetime import date
from urllib.parse import urlencode
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

DATA_PATH = Variable.get('AIRFLOW_DATA')
DATE_FORMAT = '%Y%m%d'
PUBLIC_KEY = Variable.get('PUBLIC_KEY')
SPARK_MASTER_URL = Variable.get('SPARK_MASTER_URL')
MINIO_ACCESS_KEY = Variable.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = Variable.get('MINIO_SECRET_KEY')
MINIO_BUCKET = Variable.get('MINIO_BUCKET')
MINIO_ENDPOINT = Variable.get('MINIO_ENDPOINT')
CLICKHOUSE_TABLE = Variable.get('CLICKHOUSE_TABLE')
SQL_QUERY = f"""
    SELECT address
    FROM {CLICKHOUSE_TABLE}
    WHERE square > 60
    ORDER BY square DESC
    LIMIT 25
"""

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'data_engineer',
    'poke_interval': 600,
    'catchup': False,
    'max_active_runs': 1,
    'schedule_interval': '0 0 * * *'
}

def remove_file(filepath):
    """Безопасно удаляет файл, игнорируя ошибку если файл не существует"""
    try:
        if os.path.exists(filepath):
            os.remove(filepath)
            logging.info(f'Файл {filepath} удален')
    except OSError as e:
        logging.warning(f'Ошибка при удалении {filepath}: {e}')

def extract_file(**context):
    """Загружает архив из публичного источника, извлекает файл и загружает его в S3-хранилище"""
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    final_url = base_url + urlencode(dict(public_key=PUBLIC_KEY))

    response = requests.get(final_url)
    if response.status_code == 200:
        try:
            download_url = response.json().get('href')
            if download_url:
                download_response = requests.get(download_url)

                archive_path = os.path.join(DATA_PATH, 'archive.zip')
                with open(archive_path, 'wb') as file:
                    file.write(download_response.content)
                    logging.info(f'Архив успешно загружен из источника')
            else:
                logging.warning('Ключ "href" отсутствует в ответе')
        except ValueError as e:
            logging.error(f'Невозможно декодировать JSON из ответа: {response.text}')
            raise
    else:
        raise AirflowException(f'Ошибка загрузки: {response.status_code}, {response.text}')
    
    with zipfile.ZipFile(archive_path, 'r') as zip_file:
        if not zip_file.namelist():
            raise ValueError('Архив пустой')
        extracted_file = zip_file.namelist()[0]
        zip_file.extract(extracted_file, DATA_PATH)
    extracted_path = os.path.join(DATA_PATH, extracted_file)
    remove_file(archive_path)
    logging.info(f'Файл {extracted_file} успешно извлечен из архива')

    raw_output = f'raw/{date.today().strftime(DATE_FORMAT)}/{extracted_file}'

    s3 = S3Hook(aws_conn_id='minio_conn')
    s3.load_file(
        filename=extracted_path,
        key=raw_output,
        bucket_name=MINIO_BUCKET,
        replace=True
    )
    context['ti'].xcom_push(value=raw_output, key='raw_output')
    logging.info(f'Файл {extracted_file} успешно сгружен в S3-хранилище')
    remove_file(extracted_path)

def transform_file(**context):
    """Очищает и преобразует данные с помощью Spark"""
    stg_input = f"s3a://{MINIO_BUCKET}/{context['ti'].xcom_pull(task_ids='extract_file', key='raw_output')}"

    try:
        spark = SparkSession.builder \
            .appName('transform_csv_from_s3') \
            .master(SPARK_MASTER_URL) \
            .config('spark.jars.packages', 
                    'org.apache.hadoop:hadoop-aws:3.3.4,'
                    'com.amazonaws:aws-java-sdk-bundle:1.12.262') \
            .config('spark.hadoop.fs.s3a.endpoint', f'http://{MINIO_ENDPOINT}') \
            .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY) \
            .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY) \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel('WARN')
        
        df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('multiLine', 'true') \
                .option('encoding', 'UTF-16') \
                .csv(stg_input)
        
        regex_1 = r'(18[0-9]{2}|19[0-9]{2}|200[0-9]|201[0-9]|202[0-5])'
        regex_2 = r'\d\d\.\d\d\.(\d\d)'
        regex_3 = r'9[5-9][0-9]'
        regex_4 = r'^[0-9]+(?:\.[0-9]+)?$'

        df = df\
            .withColumn(
                'maintenance_year',
                F.when(F.regexp_extract(F.col('maintenance_year'), regex_1, 0) != '', F.regexp_extract(F.col('maintenance_year'), regex_1, 0))
                    .when(
                        F.regexp_extract(F.col('maintenance_year'), regex_2, 0) != '',
                        F.when(
                            F.regexp_extract(F.col('maintenance_year'), regex_2, 1) < 25,
                            F.regexp_extract(F.col('maintenance_year'), regex_2, 1) + 2000
                        )
                            .otherwise(F.regexp_extract(F.col('maintenance_year'), regex_2, 1) + 1900)
                    )
                    .when(
                        F.regexp_extract(F.col('maintenance_year'), regex_3, 0) != '',
                        F.regexp_extract(F.col('maintenance_year'), regex_3, 0) + 1000
                    )
                    .cast(T.IntegerType())
            ) \
            .withColumn(
                'square', 
                F.when(F.col('square') == '—', F.lit(None)) \
                    .otherwise(F.regexp_replace('square', ' ', '')) \
                    .cast(T.DoubleType())
            ) \
            .withColumn(
                'population',
                F.when(F.col('population') == '—', F.lit(None)) \
                    .otherwise(F.regexp_replace('population', ' ', '')) \
                    .cast(T.IntegerType())
            ) \
            .withColumn(
                'communal_service_id',
                F.when(F.regexp_extract(F.col('communal_service_id'), regex_4, 0) != '', F.regexp_extract(F.col('communal_service_id'), regex_4, 0))
                    .cast(T.DoubleType())
            )

        df.printSchema()
        df = df.cache()
        
        logging.info('====================================================================')
        logging.info(f'Количество пустых строк: {df.filter(F.coalesce(*[F.col(column) for column in df.columns]).isNull()).count()}')
        
        logging.info('====================================================================')
        logging.info('Средний год постройки зданий:')
        df.select([F.round(F.mean('maintenance_year'), 2).alias('mean_year'), F.round(F.median('maintenance_year'), 2).alias('median_year')]) \
            .show(1, 0)
        
        logging.info('====================================================================')
        logging.info('Топ 10 городов/областей по количеству объектов:')
        df.groupBy('region') \
            .agg(F.count('*').alias('objects_count')) \
            .orderBy(F.desc('objects_count')) \
            .limit(10) \
            .show(10, 0)
        
        logging.info('====================================================================')
        logging.info('Здания с максимальной и минимальной площадью в рамках каждой области:')

        window_min = Window.partitionBy('region').orderBy(F.col('square').asc(), F.col('address').asc())
        window_max = Window.partitionBy('region').orderBy(F.col('square').desc(), F.col('address').asc())

        df_with_rn = df.where((F.col('region').isNotNull()) & (F.col('square').isNotNull())) \
            .withColumn('rn_min', F.row_number().over(window_min)) \
            .withColumn('rn_max', F.row_number().over(window_max))

        df_min = df_with_rn.where(F.col('rn_min') == 1) \
            .select(F.col('region'), F.col('address').alias('house_with_min_square'))

        df_max = df_with_rn.where(F.col('rn_max') == 1) \
            .select(F.col('region'), F.col('address').alias('house_with_max_square'))

        df_min.join(df_max, on='region', how='inner') \
            .orderBy('region') \
            .show(100, False)
        
        logging.info('====================================================================')
        logging.info('Количество зданий по десятилетиям:')
        df.withColumn(
                'decade',
                F.col('maintenance_year') - F.col('maintenance_year') % 10
            ) \
            .where(F.col('decade').isNotNull()) \
            .groupBy('decade') \
            .agg(F.count('*').alias('count_per_decade')) \
            .orderBy(F.col('decade')) \
            .show(200, 0)
        
        stg_output = f's3a://{MINIO_BUCKET}/stg/{date.today().strftime(DATE_FORMAT)}'
        context['ti'].xcom_push(value=stg_output, key='stg_output')

        df.unpersist()

        df.coalesce(1).write \
            .option('header', 'true') \
            .option('multiLine', 'true') \
            .option('encoding', 'UTF-8') \
            .csv(stg_output, mode='overwrite')
        
        logging.info('Очищенный файл успешно загружен в S3-хранилище')
    finally:
        spark.stop()

def load_file(**context):
    """Загружает очищенные данные из S3-хранилища в Clickhouse"""
    marts_input = context['ti'].xcom_pull(task_ids='transform_file', key='stg_output')
    try:
        spark = SparkSession.builder \
            .appName('load_csv_to_clickhouse') \
            .master(SPARK_MASTER_URL) \
            .config('spark.jars.packages', 
                    'org.apache.hadoop:hadoop-aws:3.3.4,'
                    'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
                    'com.clickhouse:clickhouse-jdbc:0.4.6') \
            .config('spark.hadoop.fs.s3a.endpoint', f'http://{MINIO_ENDPOINT}') \
            .config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY) \
            .config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY) \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel('WARN')
        
        df = spark.read \
                .option('header', 'true') \
                .option('inferSchema', 'true') \
                .option('multiLine', 'true') \
                .option('encoding', 'UTF-8') \
                .csv(marts_input)
        
        df.printSchema()
        
        clickhouse_options = {
            'url': 'jdbc:clickhouse://clickhouse:8123/houses',
            'driver': "com.clickhouse.jdbc.ClickHouseDriver",
            'user': 'default',
            'password': '',
            'dbtable': 'houses_rf'
        }

        df.write \
            .format('jdbc') \
            .mode('append') \
            .options(**clickhouse_options) \
            .save()
        
        logging.info('Данные успешно загружены в Clickhouse')
    finally:
        spark.stop()

def create_clickhouse_connection():
    """Создает соединение с Clickhouse в Airflow, если оно не существует"""
    conn_id = 'clickhouse_conn'
    
    with settings.Session() as session:
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if existing_conn is None:
            new_conn = Connection(
                conn_id=conn_id,
                conn_type='clickhouse',
                host='clickhouse',
                port=9000,
                login='default',
                schema='default'
            )
            session.add(new_conn)
            session.commit()
            logging.info(f'Создано соединение: {conn_id}')
        else:
            logging.info(f'Соединение {conn_id} уже существует')

def show_query_result(task_instance):
    """Выводит результат запроса к Clickhouse в логах"""
    query_result = [col for pack in task_instance.xcom_pull(task_ids='make_query') for col in pack]
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)
    if query_result:
        srs = pd.Series(query_result, index=range(1, 26), name='Топ 25 домов')
        logging.info('Результат запроса к ClickHouse:')
        logging.info(f'\n{srs}')
    else:
        logging.warning('Результаты запроса отсутствуют')
    
with DAG(
    dag_id='houses_etl',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    tags=['team_oknd', 'houses_rf']
) as dag:
    
    start = PythonOperator(
        task_id='start',
        python_callable = lambda: logging.info('DAG started')
    )

    extract = PythonOperator(
        task_id='extract_file',
        python_callable=extract_file
    )

    transform = PythonOperator(
        task_id='transform_file',
        python_callable=transform_file
    )

    load = PythonOperator(
        task_id='load_file',
        python_callable=load_file
    )

    create_connection = PythonOperator(
        task_id='create_clickhouse_connection',
        python_callable=create_clickhouse_connection,
    )

    make_query = ClickHouseOperator(
        task_id='make_query',
        sql=SQL_QUERY,
        clickhouse_conn_id='clickhouse_conn'
    )

    show_query = PythonOperator(
        task_id='show_query_result',
        python_callable = show_query_result
    )

    end = PythonOperator(
        task_id='end',
        python_callable = lambda: logging.info('DAG completed successfully')
    )

    start >> extract >> transform >> load >> create_connection >> make_query >> show_query >> end