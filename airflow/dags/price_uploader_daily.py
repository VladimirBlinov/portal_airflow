from datetime import datetime, timedelta
import logging
import os
import timeit

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from etlutils.instruments_helper import get_daily_data, list_to_csv_as_row, get_extracted

DEFAULT_ARGS = {
    'owner': 'airflow',
}

DAG_ID = 'price_uploader_daily_dag'
TAG = 'price_uploader_daily'
TEMP_DIR = '/tmp'
WORK_DIR = os.path.join(TEMP_DIR, DAG_ID)
if not os.path.exists(WORK_DIR):
    os.mkdir(WORK_DIR)
DATE_FORMAT = "%Y%m%d"

with DAG(
        DAG_ID,
        default_args=DEFAULT_ARGS,
        schedule_interval='@daily',
        start_date=datetime(2021, 12, 27),
        catchup=True,
        tags=[TAG]) as dag:

    def extract(**kwargs):
        logging.info('start')
        logging.info(f'{kwargs}')
        execution_date = kwargs['data_interval_start']
        logging.info(f'execution_date: {execution_date}')
        logging.info(f'execution_date: {execution_date.strftime(DATE_FORMAT)}')
        extracted_file_path = os.path.join(WORK_DIR, f'extracted_{DAG_ID}_{execution_date.strftime(DATE_FORMAT)}.csv')
        logging.info(f'extracted_file_path: {extracted_file_path}')
        open(extracted_file_path, 'w').close()
        start_time = execution_date
        end_time = start_time + timedelta(days=1)
        logging.info(f'start_time, end_time: {start_time} {end_time}')
        pg_hook = PostgresHook('airflow_database')
        sql = """SELECT "InstrumentID", "Instrument", "Ticker", "MarketplaceID" FROM public."Instrument";"""
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    current_row = 0
                    total_rows = cursor.rowcount
                    if cursor.rowcount > 0:
                        for row in cursor:
                            current_row += 1
                            logging.info(f'extracting {current_row} of {total_rows}')
                            try:
                                start = timeit.timeit()
                                instrument_data = get_daily_data(row, start_time, end_time)
                                if instrument_data:
                                    list_to_csv_as_row(extracted_file_path, instrument_data)
                                logging.info(f'execution time: {(timeit.timeit() - start):.2f}')
                            except Exception as e:
                                logging.info(f'Exception: {e}')
                except Exception as e:
                    logging.info(f'Exception: {e}')
        ti = kwargs['ti']
        ti.xcom_push(value=extracted_file_path, key='extracted_file_path')
        ti.xcom_push(value=execution_date.strftime(DATE_FORMAT), key='execution_date')


    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract,
        provide_context=True
    )


    def load(**kwargs):
        ti = kwargs['ti']
        extracted_file_path = ti.xcom_pull(key='extracted_file_path', task_ids='extract_task')
        execution_date = ti.xcom_pull(key='execution_date', task_ids='extract_task')
        logging.info(f'execution_date: {execution_date}')
        logging.info(f'extracted_file_path: {extracted_file_path}')
        extracted_df = get_extracted(extracted_file_path)
        if not extracted_df.empty:
            for idx in range(extracted_df.shape[0]):
                instrument_id, open, high, low, close, volume, date_time, timeframe_id, marketplace = extracted_df.iloc[idx,
                                                                                                      :].tolist()
                instrument_id = int(instrument_id)
                volume = int(volume)
                date_time = datetime.strptime(str(int(date_time)), DATE_FORMAT)
                timeframe_id = int(timeframe_id)
                marketplace = int(marketplace)
                pg_hook = PostgresHook('airflow_database')
                with pg_hook.get_conn() as conn:
                    with conn.cursor() as cursor:
                        sql = """SELECT EXISTS (SELECT "InstrumentPriceID" FROM public."InstrumentPrice"
                                                WHERE "InstrumentID" = %s AND "DateTime" = %s AND "TimeFrameID" = %s 
                                                AND "MarketPlaceID" = %s);"""
                        params = (instrument_id, date_time, timeframe_id, marketplace)
                        try:
                            cursor.execute(sql, params)
                            instrument_exists = cursor.fetchone()[0]
                        except Exception as e:
                            logging.info(f'Exception: {e}')
                            instrument_exists = None
                        logging.info(f'{*params,} Already exist: {instrument_exists}')

                        if instrument_exists:
                            sql_update = """UPDATE public."InstrumentPrice"
                                            SET "Open"=%s, "High"=%s, "Low"=%s, "Close"=%s, "Volume"=%s
                                            WHERE "InstrumentID"=%s AND "DateTime"=%s AND "TimeFrameID"=%s 
                                            AND "MarketPlaceID"=%s ;"""
                            params_update = (open, high, low, close, volume, instrument_id, date_time,
                                             timeframe_id, marketplace)
                            try:
                                cursor.execute(sql_update, params_update)
                                logging.info(f'{*params,} updated')
                            except Exception as e:
                                logging.info(f'Exception: {e}')
                        else:
                            sql_insert = """INSERT INTO public."InstrumentPrice"(
                                "InstrumentID", "Open", "High", "Low", "Close", "Volume", "DateTime",
                                "TimeFrameID", "MarketPlaceID")
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                            params_insert = (instrument_id, open, high, low, close, volume,  date_time, timeframe_id,
                                             marketplace)
                            try:
                                cursor.execute(sql_insert, params_insert)
                                logging.info(f'{*params,} inserted')
                            except Exception as e:
                                logging.info(f'Exception: {e}')
        else:
            logging.info('Extracted file is empty')


    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        provide_context=True
    )

    extract_task >> load_task
