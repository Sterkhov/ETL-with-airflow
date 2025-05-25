from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pyodbc
import pandas as pd
import logging
from dotenv import load_dotenv
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_from_source(table_name):
    """
    Извлечение данных из MSSQL и загрузка в stage-таблицы DWH.
    Включает создание таблицы, очистку и массовую вставку.
    """
    load_dotenv()

    # Строка подключения к источнику MSSQL
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('MSSQL_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_NAME')};"
        f"UID={os.getenv('MSSQL_USER')};"
        f"PWD={os.getenv('MSSQL_PASSWORD')};"
        f"TrustServerCertificate=yes;"
    )

    try:
        conn = pyodbc.connect(conn_str)
        # Извлечение всех данных + текущая дата как дата загрузки
        query = f"SELECT *, GETDATE() AS load_date FROM dbo.{table_name}"
        df = pd.read_sql(query, conn)
        df["load_date"] = pd.to_datetime(df["load_date"]).dt.to_pydatetime()
        logging.info(f"table name - {table_name}, extracted rows - {len(df)}")

        cursor = conn.cursor()

        # DDL и insert для каждой таблицы в зависимости от названия
        if table_name == "Регистр_Продадж":
            cursor.execute(f"""DROP TABLE IF EXISTS dbo.stage_{table_name}];
                CREATE TABLE dbo.stage_{table_name}] (
                    [НоменклатураID] nvarchar(32) NOT NULL,
                    [ДокументID] nvarchar(32) NOT NULL,
                    [ВыручкаСУчетомСкидокБезНалогов] numeric(15, 2) NOT NULL,
                    [СуммаКНП2Доход] numeric(15, 2) NOT NULL,
                    [СтоимостьЗакупкиБезНДС] numeric(15, 2) NOT NULL,
                    [НеликвиднаяПродажа] varchar(2) NOT NULL,
                    [ДокументTип] varchar(36),
                    [ДокументВид] varchar(36),
                    [ОбластьДанныхОсновныеДанные] numeric(7, 0) NOT NULL,
                    [Активность] varchar(2) NOT NULL,
                    [load_date] datetime2(0)
                );
            """)
            insert_query = """INSERT INTO dbo.stage_{table_name} (...) VALUES (?, ?, ..., ?)"""  # обрезан для краткости

        elif table_name == "Справочники_Партнеры":
            cursor.execute(f"""DROP TABLE IF EXISTS dbo.stage_{table_name}];
                CREATE TABLE dbo.stage_{table_name}] (
                    ...
                );
            """)
            insert_query = """INSERT INTO dbo.stage_{table_name} (...) VALUES (?, ?, ..., ?)"""

        elif table_name == "Документы_Продажи":
            cursor.execute(f"""DROP TABLE IF EXISTS dbo.stage_{table_name}];
                CREATE TABLE dbo.stage_{table_name}] (
                    ...
                );
            """)
            insert_query = """INSERT INTO dbo.stage_{table_name} (...) VALUES (?, ?, ..., ?)"""

        elif table_name == "Справочники_Номенклатура":
            cursor.execute(f"""DROP TABLE IF EXISTS dbo.stage_{table_name}];
                CREATE TABLE dbo.stage_{table_name}] (
                    ...
                );
            """)
            insert_query = """INSERT INTO dbo.stage_{table_name} (...) VALUES (?, ?, ?)"""

        else:
            raise ValueError(f"Неизвестная таблица: {table_name}")

        # Вставка данных в stage
        records = [tuple(row) for row in df.itertuples(index=False, name=None)]
        cursor.executemany(insert_query, records)
        conn.commit()
        logging.info(f"Загружено в dbo.stage_{table_name}: {len(records)} строк.")

    except Exception as e:
        logging.error(f"Ошибка в extract_from_source для {table_name}: {str(e)}")
        raise

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'sterkov',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    'sales_etl',
    default_args=default_args,
    description='ETL для продаж в DWH',
    schedule='0 6 * * *',  # Каждый день в 6:00 по МСК
    catchup=False,
) as dag:

    # ==== Этап 1: Извлечение и загрузка данных в stage ====
    extract_sales = PythonOperator(
        task_id='extract_sales',
        python_callable=extract_from_source,
        op_kwargs={'table_name': 'Регистр_Продадж'},
    )

    extract_partners = PythonOperator(
        task_id='extract_partners',
        python_callable=extract_from_source,
        op_kwargs={'table_name': 'Справочники_Партнеры'},
    )

    extract_docs = PythonOperator(
        task_id='extract_docs',
        python_callable=extract_from_source,
        op_kwargs={'table_name': 'Документы_Продажи'},
    )

    extract_nomen = PythonOperator(
        task_id='extract_nomen',
        python_callable=extract_from_source,
        op_kwargs={'table_name': 'Справочники_Номенклатура'},
    )


    # ==== Этап 2: Загрузка в слой ODS ====
    create_ods_tables = SQLExecuteQueryOperator(
        task_id='create_ods_tables',
        conn_id=DWH_CONN,
        sql="/opt/airflow/sql/ods_tables_create.sql",
        database='source_db',
    )

    processing_ods_tables = SQLExecuteQueryOperator(
        task_id='processing_ods_tables',
        conn_id=DWH_CONN,
        sql="/opt/airflow/sql/ods_processing.sql",
        database='source_db',
    )

    # ==== Этап 3: Загрузка в витрины данных (Data Mart) ====
    create_fact_sales = SQLExecuteQueryOperator(
        task_id='create_fact_sales',
        conn_id=DWH_CONN_ID,
        sql="/opt/airflow/sql/fact_sales.sql",
        database='source_db',
    )

    create_share_shipments = SQLExecuteQueryOperator(
        task_id='create_share_shipments',
        conn_id=DWH_CONN_ID,
        sql="/opt/airflow/sql/share_shipments.sql",
        database='source_db',
    )

    # ==== Определение зависимостей между задачами ====
    [extract_sales, extract_partners, extract_docs, extract_nomen] \
        >> create_ods_tables \
        >> processing_ods_tables \
        >> create_fact_sales \
        >> create_share_shipments
