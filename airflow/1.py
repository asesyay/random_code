dag.py
from airflow import DAG
from nuke.airflow import NukeOperator
from app.clients.grpc.router_courier_gateway import CouriersClientV2
from couriers_loader.loader import run
from datetime import datetime, timedelta
import pendulum

DEFAULT_ARGS = {
    "owner": "SBLM",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 22),
    "email": ["agoptar@ozon.ru"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=24),
}


with DAG(
    dag_id="couriers_loader",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2025, 5, 21),
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
) as dag:
    loader_task = NukeOperator(
        task_id="test_task",
        python_callable=run,
        op_kwargs={
            "vertica_sblm_pvz": "{{ macros.connections.get_conn('sblm_team_vertica_pvz') }}"
        },
    ) 


    loader_task


loader.py
from app.clients.grpc.router_courier_gateway import CouriersClientV2

import nuke
#@nuke.inject
async def run(client: CouriersClientV2, **kwargs):
    from google.protobuf.json_format import MessageToDict
    from pb.gitlab.ozon.ru.router.router_courier_gateway.api.v2.couriers.couriers_pb2 import GetShortInfoByIDsV2Req
    import json
    import pandas as pd
    import sqlalchemy as sa
    from datetime import datetime, timedelta
    from couriers_loader.utils import log, VerticaLoader

    vertica_secrets = json.loads(json.dumps(json.loads(kwargs.get('vertica_sblm_pvz'))))

    vertica_username = vertica_secrets['login']
    vertica_password = vertica_secrets['password']
    
    engine_vertica = sa.create_engine(
        'vertica+vertica_python://{user}:{password}@{server}:{port}/{database}'.format(
            user=str(vertica_username),
            password=str(vertica_password),
            server=str('prodvertica.s.o3.ru'),
            port=str('5433'),
            database=str('OLAP')
        )
    )
    
    log("Start load courier ids from dwh_data")
    
    sql = f"""
    SELECT DISTINCT SourceKey
    FROM dwh_data.Anc_Courier cinfo 
    """
    
    df = pd.read_sql(sql, engine_vertica)
    lst_ids = df.SourceKey.to_list()
    
    log(f"Load {len(lst_ids)} ids from dwh_data")
    
    lst_out = []
    
    log("Start geting data from courier-app grpc")
    
    for i in range(0, len(lst_ids), 10000):
        payload = {
            "courier_ids": lst_ids[i:i+10000],
        }


        response = await client.GetShortInfoByIDsV2(
            GetShortInfoByIDsV2Req(
                **payload,
            )
        )

        response_dict = MessageToDict(response)
        lst_out += response_dict['list']
        
    log(f"Load {len(lst_out)} ids from courier-app grpc")
    
    df_out = pd.DataFrame(lst_out)
    df_out['type'] = df_out['type'].apply(lambda x: x['name'])
    df_out = df_out[['id', 'type', 'status', 'ozonId']]
    
    df_out['id'] = df_out['id'].astype(int)
    df_out['ozonId'] = df_out['ozonId'].fillna(0).astype(int)
    
    log(f"Start load data to OperationsLosses_team.couriers")
    
    loader_obj = VerticaLoader(engine=engine_vertica)

    loader_obj.load(
        table="couriers",
        schema="OperationsLosses_team",
        clear_out=True,
        df=df_out
    )


utils.py
import pandas as pd
import numpy as np
import sqlalchemy as sa
import re
from datetime import datetime


def log(*args):
    print(*args)


class VerticaLoader:

    def __init__(self, engine: sa.engine.base.Engine):
        self.engine = engine

    def __get_type_map(self, df):
        type_map = {
            np.dtype("i8"): sa.types.Integer(),
            pd.Int64Dtype(): sa.types.Integer(),
            np.dtype("f8"): sa.types.Float(),
            np.dtype("O"): sa.types.VARCHAR(1000),
            np.dtype("<M8[ns]"): sa.types.TIMESTAMP(),
            np.dtype("bool"): sa.BOOLEAN()
        }
        t_map = {
            name: type
            for (name, type) in zip(df.columns, [type_map[item] for item in df.dtypes])
        }
        return t_map

    def __ping_connection(self):
        try:
            with self.engine.connect() as con:
                con.execute(sa.text("SELECT 1;"))
            log("Vertica connection - OK")
        except Exception as e:
            log(f"Exception [{e}] was occurred. Trying to renew connection")
            self.engine.dispose()

    def __truncate_table(self, table, schema):
        ddl_query_truncate = f"TRUNCATE TABLE {schema}.{table}"
        with self.engine.connect() as con:
            log(f"Truncating table {schema}.{table}")
            con.execute(sa.text(ddl_query_truncate))

    def __analyze_statistics(self, table, schema):
        ddl_query_statistic = f"SELECT ANALYZE_STATISTICS('{schema}.{table}')"
        with self.engine.connect() as con:
            log(f"Analyze statistics {schema}.{table}")
            con.execute(sa.text(ddl_query_statistic))

    def __refresh(self, table, schema):
        ddl_query_refresh = f"SELECT REFRESH('{schema}.{table}')"
        with self.engine.connect() as con:
            log(f"Refreshing {schema}.{table}")
            con.execute(sa.text(ddl_query_refresh))

    def load(self, *, table, schema, df, clear_out=False, refresh=False, analyze_statistic=False):
        self.__ping_connection()
        log(f"Start loading data into the table {table}")
        start_time = datetime.now()

        if clear_out:
            self.__truncate_table(table, schema)

        types = self.__get_type_map(df)

        log(f"Loading data into table {schema}.{table}")
        df.to_sql(
            name=table,
            schema=schema,
            con=self.engine,
            if_exists="append",
            dtype=types,
            index=False,
            chunksize=100_000,
        )

        log("Process completed successfully")

        if refresh:
            self.__refresh(table, schema)
        if analyze_statistic:
            self.__analyze_statistics(table, schema)

        log(f"Full duration:{datetime.now() - start_time}")


def remake_global_postings(posting_name: str) -> str:
    entities = str(posting_name).split("-")
    if entities[0] == "pl":
        return "-".join(entities[2:5])
    elif "RP" in entities[0]:
        return "-".join(entities[1:])
    return posting_name


def fetch_ozon_id(posting_name: str) -> str:
    return str(posting_name).split("-")[0]

def get_postings_from_raw(text: str) -> list:
    return re.findall(r'\d+-\d+-\d+', text)


Мой код:
from app.clients.grpc.afr_api import AfrApiClient
from ia_functions.functions import describe_grpc_client, describe_grpc_method

import json
import pandas as pd
import sqlalchemy as sa
from datetime import datetime, timedelta

import vertica_python

connection_info = {
    'host': 'vertica-sandbox.s.o3.ru',  # Имя сервера или IP
    'port': 5433,                 # Обычно 5433
    'user': '',
    'password': '',
    'database': 'OLAP',
    'unicode_error': 'replace',   # Обработка некорректных символов
    'ssl': False                  # SSL (если нужно)
}

connection = vertica_python.connect(**connection_info)

sql_query = """
SELECT
    REGEXP_SUBSTR(t1.OrderNumber, '^[^-]+') as user_id,
    CAST(t2.ItemId AS VARCHAR) as ItemIdBefore,
    t2.Name as NameItem,
    t1.FirstPrice,
    t1.Price,
    t1.FirstPrice - t1.Price as PriceDifference,
    t1.ClientPostingId as ClientPostingID,
    t3.PostingNumber as PostingNumber,
    t2.DamageReason,
    t1.OrderDate::TIMESTAMP AS OrderDate
FROM
    dwh.Dim_ClientPostingItem as t1
JOIN
    dwh.Dim_ItemDiscounted as t2 ON t2.ItemDiscountedId = t1.ItemId
LEFT JOIN
    dwh_data.Atr_ClientPosting_PostingNumber as t3 ON t3.ClientPostingId = t1.ClientPostingId
WHERE
    TRUNC(t1.OrderDate) = CURRENT_DATE - INTERVAL '1 day';
    """
with vertica_python.connect(**connection_info) as connection:
    cur = connection.cursor()
    cur.execute(sql_query)
    rows = cur.fetchall()
    df_result_1 = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
df_result_1

### Пройдемся по полученным user_id grpc nuke
from pb.gitlab.ozon.ru.afr.afr_api.api.afr_api.afr_api_pb2 import V1GetUserMultiaccScoreVerboseReq
from google.protobuf.json_format import MessageToDict
import nuke
import asyncio

async def get_multiacc_score(client, user_id: int):
    payload = {"user_id": user_id}
    response = await client.V1GetUserMultiaccScoreVerbose(
        V1GetUserMultiaccScoreVerboseReq(**payload)
    )
    result = MessageToDict(response)
    result['original_user_id'] = user_id  # Добавляем original_user_id
    return result

@nuke.inject
async def main(client: AfrApiClient, user_ids: list[int]):
    results = []
    for user_id in user_ids:
        result = await get_multiacc_score(client, user_id)
        results.append(result)
    return results

user_list = df_result_1['user_id'].astype(int).to_list()

response_dict = await main(user_ids=user_list[:1000])
from collections import defaultdict
import pandas as pd

def extract_duplicates_data(response_dict):
    data = []
    for item in response_dict:
        try:
            original_user_id = item['original_user_id']
            duplicates_count = item.get('duplicatesCount', {})
            
            # Инициализация структур данных
            all_duplicate_user_ids = set()
            duplicates_by_type = defaultdict(list)
            
            # Обработка дубликатов с проверкой наличия ключей
            for dup in item.get('duplicates', []):
                # Проверяем наличие обязательных ключей
                if 'key' not in dup or 'userId' not in dup:
                    continue  # Пропускаем некорректные записи
                
                user_ids = dup['userId']
                # Фильтрация: удаляем original_user_id и пустые значения
                filtered_user_ids = [
                    str(uid) for uid in user_ids
                    if str(uid) != str(original_user_id) and uid is not None
                ]
                all_duplicate_user_ids.update(filtered_user_ids)
                duplicates_by_type[dup['key']].extend(filtered_user_ids)
            
            # Формирование записи
            record = {
                'original_user_id': original_user_id,
                'all_duplicate_user_ids': list(all_duplicate_user_ids),
                **{k: v for k, v in duplicates_count.items() if v}  # Исключаем пустые значения
            }
            
            # Добавляем user_ids по типам дубликатов
            for dup_type, user_ids in duplicates_by_type.items():
                record[f'{dup_type}_user_ids'] = list(set(user_ids))
            
            data.append(record)
            
        except Exception as e:
            print(f"Ошибка обработки элемента: {e}\nСам элемент: {item}")
            continue
    
    return pd.DataFrame(data)

df = extract_duplicates_data(response_dict)
import pandas as pd
import vertica_python
from itertools import chain

all_user_ids = set(chain.from_iterable(df['all_duplicate_user_ids'].dropna()))
user_ids_str = ",".join([f"'{str(uid)}'" for uid in all_user_ids])


sql_query = f"""
    SELECT 
        t1.*,
        '{original_user_id}' as original_user_id,
        REGEXP_SUBSTR(t1.OrderNumber, '^[^-]+') as current_user_id
    FROM dwh.Dim_ClientPostingItem t1
    WHERE REGEXP_SUBSTR(t1.OrderNumber, '^[^-]+') IN ({user_ids_str})  -- Используем прямое сравнение user_id
    """
    
with vertica_python.connect(**connection_info) as connection:
    cur = connection.cursor()
    cur.execute(sql_query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    # Добавляем результаты в общий DataFrame
    temp_df = pd.DataFrame(rows, columns=columns)
