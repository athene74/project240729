import threading
import time
# 목적: 파이썬 코드를 활용하여 OpenAPI 호출 자동화
# Step1: OpenAPI를 제공하는 사이트에서 제공하는 샘플 프로그램을 확보한다.

import requests
import time
import json
import pandas as pd
import cx_Oracle
base_date = int(time.strftime("%Y%m%d"))

# access_key =
access_key='FkaKMCtafz+d9EsoSI707Bd3bkXhQ08xozVQoHyVLB2j97qqLzn4VFarSthh65PmeQ55uYVCm6fbIQ80UrKz6w=='

def get_request_url(base_date):
    url = 'http://apis.data.go.kr/B552115/PwrAmountByGen/getPwrAmountByGen'
    params = {'serviceKey': access_key , 'numOfRows' : 300, 'pageNo' : 1, # 하루에 나오는 총 데이터의 개수는 288개 이므로 300은 충분함.
        'dataType': 'JSON','baseDate':base_date
    }
    response = requests.get(url, params=params)
    # response.content # => response.content는 한글이 인코딩된 형식이므로
    #                       response.text 를 응답받기로함
    return response.text

raw_str = get_request_url(base_date)

raw_json = json.loads(raw_str)

parsed_json = raw_json['response']['body']['items']['item']

df = pd.DataFrame(parsed_json)
df = df[sorted(df.columns)]


def preprocessed_df_to_oracle(df):
    conn = cx_Oracle.connect('open_source/1111@192.168.0.18:1521/xe')
    cur = conn.cursor()
    table_name = 'power_amounts'

    # 테이블 존재 여부 확인
    check_table_query = f"""
    SELECT COUNT(*)
    FROM all_tables
    WHERE table_name = UPPER('{table_name}')
    """
    cur.execute(check_table_query)
    table_exists = cur.fetchone()[0] > 0

    if not table_exists:
        create_table_query = f"""
        CREATE TABLE {table_name} (
            baseDatetime TIMESTAMP PRIMARY KEY,
            fuelPwr1 NUMBER,
            fuelPwr2 NUMBER,
            fuelPwr3 NUMBER,
            fuelPwr4 NUMBER,
            fuelPwr5 NUMBER,
            fuelPwr6 NUMBER,
            fuelPwr7 NUMBER,
            fuelPwr8 NUMBER,
            fuelPwr9 NUMBER,
            fuelPwrTot NUMBER
        )
        """
        cur.execute(create_table_query)

    # 데이터프레임의 각 행을 삽입 또는 업데이트
    for index, row in df.iterrows():
        baseDatetime = row['baseDatetime']
        fuelPwr1 = row['fuelPwr1']
        fuelPwr2 = row['fuelPwr2']
        fuelPwr3 = row['fuelPwr3']
        fuelPwr4 = row['fuelPwr4']
        fuelPwr5 = row['fuelPwr5']
        fuelPwr6 = row['fuelPwr6']
        fuelPwr7 = row['fuelPwr7']
        fuelPwr8 = row['fuelPwr8']
        fuelPwr9 = row['fuelPwr9']
        fuelPwrTot = row['fuelPwrTot']

        # 업데이트
        update_query = f"""
        UPDATE {table_name}
        SET fuelPwr1 = :fuelPwr1,
            fuelPwr2 = :fuelPwr2,
            fuelPwr3 = :fuelPwr3,
            fuelPwr4 = :fuelPwr4,
            fuelPwr5 = :fuelPwr5,
            fuelPwr6 = :fuelPwr6,
            fuelPwr7 = :fuelPwr7,
            fuelPwr8 = :fuelPwr8,
            fuelPwr9 = :fuelPwr9,
            fuelPwrTot = :fuelPwrTot
        WHERE baseDatetime = :baseDatetime
        """
        cur.execute(update_query, {
            'baseDatetime': baseDatetime,
            'fuelPwr1': fuelPwr1,
            'fuelPwr2': fuelPwr2,
            'fuelPwr3': fuelPwr3,
            'fuelPwr4': fuelPwr4,
            'fuelPwr5': fuelPwr5,
            'fuelPwr6': fuelPwr6,
            'fuelPwr7': fuelPwr7,
            'fuelPwr8': fuelPwr8,
            'fuelPwr9': fuelPwr9,
            'fuelPwrTot': fuelPwrTot
        })

        # 삽입
        if cur.rowcount == 0:
            insert_query = f"""
            INSERT INTO {table_name} (baseDatetime, fuelPwr1, fuelPwr2, fuelPwr3, fuelPwr4, fuelPwr5, fuelPwr6,
                                      fuelPwr7, fuelPwr8, fuelPwr9, fuelPwrTot)
            VALUES (:baseDatetime, :fuelPwr1, :fuelPwr2, :fuelPwr3, :fuelPwr4, :fuelPwr5, :fuelPwr6,
                    :fuelPwr7, :fuelPwr8, :fuelPwr9, :fuelPwrTot)
            """
            cur.execute(insert_query, {
                'baseDatetime': baseDatetime,
                'fuelPwr1': fuelPwr1,
                'fuelPwr2': fuelPwr2,
                'fuelPwr3': fuelPwr3,
                'fuelPwr4': fuelPwr4,
                'fuelPwr5': fuelPwr5,
                'fuelPwr6': fuelPwr6,
                'fuelPwr7': fuelPwr7,
                'fuelPwr8': fuelPwr8,
                'fuelPwr9': fuelPwr9,
                'fuelPwrTot': fuelPwrTot
            })

    conn.commit()
    cur.close()
    conn.close()

def fuelPwr_collect_scheduler():
    print('\n발전원 별 발전량 수집기 스케줄러 동작\n')
    while True:
        raw_str = get_request_url(base_date)
        raw_json = json.loads(raw_str)
        parsed_json = raw_json['response']['body']['items']['item']
        df = pd.DataFrame(parsed_json)
        df = df[sorted(df.columns)]
        preprocessed_df_to_oracle(df)
        print("수집이 완료되었습니다.")
        time.sleep(300)  # 5분 주기로 데이터 수집

fuelPwr_collect_scheduler()