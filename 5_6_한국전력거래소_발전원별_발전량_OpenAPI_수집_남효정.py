import threading
import time
import requests
import json
import pandas as pd
import cx_Oracle

# 공공데이터포털 사이트에서 '한국전력거래소_발전원별 발전량(계통기준)' OPEN API를 사용한다.

# 검증된 URL
# http://apis.data.go.kr/B552115/PwrAmountByGen/getPwrAmountByGen?serviceKey=

# OpenAPI 액세스 키
# access_key = 'Yff2okQ2i8jEUaDanPqgbSuRzoFOaP0AfYzU7Mkhe4QP9tapdiZiOvmhk%2BZeOrBMrxJqbxU6M58b1OeuM%2FOKsg%3D%3D'
access_key='Yff2okQ2i8jEUaDanPqgbSuRzoFOaP0AfYzU7Mkhe4QP9tapdiZiOvmhk+ZeOrBMrxJqbxU6M58b1OeuM/OKsg=='

# 파라메터 세팅
# params ={키1:값1, .... , 키N:값N  }

# response = requests.get(url, params=params)
# print(response.content)

def get_request_url(base_date):
    url = 'http://apis.data.go.kr/B552115/PwrAmountByGen/getPwrAmountByGen'
    params = {'serviceKey': access_key ,
              'numOfRows' : 300,
              'pageNo' : 1,
              'dataType': 'JSON',
              'baseDate': base_date
    }
    response = requests.get(url, params=params)
    # response.content # => response.content는 한글이 인코딩된 형식이므로
    #                       response.text 를 응답받기로함
    return response.text



def preprocessed_df_to_oracle(df):
    conn = cx_Oracle.connect('open_source/1111@192.168.0.4:1521/xe')
    cur = conn.cursor()

    # 중복 여부 검사 및 삽입
    for index, row in df.iterrows():
        baseDatetime = row['baseDatetime']
        check_query = "SELECT COUNT(*) FROM power_amounts WHERE baseDatetime = :1"
        cur.execute(check_query, (baseDatetime,))
        count = cur.fetchone()[0]

    if count == 0:
        # 중복되지 않으면 데이터 삽입
        insert_query = '''
            INSERT INTO power_amounts (
                baseDatetime, fuelPwr1, fuelPwr2, fuelPwr3, fuelPwr4, fuelPwr5,
                fuelPwr6,fuelPwr7,fuelPwr8,fuelPwr9,fuelPwrTot)
            VALUES (
                :baseDatetime,:fuelPwr1,:fuelPwr2,:fuelPwr3,:fuelPwr4,:fuelPwr5,
                :fuelPwr6,:fuelPwr7,:fuelPwr8,:fuelPwr9,:fuelPwrTot)
        '''
        cur.execute(insert_query, {
            'baseDatetime': row['baseDatetime'],
            'fuelPwr1': row['fuelPwr1'],
            'fuelPwr2': row['fuelPwr2'],
            'fuelPwr3': row['fuelPwr3'],
            'fuelPwr4': row['fuelPwr4'],
            'fuelPwr5': row['fuelPwr5'],
            'fuelPwr6': row['fuelPwr6'],
            'fuelPwr7': row['fuelPwr7'],
            'fuelPwr8': row['fuelPwr8'],
            'fuelPwr9': row['fuelPwr9'],
            'fuelPwrTot': row['fuelPwrTot']
        })
        conn.commit()
        print("Data inserted successfully for baseDatetime:", baseDatetime)
    else:
        print("Data already exists for baseDatetime:", baseDatetime)

    # {'baseDatetime': baseDate, 'fuelPwr1': 수력, 'fuelPwr2': 유류, 'fuelPwr3': 유연탄,
    #  'fuelPwr4': 원자력, 'fuelPwr5': 양수, 'fuelPwr6': 가스,
    #  #              'fuelPwr7': 국내탄, 'fuelPwr8': 신재생, 'fuelPwr9': 태양광, 'fuelPwrTot': 총량})

    cur.close()
    conn.close()

def power_amount_info_collector():
    # 현재 시간 설정
    base_date = time.strftime("%Y%m%d")
    # print(baseDate)

    raw_str_json = get_request_url(base_date)
    # print(raw_str_json)

    if raw_str_json:
        raw_json = json.loads(raw_str_json)
        df = pd.DataFrame(raw_json['response']['body']['items']['item'])
        df = df[sorted(df.columns)]
        print(df)

        preprocessed_df_to_oracle(df)

def power_amount_info_scheduler():
    print('\n발전량정보 수집기 스케줄러 동작\n')
    while True:
        power_amount_info_collector()
        print("수집완료 ")
        time.sleep(300) # 1시간 주기로 데이터 수집


print('<한국전력거래소_발전원별_발전량 데이터수집 스케줄러 ver1.0 >')

def print_main_menu():
    print('\n1. 한국전력거래소_발전원별_발전량 실시간 데이터 수집')
    # print('2. 업데이트 예정')
    print('3. 스케줄러 종료')
    print('* 엔터: 메뉴 업데이트\n')
while True:
    print_main_menu()
    print('메뉴입력: ', end='')
    selection = input()
    if selection == '':  continue
    else:                menu_num = int(selection)

    if(menu_num == 1):
        t = threading.Thread(target=power_amount_info_scheduler, daemon=True)
        t.start()
    # elif(menu_num == 2):
    #     print('업데이트 예정중입니다.')
    elif(menu_num == 3):
        break
    elif (menu_num == 0):
        continue
