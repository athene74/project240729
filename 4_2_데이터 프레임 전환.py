import pandas as pd
import requests
import json
import time

base_date = int(time.strftime("%Y%m%d")) - 1

access_key='FkaKMCtafz+d9EsoSI707Bd3bkXhQ08xozVQoHyVLB2j97qqLzn4VFarSthh65PmeQ55uYVCm6fbIQ80UrKz6w=='



def get_request_url(base_date):
    url = 'http://apis.data.go.kr/B552115/PwrAmountByGen/getPwrAmountByGen'
    params = {'serviceKey': access_key , 'numOfRows' : 300, 'pageNo' : 1, # 하루에 나오는 총 데이터의 개수는 288개 이므로 300은 충분함.
        'dataType': 'JSON','baseDate':base_date
    }
    response = requests.get(url, params=params)


    return response.text


# print(get_request_url(20240801))

raw_str = get_request_url(base_date)

raw_json = json.loads(raw_str)

parsed_json = raw_json['response']['body']['items']['item']

df = pd.DataFrame(parsed_json)
df = df[sorted(df.columns)]
df.to_csv('data_test.csv', index=False)
print(df)