# import cx_Oracle
# import pandas as pd
# import matplotlib.pyplot as plt
# from matplotlib.animation import FuncAnimation
# import matplotlib.dates as mdates
# from matplotlib import font_manager,rc
# import numpy as np
# import seaborn as sns
#
#
# font_location = "C:\Windows\Fonts\malgun.ttf"
# font_name = font_manager.FontProperties(fname=font_location).get_name()
# rc('font',family=font_name)
#
# df = pd.read_csv('data_test.csv')
#
# df['baseDatetime'] = pd.to_datetime(df['baseDatetime'], format='%Y%m%d%H%M%S')
# df['시간'] = df['baseDatetime'].dt.hour
#
# df = df.set_index('시간') # 파이차트 용 인덱스
# df = df.rename(columns={'fuelPwr1': '수력', 'fuelPwr2': '유류','fuelPwr3':'유연탄','fuelPwr4':'원자력','fuelPwr5':'양수','fuelPwr6':'가스','fuelPwr7':'국내탄','fuelPwr8':'신재생','fuelPwr9':'태양광','fuelPwrTot':'합계','rn':'순서'})
# def calculate_proportions(row):
#     total = row['합계']
#     return {col: (row[col] / total) * 100 for col in df.columns if col != 'baseDatetime' and col != '합계' and col !='순서'}
#
#
#
# def update(frame):
#
#     proportions = df.apply(calculate_proportions, axis=1)
#     proportions_df = pd.DataFrame(list(proportions))
#     proportions_df.index = df.index
#
#     ax.clear()
#     ax.pie(
#         proportions_df.iloc[frame],
#         labels=proportions_df.columns,
#         autopct='%1.1f%%',
#         startangle=140
#     )
#     ax.set_title(f'발전원 별 비중 {df["baseDatetime"].iloc[frame]}')
#
#
# fig, ax = plt.subplots(figsize=(10 ,10))
#
#
# ani = FuncAnimation(
#     fig,
#     update,
#     frames=len(df),
#     interval= 100  # 1초마다 5분 단위로 업데이트
# )
# plt.show()
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib import font_manager, rc

# 한글 폰트 설정
font_location = "C:/Windows/Fonts/malgun.ttf"
font_name = font_manager.FontProperties(fname=font_location).get_name()
rc('font', family=font_name)

# 데이터 로드 및 전처리
df = pd.read_csv('data_test.csv')
df['baseDatetime'] = pd.to_datetime(df['baseDatetime'], format='%Y%m%d%H%M%S')
df['시간'] = df['baseDatetime'].dt.hour
df = df.set_index('시간')
df = df.rename(columns={
    'fuelPwr1': '수력', 'fuelPwr2': '유류', 'fuelPwr3': '유연탄',
    'fuelPwr4': '원자력', 'fuelPwr5': '양수', 'fuelPwr6': '가스',
    'fuelPwr7': '국내탄', 'fuelPwr8': '신재생', 'fuelPwr9': '태양광',
    'fuelPwrTot': '합계', 'rn': '순서'
})

def calculate_proportions(row):
    total = row['합계']
    return {col: (row[col] / total) * 100 for col in df.columns if col not in ['baseDatetime', '합계', '순서']}

# 프로포션 계산 및 데이터프레임 생성
proportions = df.apply(calculate_proportions, axis=1)
proportions_df = pd.DataFrame(proportions.tolist(), index=df.index)
proportions_df.columns = [col for col in df.columns if col not in ['baseDatetime', '합계', '순서']]

def update(frame):
    ax.clear()
    data = proportions_df.iloc[frame]
    ax.pie(
        data,
        labels=data.index,
        autopct='%1.1f%%',
        startangle=140
    )
    ax.set_title(f'발전원 별 비중 {df["baseDatetime"].iloc[frame]}')

# 애니메이션 설정
fig, ax = plt.subplots(figsize=(10, 10))
ani = FuncAnimation(fig, update, frames=len(df), interval=1000)  # 1초마다 업데이트

plt.show()