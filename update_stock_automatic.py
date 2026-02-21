import FinanceDataReader as fdr
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

# 1. 주식 데이터 수집 및 보조지표 계산
def get_analysis_data():
    print("종목 분석 시작...")
    df_krx = fdr.StockListing('KRX')
    
    # [수정 포인트] 시가총액 컬럼 이름이 'MarCap' 혹은 'MarketCap'인 경우를 모두 체크
    possible_cols = ['MarCap', 'MarketCap']
    target_col = next((col for col in possible_cols if col in df_krx.columns), None)
    
    if target_col:
        # 찾은 컬럼 이름으로 정렬
        top_1000 = df_krx.sort_values(by=target_col, ascending=False).head(1000)
    else:
        # 만약 둘 다 없다면 에러 방지를 위해 전체 컬럼 확인 로그 출력 후 중단
        print(f"현재 데이터 컬럼 목록: {df_krx.columns.tolist()}")
        raise KeyError("시가총액(MarCap/MarketCap) 컬럼을 찾을 수 없습니다.")
    
    analysis_results = []
    for _, row in top_1000.iterrows():
        code, name = row['Code'], row['Name']
        # 최근 100일치 데이터를 가져와 지표 계산
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d'))
        if df.empty: continue
        
        # 보조지표 계산
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        # RSI 계산
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0
        ema_up = up.ewm(com=13, adjust=False).mean()
        ema_down = down.abs().ewm(com=13, adjust=False).mean()
        df['RSI'] = 100 - (100 / (1 + (ema_up / ema_down)))
        
        # 마지막 날의 데이터만 추출
        last_row = df.iloc[-1].copy()
        last_row['Code'] = code
        last_row['Name'] = name
        analysis_results.append(last_row)
        
    return pd.DataFrame(analysis_results)

# 2. 구글 드라이브 업로드 함수
def upload_to_drive(file_path):
    scope = ['https://www.googleapis.com/auth/drive']
    # 깃허브 시크릿에서 가져온 키로 인증
    key_dict = json.loads(os.environ.get('GDRIVE_SERVICE_ACCOUNT_KEY'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    
    gauth = GoogleAuth()
    gauth.credentials = creds
    drive = GoogleDrive(gauth)
    
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')
    file_title = f"stock_analysis_{datetime.now().strftime('%Y%m%d')}.csv"
    
    f = drive.CreateFile({'title': file_title, 'parents': [{'id': folder_id}]})
    f.SetContentFile(file_path)
    f.Upload()
    print(f"구글 드라이브 업로드 완료: {file_title}")

if __name__ == "__main__":
    # 데이터 분석 실행
    final_df = get_analysis_data()
    
    # CSV 파일로 저장
    csv_file = "analysis_result.csv"
    final_df.to_csv(csv_file, index=True, encoding='utf-8-sig')
    
    # 구글 드라이브 업로드 실행
    upload_to_drive(csv_file)
