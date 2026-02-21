import FinanceDataReader as fdr
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

def get_analysis_data():
    print("1. 종목 리스트 수집 시작...")
    df_krx = fdr.StockListing('KRX')
    
    # [오류 반영] MarCap, MarketCap, Marcap 등 대소문자 무관하게 시가총액 열 찾기
    target_col = next((col for col in df_krx.columns if col.lower() == 'marcap'), None)
            
    if target_col:
        print(f"계산 근거: '{target_col}' 컬럼을 기준으로 상위 1000개 선별")
        top_1000 = df_krx.sort_values(by=target_col, ascending=False).head(1000)
    else:
        raise KeyError("시가총액 컬럼을 찾을 수 없습니다. 데이터 형식을 확인하세요.")
    
    results = []
    for _, row in top_1000.iterrows():
        code, name = row['Code'], row['Name']
        # 최근 100일치 데이터를 가져와 보조지표 계산
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d'))
        if df.empty: continue
        
        # 이동평균선 및 RSI 계산
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0
        down[down > 0] = 0
        df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13).mean() / down.abs().ewm(com=13).mean())))
        
        last_row = df.iloc[-1].copy()
        last_row['Code'], last_row['Name'] = code, name
        results.append(last_row)
        
    return pd.DataFrame(results)

def upload_to_drive(file_path):
    scope = ['https://www.googleapis.com/auth/drive']
    key_content = os.environ.get('GDRIVE_SERVICE_ACCOUNT_KEY')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')

    if not key_content or not folder_id:
        raise ValueError("깃허브 Secrets에 Key 또는 Folder ID가 설정되지 않았습니다.")
        
    key_dict = json.loads(key_content)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    
    gauth = GoogleAuth()
    gauth.credentials = creds
    drive = GoogleDrive(gauth)
    
    file_title = f"stock_analysis_{datetime.now().strftime('%Y%m%d')}.csv"
    
    # [수정] 업로드 설정을 보강하여 403 에러 방지
    file_metadata = {
        'title': file_title, 
        'parents': [{'id': folder_id}]
    }
    
    f = drive.CreateFile(file_metadata)
    f.SetContentFile(file_path)
    
    # [수정] 공유 드라이브 및 할당량 관련 설정 추가
    f.Upload(param={'supportsAllDrives': True}) 
    print(f"최종 성공: 구글 드라이브 업로드 완료! ({file_title})")
