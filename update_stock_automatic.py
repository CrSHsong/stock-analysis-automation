import FinanceDataReader as fdr
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

def get_analysis_data():
    print("종목 분석 시작...")
    df_krx = fdr.StockListing('KRX')
    
    # [수정] 대소문자 상관없이 시가총액 컬럼(marcap)을 찾습니다.
    target_col = None
    for col in df_krx.columns:
        if col.lower() == 'marcap':
            target_col = col
            break
            
    if target_col:
        print(f"찾은 시가총액 컬럼명: {target_col}")
        top_1000 = df_krx.sort_values(by=target_col, ascending=False).head(1000)
    else:
        print(f"현재 컬럼 목록: {df_krx.columns.tolist()}")
        raise KeyError("시가총액 컬럼을 찾을 수 없습니다. (Marcap 확인 필요)")
    
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
