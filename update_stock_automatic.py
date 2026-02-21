import FinanceDataReader as fdr
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

def get_analysis_data():
    print("종목 분석 시작... (약 10~15분 소요)")
    df_krx = fdr.StockListing('KRX')
    
    # 대소문자 무관하게 시가총액 컬럼 찾기
    target_col = next((col for col in df_krx.columns if col.lower() == 'marcap'), None)
    if not target_col:
        raise KeyError("시가총액 컬럼을 찾을 수 없습니다.")
    
    top_1000 = df_krx.sort_values(by=target_col, ascending=False).head(1000)
    
    analysis_results = []
    for _, row in top_1000.iterrows():
        code, name = row['Code'], row['Name']
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d'))
        if df.empty: continue
        
        # 보조지표 계산
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0; down[down > 0] = 0
        df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13).mean() / down.abs().ewm(com=13).mean())))
        
        last_row = df.iloc[-1].copy()
        last_row['Code'], last_row['Name'] = code, name
        analysis_results.append(last_row)
        
    return pd.DataFrame(analysis_results)

def upload_to_drive(file_path):
    print("구글 드라이브 업로드 시도 중...")
    scope = ['https://www.googleapis.com/auth/drive']
    key_content = os.environ.get('GDRIVE_SERVICE_ACCOUNT_KEY')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')

    if not key_content or not folder_id:
        raise ValueError("깃허브 Secrets 설정(Key 또는 Folder ID)을 확인하세요.")
        
    key_dict = json.loads(key_content)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    
    gauth = GoogleAuth()
    gauth.credentials = creds
    drive = GoogleDrive(gauth)
    
    file_title = f"stock_analysis_{datetime.now().strftime('%Y%m%d')}.csv"
    
    # [수정] 개인용 드라이브에서 Quota 에러를 방지하는 가장 단순한 설정
    f = drive.CreateFile({
        'title': file_title, 
        'parents': [{'id': folder_id}]
    })
    f.SetContentFile(file_path)
    
    # [수정] 불필요한 param을 제거하여 충돌 방지
    f.Upload() 
    print(f"최종 성공: 구글 드라이브 업로드 완료! ({file_title})")

if __name__ == "__main__":
    final_df = get_analysis_data()
    csv_file = "analysis_result.csv"
    final_df.to_csv(csv_file, index=True, encoding='utf-8-sig')
    upload_to_drive(csv_file)
