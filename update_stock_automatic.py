import FinanceDataReader as fdr
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

def get_analysis_data():
    # 365일(1년) 데이터를 가져오도록 설정
    days_to_load = 365
    print(f"🚀 1,500개 종목 1년치({days_to_load}일) 데이터 분석 시작...")
    
    df_krx = fdr.StockListing('KRX')
    target_col = next((col for col in df_krx.columns if col.lower() == 'marcap'), None)
    top_1500 = df_krx.sort_values(by=target_col, ascending=False).head(1500)
    
    available_cols = [c for c in ['Code', 'Name', 'Market', 'Sector', 'PER', 'PBR'] if c in top_1500.columns]
    results = []
    
    for _, row in top_1500.iterrows():
        code, name = row['Code'], row['Name']
        # 1년치 데이터 수집
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=days_to_load)).strftime('%Y-%m-%d'))
        
        # 데이터가 충분하지 않으면 제외 (최소 1년치 영업일 약 240일 고려)
        if df.empty or len(df) < 100: continue
        
        # 기술적 지표 계산
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0; down[down > 0] = 0
        df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13).mean() / down.abs().ewm(com=13).mean())))
        
        std = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['SMA20'] + (std * 2)
        df['BB_Lower'] = df['SMA20'] - (std * 2)
        
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
        
        # [최신가 보정] 가장 마지막 행(가장 최신 날짜)을 추출
        last_row = df.iloc[-1].copy()
        last_row['Date'] = df.index[-1].strftime('%Y-%m-%d') # 실제 데이터 날짜 기록
        last_row['Code'], last_row['Name'] = code, name
        
        for col in available_cols:
            if col not in ['Code', 'Name']: last_row[col] = row[col]
        results.append(last_row)
        
    return pd.DataFrame(results)

def upload_via_gas(file_path, file_name):
    url = os.environ.get('GAS_WEBAPP_URL')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')
    with open(file_path, "r", encoding='utf-8-sig') as f:
        content = f.read()
    data = {"fileName": file_name, "fileContent": content, "folderId": folder_id}
    requests.post(url, data=json.dumps(data))

if __name__ == "__main__":
    final_df = get_analysis_data()
    final_df = final_df.sort_values(by='RSI', ascending=True)
    
    # 1. 전체 리포트 업로드
    full_file = "analysis_full.csv"
    final_df.to_csv(full_file, index=False, encoding='utf-8-sig')
    upload_via_gas(full_file, f"stock_full_{datetime.now().strftime('%Y%m%d')}.csv")
    
    # 2. 타겟 종목 업로드
    candidates = final_df[(final_df['RSI'] <= 35) | (final_df['MACD_Hist'] > 0)]
    candidate_file = "target_candidates.csv"
    candidates.to_csv(candidate_file, index=False, encoding='utf-8-sig')
    upload_via_gas(candidate_file, f"target_candidates_{datetime.now().strftime('%Y%m%d')}.csv")
