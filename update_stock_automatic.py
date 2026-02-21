import FinanceDataReader as fdr
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

def get_analysis_data():
    print("🚀 1,500개 종목 정밀 분석 시작...")
    df_krx = fdr.StockListing('KRX')
    
    target_col = next((col for col in df_krx.columns if col.lower() == 'marcap'), None)
    top_1500 = df_krx.sort_values(by=target_col, ascending=False).head(1500)
    
    available_cols = [c for c in ['Code', 'Name', 'Market', 'Sector', 'PER', 'PBR'] if c in top_1500.columns]
    results = []
    
    for _, row in top_1500.iterrows():
        code, name = row['Code'], row['Name']
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=250)).strftime('%Y-%m-%d'))
        if df.empty or len(df) < 30: continue
        
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
        
        last_row = df.iloc[-1].copy()
        last_row['Code'], last_row['Name'] = code, name
        for col in available_cols:
            if col not in ['Code', 'Name']: last_row[col] = row[col]
        results.append(last_row)
        
    return pd.DataFrame(results)

def upload_via_gas(file_path, file_name):
    print(f"📡 {file_name} 전송 중...")
    url = os.environ.get('GAS_WEBAPP_URL')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')
    
    with open(file_path, "r", encoding='utf-8-sig') as f:
        content = f.read()
    
    data = {"fileName": file_name, "fileContent": content, "folderId": folder_id}
    response = requests.post(url, data=json.dumps(data))
    print(f"✅ 결과: {response.text}")

if __name__ == "__main__":
    final_df = get_analysis_data()
    final_df = final_df.sort_values(by='RSI', ascending=True) # RSI 낮은 순 정렬
    
    # 1. 전체 리포트 업로드
    full_file = "analysis_full.csv"
    final_df.to_csv(full_file, index=False, encoding='utf-8-sig')
    upload_via_gas(full_file, f"stock_full_{datetime.now().strftime('%Y%m%d')}.csv")
    
    # 2. 유망 종목(RSI 35 이하 또는 MACD 반전) 공략집 업로드
    candidates = final_df[(final_df['RSI'] <= 35) | (final_df['MACD_Hist'] > 0)]
    candidate_file = "target_candidates.csv"
    candidates.to_csv(candidate_file, index=False, encoding='utf-8-sig')
    upload_via_gas(candidate_file, f"target_candidates_{datetime.now().strftime('%Y%m%d')}.csv")
