import FinanceDataReader as fdr
import pandas as pd
import requests
import base64
import os
from datetime import datetime, timedelta

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
        
    return final_df
    
def upload_via_gas(file_path):
    print("GAS를 통해 드라이브 업로드 시도 중...")
    url = os.environ.get('GAS_WEBAPP_URL')
    
    with open(file_path, "rb") as f:
        content = f.read().decode('utf-8-sig')
    
    # 데이터를 JSON 형태로 전송
    data = {
        "fileName": f"stock_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        "fileContent": content
    }
    
    response = requests.post(url, json=data)
    print(f"결과: {response.text}")

if __name__ == "__main__":
    df = get_analysis_data()
    csv_file = "analysis_result.csv"
    df.to_csv(csv_file, index=True, encoding='utf-8-sig')
    upload_via_gas(csv_file)


