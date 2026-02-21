import FinanceDataReader as fdr
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

def get_analysis_data():
    print("종목 분석 시작... (약 10~15분 소요)")
    df_krx = fdr.StockListing('KRX')
    target_col = next((col for col in df_krx.columns if col.lower() == 'marcap'), None)
    
    if not target_col:
        raise KeyError("시가총액 컬럼을 찾을 수 없습니다.")
    
    top_1000 = df_krx.sort_values(by=target_col, ascending=False).head(1000)
    results = []
    
    for _, row in top_1000.iterrows():
        code, name = row['Code'], row['Name']
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d'))
        if df.empty: continue
        
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0; down[down > 0] = 0
        df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13).mean() / down.abs().ewm(com=13).mean())))
        
        last_row = df.iloc[-1].copy()
        last_row['Code'], last_row['Name'] = code, name
        results.append(last_row)
        
    return pd.DataFrame(results)

def upload_via_gas(file_path):
    print("GAS(우체국)를 통해 드라이브 업로드 시도 중...")
    url = os.environ.get('GAS_WEBAPP_URL')
    
    if not url:
        raise ValueError("깃허브 Secrets에 GAS_WEBAPP_URL이 설정되지 않았습니다.")

    # CSV 파일을 읽어서 텍스트로 변환
    with open(file_path, "r", encoding='utf-8-sig') as f:
        content = f.read()
    
    data = {
        "fileName": f"stock_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        "fileContent": content
    }
    
    # 구글 앱스 스크립트로 데이터 전송
    response = requests.post(url, data=json.dumps(data))
    print(f"서버 응답: {response.text}")

if __name__ == "__main__":
    final_df = get_analysis_data()
    csv_file = "analysis_result.csv"
    final_df.to_csv(csv_file, index=True, encoding='utf-8-sig')
    # [수정 완료] 괄호 짝을 맞췄습니다.
    upload_via_gas(csv_file)
