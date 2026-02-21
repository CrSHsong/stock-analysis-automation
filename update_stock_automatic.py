import FinanceDataReader as fdr
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

def get_analysis_data():
    # 1년(365일)치 데이터를 수집하도록 설정
    days_to_load = 365
    print(f"🚀 1,500개 종목 {days_to_load}일치 데이터 및 재무 지표 수집 시작...")
    
    # 1. KRX 전체 종목 리스트 수집
    df_krx = fdr.StockListing('KRX')
    
    # [핵심] 유연한 컬럼 찾기 함수: 이름이 조금 달라도 찾아냅니다.
    def find_col(target_names, df):
        for col in df.columns:
            if col.strip().upper() in [name.upper() for name in target_names]:
                return col
        return None

    # 시가총액, PBR, PER 컬럼 식별
    marcap_col = find_col(['Marcap', '시가총액', '시가총액(억)'], df_krx)
    pbr_col = find_col(['PBR', 'pbr', 'PBR(배)'], df_krx)
    per_col = find_col(['PER', 'per', 'PER(배)'], df_krx)

    # 시가총액 순 1,500개 추출
    top_1500 = df_krx.sort_values(by=marcap_col, ascending=False).head(1500)
    results = []
    
    for _, row in top_1500.iterrows():
        code, name = row['Code'], row['Name']
        # 1년치 데이터 수집
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=days_to_load)).strftime('%Y-%m-%d'))
        
        if df.empty or len(df) < 30: continue
        
        # --- 기술적 지표 계산 ---
        # 1. 이동평균선
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        
        # 2. RSI (상대강도지수)
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0; down[down > 0] = 0
        df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13).mean() / down.abs().ewm(com=13).mean())))
        
        # 3. 볼린저 밴드
        std = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['SMA20'] + (std * 2)
        df['BB_Lower'] = df['SMA20'] - (std * 2)
        
        # 4. MACD
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
        
        # --- 데이터 병합 및 강제 주입 ---
        last_row = df.iloc[-1].copy()
        last_row['Date'] = df.index[-1].strftime('%Y-%m-%d')
        last_row['Code'], last_row['Name'] = code, name
        
        # PBR, PER을 파일의 명확한 컬럼으로 강제 할당
        last_row['PBR'] = row[pbr_col] if pbr_col else "N/A"
        last_row['PER'] = row[per_col] if per_col else "N/A"
        last_row['Market'] = row['Market'] if 'Market' in row else "Unknown"
        
        results.append(last_row)
        
    return pd.DataFrame(results)

def upload_via_gas(file_path, file_name):
    print(f"📡 {file_name} 전송 중...")
    url = os.environ.get('GAS_WEBAPP_URL')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID')
    
    with open(file_path, "r", encoding='utf-8-sig') as f:
        content = f.read()
    
    data = {
        "fileName": file_name,
        "fileContent": content,
        "folderId": folder_id
    }
    
    response = requests.post(url, data=json.dumps(data))
    print(f"✅ 서버 응답: {response.text}")

if __name__ == "__main__":
    final_df = get_analysis_data()
    
    # 제미나이가 잘 읽도록 RSI 낮은 순(과매도)으로 우선 정렬
    final_df = final_df.sort_values(by='RSI', ascending=True)
    
    # 1. 전체 데이터 업로드
    full_file = "analysis_full.csv"
    final_df.to_csv(full_file, index=False, encoding='utf-8-sig')
    upload_via_gas(full_file, f"stock_full_{datetime.now().strftime('%Y%m%d')}.csv")
    
    # 2. 타겟 후보(RSI 35 이하 또는 MACD 반전) 업로드
    candidates = final_df[(final_df['RSI'] <= 35) | (final_df['MACD_Hist'] > 0)]
    candidate_file = "target_candidates.csv"
    candidates.to_csv(candidate_file, index=False, encoding='utf-8-sig')
    upload_via_gas(candidate_file, f"target_candidates_{datetime.now().strftime('%Y%m%d')}.csv")
