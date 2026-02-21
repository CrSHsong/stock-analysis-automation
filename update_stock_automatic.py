import FinanceDataReader as fdr
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

def get_analysis_data():
    print("🚀 1,500개 종목 정밀 분석 시작... (약 15~20분 소요)")
    
    # 1. KRX 종목 리스트 및 기본 재무지표 수집
    df_krx = fdr.StockListing('KRX')
    
    # 시가총액 컬럼 찾기 및 1,500개 추출 (우선주 포함)
    target_col = next((col for col in df_krx.columns if col.lower() == 'marcap'), None)
    top_1500 = df_krx.sort_values(by=target_col, ascending=False).head(1500)
    
    # 필요한 기본 정보 미리 저장 (PER, PBR 등)
    # FinanceDataReader의 Listing 정보에 포함된 재무 데이터를 활용합니다.
    fundamental_cols = ['Code', 'Name', 'Market', 'Sector', 'PER', 'PBR']
    # 실제 컬럼명이 다를 수 있으므로 존재하는 컬럼만 선택
    available_cols = [c for c in fundamental_cols if c in top_1500.columns]
    df_base = top_1500[available_cols].copy()

    results = []
    
    for _, row in top_1500.iterrows():
        code, name = row['Code'], row['Name']
        # 기술적 지표 계산을 위해 충분한 데이터(120일치) 확보
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=150)).strftime('%Y-%m-%d'))
        
        if df.empty or len(df) < 30: continue
        
        # --- [기술적 지표 계산] ---
        # 1. 이동평균선
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        
        # 2. RSI
        delta = df['Close'].diff()
        up, down = delta.copy(), delta.copy()
        up[up < 0] = 0; down[down > 0] = 0
        df['RSI'] = 100 - (100 / (1 + (up.ewm(com=13).mean() / down.abs().ewm(com=13).mean())))
        
        # 3. 볼린저 밴드 (20일, 2표준편차)
        std = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['SMA20'] + (std * 2)
        df['BB_Lower'] = df['SMA20'] - (std * 2)
        
        # 4. MACD (12, 26, 9)
        exp1 = df['Close'].ewm(span=12, adjust=False).mean()
        exp2 = df['Close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
        
        # --- [데이터 정리] ---
        last_row = df.iloc[-1].copy()
        last_row['Code'], last_row['Name'] = code, name
        
        # KRX 리스트에서 가져온 재무 데이터 합치기
        for col in available_cols:
            if col not in ['Code', 'Name']:
                last_row[col] = row[col]
        
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
    
    # 제미나이가 읽기 좋게 RSI 낮은 순(과매도)으로 정렬
    final_df = final_df.sort_values(by='RSI', ascending=True)
    
    # 1. 전체 리포트 저장 및 업로드
    full_file = "analysis_full_1500.csv"
    final_df.to_csv(full_file, index=False, encoding='utf-8-sig')
    upload_via_gas(full_file, f"stock_full_{datetime.now().strftime('%Y%m%d')}.csv")
    
    # 2. 제미나이 전용 '공략집' (RSI 35 이하 또는 MACD 골든크로스 종목)
    # MACD 히스토그램이 양수로 전환된 종목 추가
    candidates = final_df[(final_df['RSI'] <= 35) | (final_df['MACD_Hist'] > 0)]
    candidate_file = "target_candidates.csv"
    candidates.to_csv(candidate_file, index=False, encoding='utf-8-sig')
    upload_via_gas(candidate_file, f"target_candidates_{datetime.now().strftime('%Y%m%d')}.csv")
