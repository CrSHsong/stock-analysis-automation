import FinanceDataReader as fdr
import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta
from io import StringIO

def get_naver_financials(total_pages=30):
    """네이버 증권에서 5가지 핵심 재무 지표를 포함하여 수집합니다."""
    print(f"📡 네이버 증권에서 {total_pages*50}개 종목 재무 데이터 수집 중...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    
    # 5가지 지표를 강제 호출하는 파라미터 (영업이익률, ROE, PER, PBR, 부채비율)
    field_params = "&fieldIds=operating_profit_rate&fieldIds=roe&fieldIds=per&fieldIds=pbr&fieldIds=debt_ratio"
    
    all_dfs = []
    for page in range(1, total_pages + 1):
        url = f"https://finance.naver.com/sise/sise_market_sum.naver?&page={page}{field_params}"
        res = requests.get(url, headers=headers)
        df_list = pd.read_html(StringIO(res.text))
        df = df_list[1]
        df = df[df['N'].notnull()] # 유효 행 필터링
        all_dfs.append(df)
        
    full_df = pd.concat(all_dfs)
    
    # KRX 종목코드 매칭
    df_krx = fdr.StockListing('KRX')[['Code', 'Name']]
    result_df = pd.merge(full_df, df_krx, on='Name', how='inner')
    
    # 컬럼명 정리 (네이버 한글명을 영문/표준명으로 변경)
    col_map = {
        '영업이익률': 'Op_Margin',
        'ROE': 'ROE',
        'PER': 'PER',
        'PBR': 'PBR',
        '부채비율': 'Debt_Ratio',
        '현재가': 'Close_Naver'
    }
    result_df.rename(columns=col_map, inplace=True)
    return result_df

def get_analysis_data():
    days_to_load = 365 # 1년치 데이터
    print(f"🚀 1,500개 종목 1년치 기술적 지표 분석 시작...")
    
    df_finance = get_naver_financials(30)
    results = []
    
    for _, row in df_finance.iterrows():
        code, name = row['Code'], row['Name']
        df = fdr.DataReader(code, (datetime.now() - timedelta(days=days_to_load)).strftime('%Y-%m-%d'))
        
        if df.empty or len(df) < 30: continue
        
        # --- 기술적 지표 계산 ---
        # 1. 이동평균선
        df['SMA20'] = df['Close'].rolling(window=20).mean()
        df['SMA60'] = df['Close'].rolling(window=60).mean()
        
        # 2. RSI
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
        
        # --- 데이터 결합 ---
        last_row = df.iloc[-1].copy()
        last_row['Date'] = df.index[-1].strftime('%Y-%m-%d')
        last_row['Code'], last_row['Name'] = code, name
        
        # 네이버 재무 데이터 주입
        for field in ['Op_Margin', 'ROE', 'PER', 'PBR', 'Debt_Ratio']:
            last_row[field] = row[field] if field in row else "N/A"
            
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
    
    # 파일 저장 및 업로드
    full_file = "analysis_full.csv"
    final_df.to_csv(full_file, index=False, encoding='utf-8-sig')
    upload_via_gas(full_file, f"stock_full_{datetime.now().strftime('%Y%m%d')}.csv")
    
    # 타겟 종목 (RSI 과매도 또는 MACD 반전)
    candidates = final_df[(final_df['RSI'] <= 35) | (final_df['MACD_Hist'] > 0)]
    candidate_file = "target_candidates.csv"
    candidates.to_csv(candidate_file, index=False, encoding='utf-8-sig')
    upload_via_gas(candidate_file, f"target_candidates_{datetime.now().strftime('%Y%m%d')}.csv")
