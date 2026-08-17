import streamlit as st
import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time
import os
import json
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

# 忽略警告
warnings.filterwarnings('ignore')

# 页面配置
st.set_page_config(page_title="LOF 基金实时监控", layout="wide")

# 缓存目录
CACHE_DIR = ".lof_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

@st.cache_data(ttl=3*24*3600)
def get_fund_holdings(code):
    """
    获取基金持仓，带文件缓存和 Streamlit 内存缓存
    """
    cache_file = os.path.join(CACHE_DIR, f"holdings_{code}.json")
    if os.path.exists(cache_file):
        if time.time() - os.path.getmtime(cache_file) < 3 * 24 * 3600:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return pd.DataFrame(json.load(f))

    url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    headers = {
        "Referer": f"https://fundf10.eastmoney.com/ccmx_{code}.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    current_year = time.strftime("%Y")
    years = [current_year, str(int(current_year) - 1)]
    
    for year in years:
        params = {"type": "jjcc", "code": code, "topline": "10", "year": year, "month": ""}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=5)
            if 'content:"' not in r.text: continue
            
            start = r.text.find('content:"') + 9
            end = r.text.find('",', start)
            html = r.text[start:end]
            if not html or '<table' not in html: continue
                
            soup = BeautifulSoup(html, 'lxml')
            table = soup.find('table')
            if not table: continue
            
            df = pd.read_html(StringIO(str(table)), converters={'股票代码': str})[0]
            df.columns = [c.replace(' ', '').replace('\n', '').replace('\r', '') for c in df.columns]
            
            code_col = '股票代码'
            name_col = '股票名称'
            weight_cols = [c for c in df.columns if '占净值' in c]
            if not weight_cols: continue
            weight_col = weight_cols[0]
            
            df = df[[code_col, name_col, weight_col]]
            df.columns = ['股票代码', '股票名称', '占净值比例']
            df['占净值比例'] = df['占净值比例'].astype(str).str.replace('%', '').replace('--', '0')
            df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce').fillna(0) / 100.0
            
            if not df.empty:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(df.to_dict(orient='records'), f, ensure_ascii=False)
                return df
        except Exception:
            continue
    return None

def get_realtime_data():
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    status_text.text("正在获取 LOF 基金基础行情...")
    try:
        lof_prices = ak.fund_etf_category_sina(symbol="LOF基金")
        lof_prices['code'] = lof_prices['代码'].str.extract(r'(\d+)')
        lof_prices = lof_prices[['code', '名称', '最新价']]
        lof_prices['最新价'] = pd.to_numeric(lof_prices['最新价'], errors='coerce')
        lof_prices = lof_prices[lof_prices['最新价'] > 0]
    except Exception as e:
        st.error(f"获取场内行情失败: {e}")
        return None

    status_text.text("正在获取基金净值及申购状态...")
    try:
        fund_daily = ak.fund_open_fund_daily_em()
        # 兼容性处理列名
        name_cols = [c for c in fund_daily.columns if '简称' in c]
        nav_cols = [c for c in fund_daily.columns if '单位净值' in c]
        status_cols = [c for c in fund_daily.columns if '申购状态' in c]
        
        if not nav_cols or not name_cols:
             st.error(f"未找到必要的数据列。现有列: {fund_daily.columns.tolist()}")
             return None
             
        fund_daily['单位净值'] = fund_daily[nav_cols].bfill(axis=1).iloc[:, 0]
        
        # 提取核心数据
        fund_status = fund_daily[['基金代码', name_cols[0], '单位净值', status_cols[0]]]
        fund_status.columns = ['code', '简称', '昨日净值', '申购状态']
        fund_status['昨日净值'] = pd.to_numeric(fund_status['昨日净值'], errors='coerce')
        fund_status = fund_status.dropna(subset=['昨日净值'])
    except Exception as e:
        st.error(f"获取基金净值失败: {e}")
        return None

    merged_df = pd.merge(lof_prices, fund_status, on='code', how='inner')
    is_open = merged_df['申购状态'].str.contains('开放申购') | merged_df['申购状态'].str.contains('限制大额')
    not_paused = ~merged_df['申购状态'].str.contains('暂停')
    tradable_lof = merged_df[is_open & not_paused].copy()

    if tradable_lof.empty:
        st.warning("未发现可交易的 LOF 基金。")
        return None

    status_text.text("正在获取 A 股实时行情...")
    try:
        stock_spot = ak.stock_zh_a_spot()
        stock_spot['short_code'] = stock_spot['代码'].str.extract(r'(\d{6})')
        stock_spot['涨跌幅'] = pd.to_numeric(stock_spot['涨跌幅'], errors='coerce') / 100.0
        stock_dict = stock_spot.dropna(subset=['short_code']).set_index('short_code')['涨跌幅'].to_dict()
    except Exception as e:
        st.warning(f"获取股票行情失败: {e}")
        stock_dict = {}

    status_text.text(f"正在多线程分析 {len(tradable_lof)} 个基金的实时估值...")
    results = []
    
    total = len(tradable_lof)
    def process_fund(row):
        code = row['code']
        holdings = get_fund_holdings(code)
        est_change = 0.0
        covered_weight = 0.0
        if holdings is not None and not holdings.empty:
            for _, h in holdings.iterrows():
                s_code = h['股票代码']
                weight = h['占净值比例']
                if s_code in stock_dict:
                    est_change += stock_dict[s_code] * weight
                    covered_weight += weight
        est_nav = row['昨日净值'] * (1 + est_change)
        return {
            '代码': code,
            '基金名称': row['名称'],
            '现价': row['最新价'],
            '昨日净值': row['昨日净值'],
            '实时估值': est_nav,
            '估算涨幅%': round(est_change * 100, 3),
            '覆盖率%': round(covered_weight * 100, 2),
            '实时溢价%': round((row['最新价'] / est_nav - 1) * 100, 3)
        }

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(process_fund, row): row for _, row in tradable_lof.iterrows()}
        count = 0
        for future in as_completed(futures):
            count += 1
            progress_bar.progress(count / total)
            try:
                res = future.result()
                if res and not pd.isna(res['实时溢价%']):
                    results.append(res)
            except Exception:
                continue

    status_text.empty()
    progress_bar.empty()
    return pd.DataFrame(results)

def main():
    st.title("📈 LOF 基金实时折溢价监控")
    st.markdown("""
    该工具实时计算 LOF 基金的**实时估值**，逻辑如下：
    - **实时估值** = 昨日单位净值 * (1 + Σ(前10大重仓股今日涨幅 * 持仓占比))
    - 目前仅支持 A 股重仓股计算，港股/海外股暂视为 0 波动。
    """)

    if st.button("🚀 立即刷新数据"):
        df = get_realtime_data()
        if df is not None and not df.empty:
            df = df.sort_values(by='实时溢价%', ascending=False)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🔥 实时溢价 Top 10 (卖出机会)")
                st.dataframe(df.head(10).style.highlight_max(axis=0, subset=['实时溢价%'], color='lightpink'))

            with col2:
                st.subheader("❄️ 实时折价 Top 10 (买入机会)")
                st.dataframe(df.tail(10).iloc[::-1].style.highlight_min(axis=0, subset=['实时溢价%'], color='lightgreen'))

            st.subheader("📊 全部监控列表")
            st.dataframe(df, use_container_width=True)
        else:
            st.info("点击上方按钮开始抓取数据。")
    else:
        st.info("点击上方按钮开始抓取数据。")

if __name__ == "__main__":
    main()
