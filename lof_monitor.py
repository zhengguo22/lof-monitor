import akshare as ak
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time
import os
import json
import warnings
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tabulate import tabulate

# 忽略不必要的警告
warnings.filterwarnings('ignore')

# 缓存目录
CACHE_DIR = ".lof_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def get_fund_holdings(code):
    """
    从天天基金获取基金持仓（前10大重仓股）
    """
    cache_file = os.path.join(CACHE_DIR, f"holdings_{code}.json")
    # 缓存 3 天
    if os.path.exists(cache_file):
        if time.time() - os.path.getmtime(cache_file) < 3 * 24 * 3600:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return pd.DataFrame(data)

    url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    headers = {
        "Referer": f"https://fundf10.eastmoney.com/ccmx_{code}.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    # 尝试当前年份和上一年份
    current_year = time.strftime("%Y")
    years = [current_year, str(int(current_year) - 1)]
    
    for year in years:
        params = {
            "type": "jjcc",
            "code": code,
            "topline": "10",
            "year": year,
            "month": "",
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=5)
            text = r.text
            if 'content:"' not in text:
                continue
            
            start = text.find('content:"') + 9
            end = text.find('",', start)
            html = text[start:end]
            
            if not html or '<table' not in html:
                continue
                
            soup = BeautifulSoup(html, 'lxml')
            table = soup.find('table')
            if not table:
                continue
            
            df = pd.read_html(StringIO(str(table)))[0]
            # 归一化列名：去除空格、换行符、HTML 标签残余
            df.columns = [c.replace(' ', '').replace('\n', '').replace('\r', '').replace('\xa0', '') for c in df.columns]
            
            # 找到匹配的列名（兼容不同版本）
            code_cols = [c for c in df.columns if '代码' in c]
            name_cols = [c for c in df.columns if '名称' in c]
            weight_cols = [c for c in df.columns if '占净值' in c]
            
            if not code_cols or not name_cols or not weight_cols:
                continue
                
            code_col = code_cols[0]
            name_col = name_cols[0]
            weight_col = weight_cols[0]
            
            df = df[[code_col, name_col, weight_col]].copy()
            df.columns = ['股票代码', '股票名称', '占净值比例']
            
            # 确保股票代码是字符串，并处理可能的 float（如 688627.0）
            def format_code(x):
                s = str(x).split('.')[0].strip()
                if len(s) < 6 and s.isdigit() and s != '0':
                    # A 股代码补齐，但要区分港股（通常 5 位）
                    # 这里保持原始抓取到的数字字符串即可，后续匹配会处理
                    return s.zfill(6) if len(s) > 3 else s # 简单的 A 股补齐逻辑
                return s
            
            df['股票代码'] = df['股票代码'].apply(format_code)
            
            # 清理比例字符串
            df['占净值比例'] = df['占净值比例'].astype(str).str.replace('%', '').replace('--', '0')
            df['占净值比例'] = pd.to_numeric(df['占净值比例'], errors='coerce').fillna(0) / 100.0
            
            # 只有当有持仓数据时才缓存
            if not df.empty:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(df.to_dict(orient='records'), f, ensure_ascii=False)
                return df
        except Exception:
            continue
            
    return None

def get_realtime_lof_data():
    print("正在获取 LOF 基金基础数据 (Eastmoney)...")
    try:
        # 1. 获取 LOF 场内价格 (使用更稳定的 EM 接口)
        lof_spot = ak.fund_lof_spot_em()
        lof_spot.columns = [c.replace(' ', '') for c in lof_spot.columns]
        lof_prices = lof_spot[['代码', '名称', '最新价']].copy()
        lof_prices.columns = ['code', '名称', '最新价']
        lof_prices['最新价'] = pd.to_numeric(lof_prices['最新价'], errors='coerce')
        lof_prices = lof_prices[lof_prices['最新价'] > 0]
    except Exception as e:
        print(f"获取场内行情失败: {e}")
        return None

    print("正在获取基金净值及申购状态...")
    try:
        # 2. 获取基金单位净值（T-1）
        fund_daily = ak.fund_open_fund_daily_em()
        # 自动识别最新的单位净值列
        nav_cols = [c for c in fund_daily.columns if '单位净值' in c]
        if not nav_cols:
             print("未找到单位净值列")
             return None
        fund_daily['单位净值'] = fund_daily[nav_cols].bfill(axis=1).iloc[:, 0]
        
        fund_status = fund_daily[['基金代码', '基金简称', '单位净值', '申购状态']]
        fund_status.columns = ['code', '简称', '单位净值', '申购状态']
        fund_status['单位净值'] = pd.to_numeric(fund_status['单位净值'], errors='coerce')
        fund_status = fund_status.dropna(subset=['单位净值'])
    except Exception as e:
        print(f"获取基金净值失败: {e}")
        return None

    merged_df = pd.merge(lof_prices, fund_status, on='code', how='inner')
    is_open = merged_df['申购状态'].str.contains('开放申购') | merged_df['申购状态'].str.contains('限制大额')
    not_paused = ~merged_df['申购状态'].str.contains('暂停')
    tradable_lof = merged_df[is_open & not_paused].copy()

    if tradable_lof.empty:
        print("未发现可交易的 LOF 基金。")
        return None

    print(f"正在分析 {len(tradable_lof)} 个基金的实时估值 (多线程)...")
    
    # 3. 获取所有 A 股实时涨跌幅 (使用更稳定的 EM 接口)
    print("正在获取 A 股实时行情 (Eastmoney)...")
    try:
        stock_spot = ak.stock_zh_a_spot_em()
        # 提取 6 位数字代码
        stock_spot['short_code'] = stock_spot['代码'].astype(str).str.zfill(6)
        stock_spot['涨跌幅'] = pd.to_numeric(stock_spot['涨跌幅'], errors='coerce') / 100.0
        stock_dict = stock_spot.dropna(subset=['short_code']).set_index('short_code')['涨跌幅'].to_dict()
    except Exception as e:
        print(f"获取股票行情失败: {e}")
        stock_dict = {}

    # 4. 多线程获取持仓并计算估值
    results = []
    
    def process_fund(row):
        code = row['code']
        holdings = get_fund_holdings(code)
        
        # 估算涨跌幅
        est_change = 0.0
        covered_weight = 0.0
        
        if holdings is not None and not holdings.empty:
            for _, h_row in holdings.iterrows():
                s_code = h_row['股票代码']
                s_weight = h_row['占净值比例']
                if s_code in stock_dict:
                    est_change += stock_dict[s_code] * s_weight
                    covered_weight += s_weight
        
        # 估算 NAV = T-1 NAV * (1 + 持仓部分涨跌幅)
        est_nav = row['单位净值'] * (1 + est_change)
        
        results.append({
            '代码': code,
            '基金名称': row['名称'],
            '现价': row['最新价'],
            '昨日净值': row['单位净值'],
            '实时估值': round(est_nav, 4),
            '估算涨幅(%)': round(est_change * 100, 3),
            '覆盖率(%)': round(covered_weight * 100, 2),
            '实时溢价(%)': round((row['最新价'] / est_nav - 1) * 100, 3)
        })

    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(process_fund, [row for _, row in tradable_lof.iterrows()])

    if not results:
        return None
        
    return pd.DataFrame(results)

def main():
    print("-" * 60)
    print("LOF 基金实时折溢价监控 (考虑重仓股实时涨跌)")
    print("-" * 60)

    df = get_realtime_lof_data()

    if df is None or df.empty:
        print("未发现符合条件的套利品种或数据抓取失败。")
        sys.exit(0)

    # 排序
    df = df.sort_values(by='实时溢价(%)', ascending=False)

    # 输出溢价前 10 和折价前 10
    top_premium = df.head(10)
    top_discount = df.tail(10).iloc[::-1] # 折价率最大的在最前

    print("\n" + "="*120)
    print("【实时溢价排行 Top 10】 (场内卖出机会)")
    print("="*120)
    print(tabulate(top_premium, headers='keys', tablefmt='psql', showindex=False, numalign="right", floatfmt=".3f"))

    print("\n" + "="*120)
    print("【实时折价排行 Top 10】 (场入买入机会)")
    print("="*120)
    print(tabulate(top_discount, headers='keys', tablefmt='psql', showindex=False, numalign="right", floatfmt=".3f"))

    print("\n[算法说明]")
    print("1. 实时估值 = 昨日单位净值 * (1 + Σ(重仓股今日涨幅 * 持仓占比))。")
    print("2. 目前仅支持 A 股重仓股的实时计算，港股/海外重仓股暂视为 0 波动。")
    print("3. 持仓数据取自天天基金最新披露的季报/半年报（前10大重仓股）。")
    print("4. 覆盖率表示前10大重仓股中，有多少比例的资产被纳入了实时涨跌计算。")
    print("="*120)

if __name__ == "__main__":
    main()
