"""
Fetch OHLCV + amount cho toàn bộ mã HOSE/HNX/UPCoM từ VPS API.
- Lần đầu: kéo 420 ngày lịch sử (đủ 400 phiên cho Kronos)
- Hàng ngày: cập nhật 5 ngày gần nhất rồi deduplicate
Output: data/ohlcv_prices.csv
"""

import time
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_DIR   = Path(__file__).parent / 'data'
OHLCV_CSV  = DATA_DIR / 'ohlcv_prices.csv'
MAX_WORKERS = 10
BOOTSTRAP_DAYS = 420   # đủ 400 phiên giao dịch + dự phòng
UPDATE_DAYS    = 5     # cập nhật hàng ngày

EXCHANGES = ['hose', 'hnx', 'upcom']
EXCHANGE_LABEL = {'hose': 'HOSE', 'hnx': 'HNX', 'upcom': 'UPCOM'}

VPS_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Origin':     'https://banggia.vps.com.vn',
    'Referer':    'https://banggia.vps.com.vn/',
    'Accept':     'application/json, text/plain, */*',
}

VPS_HIST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'application/json',
    'Origin':     'https://banggia.vps.com.vn',
    'Referer':    'https://banggia.vps.com.vn/',
}


def fetch_tickers() -> dict[str, str]:
    result = {}
    for ex in EXCHANGES:
        url = f'https://bgapidatafeed.vps.com.vn/getlistckindex/{ex}'
        try:
            r = requests.get(url, headers=VPS_HEADERS, timeout=15)
            data = r.json()
            for item in data:
                sym = item.strip() if isinstance(item, str) else (item.get('sym') or '').strip()
                if sym:
                    result[sym] = EXCHANGE_LABEL[ex]
        except Exception as e:
            print(f'  Cảnh báo: {ex.upper()} — {e}')
    return result


def fetch_ohlcv_vps(ticker: str, exchange: str, lookback_days: int) -> list[dict]:
    """
    VPS TradingView-format API trả về: t, o, h, l, c, v
    amount = close × volume (VPS không cung cấp trực tiếp)
    """
    now_ts  = int(time.time())
    from_ts = now_ts - lookback_days * 24 * 3600
    url = (f'https://histdatafeed.vps.com.vn/tradingview/history'
           f'?symbol={ticker}&resolution=D&from={from_ts}&to={now_ts}')

    for attempt in range(3):
        try:
            r = requests.get(url, headers=VPS_HIST_HEADERS, timeout=15)
            r.raise_for_status()
            data = r.json()

            if data.get('s') != 'ok':
                return []

            t = data.get('t', [])
            o = data.get('o', [])
            h = data.get('h', [])
            l = data.get('l', [])
            c = data.get('c', [])
            v = data.get('v', [])

            if not t or not c:
                return []

            rows = []
            for i, ts in enumerate(t):
                close  = float(c[i]) if i < len(c) and c[i] else None
                if not close or close <= 0:
                    continue
                open_  = float(o[i]) if i < len(o) and o[i] else close
                high   = float(h[i]) if i < len(h) and h[i] else close
                low    = float(l[i]) if i < len(l) and l[i] else close
                vol    = float(v[i]) * 10 if i < len(v) and v[i] else 0.0  # VPS lot × 10
                amount = close * vol

                rows.append({
                    'date':     datetime.fromtimestamp(ts).strftime('%Y-%m-%d'),
                    'ticker':   ticker,
                    'exchange': exchange,
                    'open':     open_,
                    'high':     high,
                    'low':      low,
                    'close':    close,
                    'volume':   vol,
                    'amount':   amount,
                })
            return rows

        except Exception:
            if attempt < 2:
                time.sleep(1)
    return []


def run(lookback_days: int, label: str):
    print('═' * 60)
    print(f'  Fetch OHLCV — {label}')
    print('═' * 60)

    print('\n[1/3] Lấy danh sách mã từ VPS API...')
    ticker_exchange = fetch_tickers()
    tickers = list(ticker_exchange.items())
    print(f'     → {len(tickers)} mã')

    print(f'\n[2/3] Kéo OHLCV {lookback_days} ngày ({MAX_WORKERS} workers)...')
    all_rows = []
    success, failed, done = 0, [], 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_ohlcv_vps, sym, ex, lookback_days): sym
            for sym, ex in tickers
        }
        for future in as_completed(futures):
            sym = futures[future]
            done += 1
            pct = int(done / len(tickers) * 20)
            bar = '█' * pct + '░' * (20 - pct)
            print(f'\r     [{bar}] {done}/{len(tickers)}', end='', flush=True)

            rows = future.result()
            if rows:
                all_rows.extend(rows)
                success += 1
            else:
                failed.append(sym)

    print()

    print(f'\n[3/3] Lưu vào {OHLCV_CSV}...')
    if not all_rows:
        print('  Không có dữ liệu — kiểm tra kết nối.')
        return

    df_new = pd.DataFrame(all_rows)
    df_new['date'] = pd.to_datetime(df_new['date'])

    DATA_DIR.mkdir(exist_ok=True)

    if OHLCV_CSV.exists() and lookback_days == UPDATE_DAYS:
        df_old = pd.read_csv(OHLCV_CSV, parse_dates=['date'])
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    df = df.sort_values(['ticker', 'date']).drop_duplicates(['date', 'ticker'])
    df.to_csv(OHLCV_CSV, index=False)

    days = df['date'].nunique()
    rows_today = df_new['date'].nunique()
    print(f'\n' + '═' * 60)
    print(f'  HOÀN THÀNH')
    print(f'═' * 60)
    print(f'  Mã có dữ liệu   : {success} / {len(tickers)}')
    print(f'  Phiên tích lũy  : {days}')
    print(f'  Rows tổng       : {len(df):,}')
    if failed:
        print(f'  Mã thất bại     : {len(failed)}')
    print('═' * 60)


if __name__ == '__main__':
    if OHLCV_CSV.exists():
        run(lookback_days=UPDATE_DAYS, label='Cập nhật hàng ngày (5 ngày gần nhất)')
    else:
        run(lookback_days=BOOTSTRAP_DAYS, label='Bootstrap lần đầu (420 ngày lịch sử)')
