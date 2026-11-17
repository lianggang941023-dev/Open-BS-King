"""
BS King 量化终端 v2.0 — 回测模块
=================================
动态池子: 每天从全市场 MA 筛选 -> 四模块扫描 -> 前向收益统计
用法: python -B quant_terminal/backtest.py --days 60
"""
import sys, os, csv, time
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KLINE_DIR = os.path.join(BASE_DIR, 'kline_data', 'daily')
sys.path.insert(0, BASE_DIR)

from quant_terminal.engine import QuantEngine, Indicators, load_industry_concept_maps, _read_csv_tail


# ============================================================
# 1. 预加载 (一次性, 3-4秒)
# ============================================================

def preload_all(verbose=True) -> Tuple[Dict, Dict]:
    """
    一次性加载全市场 OHLCV + 收盘价序列到内存
    closes: {code: [(date, close), ...]}
    ohlcv:  {code: [{date,open,close,high,low,volume}, ...]}
    """
    closes_dict, ohlcv_dict = {}, {}
    csv_files = sorted([f for f in os.listdir(KLINE_DIR) if f.endswith('.csv')])
    total = len(csv_files)
    t0 = time.time()

    for i, fname in enumerate(csv_files):
        code = fname.replace('.csv', '')
        if not (code.startswith('SZ') or code.startswith('SH')): continue
        fpath = os.path.join(KLINE_DIR, fname)
        try:
            tail = _read_csv_tail(fpath, 300)
            if len(tail) < 120: continue
            closes_dict[code] = [(r['date'], r['close']) for r in tail]
            ohlcv_dict[code] = tail
        except Exception:
            continue
        if verbose and (i + 1) % 1000 == 0:
            print(f'  预加载: {i+1}/{total} ({time.time()-t0:.1f}s)')

    if verbose:
        print(f'  预加载完成: {len(closes_dict)} 只, {time.time()-t0:.1f}s\n')
    return closes_dict, ohlcv_dict


# ============================================================
# 2. 动态建池 (MA筛选, 毫秒级)
# ============================================================

def build_pool_for_date(all_closes: Dict, target_date: str) -> List[str]:
    """对指定日期, 用 MA21>MA55>MA120 AND close>MA21 筛选池子"""
    pool = []
    for code, closes in all_closes.items():
        idx = len(closes) - 1
        while idx >= 0 and closes[idx][0] > target_date:
            idx -= 1
        if idx < 120: continue
        valid = [c[1] for c in closes[:idx+1]]
        close = valid[-1]
        ma21 = sum(valid[-21:]) / 21
        ma55 = sum(valid[-55:]) / 55
        ma120 = sum(valid[-120:]) / 120
        if ma21 > ma55 > ma120 and close > ma21:
            pool.append(code)
    return pool


# ============================================================
# 3. 扫描 + 前向收益
# ============================================================

def slice_kline(ohlcv_dict: Dict, code: str, target_date: str) -> List[Dict]:
    """从预加载数据截取K线"""
    if code not in ohlcv_dict: return []
    return [r for r in ohlcv_dict[code] if r['date'] <= target_date]


def calc_forward(all_closes: Dict, code: str, signal_date: str,
                 holds: List[int]) -> Dict[int, Optional[float]]:
    """计算前向收益"""
    if code not in all_closes: return {h: None for h in holds}
    closes = all_closes[code]
    idx = next((i for i, (d, _) in enumerate(closes) if d == signal_date), -1)
    if idx < 0: return {h: None for h in holds}
    entry = closes[idx][1]
    if entry <= 0: return {h: None for h in holds}
    return {h: (closes[idx+h][1] - entry) / entry * 100
            if idx + h < len(closes) else None for h in holds}


# ============================================================
# 4. 主循环
# ============================================================

def run_backtest(lookback_days: int = 60, start: str = '', end: str = '',
                 holds: List[int] = None, verbose=True) -> List[Dict]:
    if holds is None: holds = [1, 3, 5, 10, 20]

    if verbose:
        date_label = f'{start}~{end}' if start else f'最近{lookback_days}天'
        print('=' * 60)
        print(f'  BS King 回测 | {date_label} | 前向{holds}')
        print('=' * 60)
        print('\n[1/3] 预加载全市场K线...')

    all_closes, all_ohlcv = preload_all(verbose)

    # 确定回测日期
    if verbose: print('[2/3] 逐日扫描...')
    all_dates_set = set()
    for closes in all_closes.values():
        for d, _ in closes: all_dates_set.add(d)
    all_dates_sorted = sorted(all_dates_set)

    if start and end:
        bt_dates = [d for d in all_dates_sorted if start <= d <= end]
    else:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        idx = all_dates_sorted.index(yesterday) if yesterday in all_dates_sorted else len(all_dates_sorted) - 1
        bt_dates = all_dates_sorted[max(0, idx - lookback_days + 1):idx + 1]

    engine = QuantEngine()
    engine.ind_map, engine.con_map = load_industry_concept_maps()
    all_signals = []
    t0 = time.time()

    for di, target_date in enumerate(bt_dates):
        pool = build_pool_for_date(all_closes, target_date)
        if verbose and di % 5 == 0:
            print(f'  [{di+1}/{len(bt_dates)}] {target_date} 池{len(pool)}只 '
                  f'信号{len(all_signals)} {time.time()-t0:.0f}s')

        for code in pool:
            kline = slice_kline(all_ohlcv, code, target_date)
            if len(kline) < 60: continue
            close = kline[-1]['close']
            prev = kline[-2]['close'] if len(kline) > 1 else close
            chg = (close - prev) / prev * 100 if prev > 0 else 0
            info = engine.pool.get(code, {'code': code, 'name': code, 'sector': '', 'concept': ''})
            num = code[2:] if code.startswith(('SZ','SH')) else code
            info['sector'] = engine.ind_map.get(num, '')
            info['concept'] = engine.con_map.get(num, '')

            try:
                r = engine.scan_single(code, info, close, chg, kline_override=kline)
            except Exception:
                continue

            fwd = calc_forward(all_closes, code, target_date, holds)
            all_signals.append({
                'date': target_date, 'code': r['code'], 'name': r['name'],
                'm1_signal': r['m1_key_point']['signal'],
                'm1_conf': r['m1_key_point']['confidence'],
                'm2_score': r['m2_volume']['score'],
                'm2_level': r['m2_volume']['level'],
                'cons_score': r['consensus']['score'],
                'cons_rec': r['consensus']['recommendation'],
                'sector': r['sector'],
                'trend_ok': r['m3_position']['trend_filter']['filter_pass'],
                'sector_strong': r['m4_ashare']['sector']['is_strong'],
                **{f'ret_{h}d': v for h, v in fwd.items()},
            })

        engine.pool.clear()

    if verbose:
        print(f'\n[3/3] 完成: {len(all_signals)} 信号, {time.time()-t0:.0f}s\n')
    return all_signals


# ============================================================
# 5. 报告
# ============================================================

def stats(subset, label, holds):
    n = len(subset)
    if n == 0: print(f'\n  {label}: 0 信号'); return
    print(f'\n  {label}: {n} 信号')
    print(f'  {"持有":<6} {"胜率":>6} {"均收益":>8} {"中位数":>8} {"最大盈":>8} {"最大亏":>8}')
    print(f'  {"-"*52}')
    for h in holds:
        rets = [s.get(f'ret_{h}d') for s in subset if s.get(f'ret_{h}d') is not None]
        if not rets: continue
        wr = sum(1 for r in rets if r > 0) / len(rets) * 100
        avg = sum(rets) / len(rets); med = sorted(rets)[len(rets)//2]
        label = f'{h}日'
        print(f'  {label:<6} {wr:>5.1f}% {avg:>+7.2f}% {med:>+7.2f}% {max(rets):>+7.2f}% {min(rets):>+7.2f}%')


def report(signals, holds=None):
    if holds is None: holds = [1, 3, 5, 10, 20]
    print('=' * 60)
    print('  回测报告')
    print('=' * 60)

    print('\n--- 表1: 按 M1 关键点信号 ---')
    for sig in ['BREAKTHROUGH_BUY', 'PULLBACK_BUY', 'WATCH', 'DANGER', 'NEUTRAL']:
        stats([s for s in signals if s['m1_signal'] == sig], sig, holds)

    print('\n--- 表2: 按共识评分区间 ---')
    for lo, hi, label in [(15, 100, '>=15 买入'), (10, 14, '10-14 关注'), (5, 9, '5-9 观望'), (0, 4, '<5 回避')]:
        stats([s for s in signals if lo <= s['cons_score'] <= hi], label, holds)

    print('\n--- 表3: 各模块独立有效性 ---')
    for level, label in [('STRONG', 'M2强确认>=7'), ('MODERATE', 'M2中确认4-6'), ('WEAK', 'M2弱确认<4')]:
        stats([s for s in signals if s['m2_level'] == level], label, holds)
    stats([s for s in signals if s.get('sector_strong')], '板块共振: YES', holds)
    stats([s for s in signals if not s.get('sector_strong')], '板块共振: NO', holds)
    stats([s for s in signals if s.get('trend_ok')], '趋势MA250: YES', holds)
    stats([s for s in signals if not s.get('trend_ok')], '趋势MA250: NO', holds)


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--days', type=int, default=60, help='回测最近N个交易日')
    p.add_argument('--start', type=str, default='', help='起始日期 YYYY-MM-DD')
    p.add_argument('--end', type=str, default='', help='结束日期 YYYY-MM-DD')
    p.add_argument('--holds', type=str, default='1,5,10,20')
    a = p.parse_args()
    holds = [int(x) for x in a.holds.split(',')]
    sigs = run_backtest(lookback_days=a.days, start=a.start, end=a.end, holds=holds)
    report(sigs, holds)


if __name__ == '__main__':
    main()
