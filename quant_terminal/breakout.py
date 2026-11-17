# -*- coding: utf-8 -*-
"""
板块突破 v2.56 (2026-09-05)
============================
行业/概念 等权合成指数 左峰突破判定 — 弹窗数据源 (行业突破 / 概念突破)

与行业趋势/概念趋势面板完全同口径 (用户 2026-09-05 定案):
- 集合: 行业 = load_pool() 有池→池内板块, 无池→全部板块
        概念 = 全部概念  — 与 analyze_sector_ma/analyze_concept_ma 一致
        (v5.0 2026-09-12: 成分门槛 5→1, 有几只算几只, 细分行业粒度下小板块不再被排除)
- 取序列: **v2.183 起优先用同花顺官方指数点位** (sector_kline 落盘的 710 个 CSV, 与行业趋势面板同一份);
         取不到时回退「涨幅等权合成 / 1000点起」(同 composite_sector_kline)。遭 `QT_BK_SRC=composite` 可强制全走合成做 A/B
        停牌过滤: 最新K线>3天不参与; 每日 ≥1 只有数据即计入该日 (v5.0, 原 ≥5)
- 口径: 收盘价序列 (不做盘中实时价覆盖 — 用户定案"收盘口径")
- 落盘: bk缓存 sector_idx_cache.json(合成序列) + sector_breakout_cache.json(判定结果)
        打开弹窗=读上次落盘秒回; 仅"启动刷新/双击按钮"触发 force 重算

三档并列独立判定 (无更远期高点限制 为各自档的约束):
  主升 (major) = 突破100日窗最高收盘   [窗口=排除当日+近5日的近100交易日, 同LeftPeakEngine]
  延续 (mid)   = 突破55日窗最高收盘, 且100日窗内存在更高点未被突破 (未破100日峰)
  反转 (minor) = 突破最近孤立峰(find_recent_two_peaks peak2), 且55日窗内存在更高点未破
三档互斥完备: 任一"收盘已站上某峰"的板块恰落入一档 (破峰更强档优先, 破峰价越低档越弱)
"""
import os, sys, json
from datetime import datetime
from collections import defaultdict as dd
from typing import Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from quant_terminal import engine as E  # noqa: E402

BK_DIR = os.path.join(BASE_DIR, 'quant_terminal')
IDX_FILE = os.path.join(BK_DIR, 'sector_idx_cache.json')      # 合成序列落盘
BK_FILE = os.path.join(BK_DIR, 'sector_breakout_cache.json')  # 三档判定落盘
TREND_LINES = (5, 10, 21, 55)                                 # 面板默认档 '4', 配对 status 用

_ACTIVE_PROBE = ('SH600519', 'SZ000001', 'SH600036')          # 活跃票探针: 判K线最新日期
import threading
_LOCK = threading.Lock()
STATE = {'running': False, 'kind': '', 'started': '', 'last_err': ''}


# ============================================================
# 工具
# ============================================================
def _kline_date() -> str:
    """本地K线最新日期 (探针活跃票CSV末行, 用于缓存新鲜度比对)"""
    for c in _ACTIVE_PROBE:
        try:
            rows = E.load_csv_kline(c)
            if rows:
                return rows[-1]['date']
        except Exception:
            continue
    return ''


def _atomic_save(fpath: str, data: dict):
    try:
        tmp = fpath + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, fpath)
    except Exception:
        pass


def _load_json(fpath: str) -> Optional[dict]:
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _read_tail(code_num: str, cache: dict, limit: int = 260) -> Optional[List[dict]]:
    """读成分股K线尾部 (进程内缓存: 全市场只读一次)"""
    if code_num in cache:
        return cache[code_num]
    prefix = 'SH' if (code_num.startswith('6') or code_num.startswith('9')) else 'SZ'
    fpath = os.path.join(E.KLINE_DIR, f'{prefix}{code_num}.csv')
    if not os.path.exists(fpath):
        cache[code_num] = None
        return None
    try:
        rows = E._read_csv_tail(fpath, limit)
    except Exception:
        rows = []
    if len(rows) < 21:
        rows = []
    cache[code_num] = rows or None
    return cache[code_num]


def _valid_stocks(codes: List[str], kc: dict) -> List[List[dict]]:
    """新鲜度过滤 (与 analyze_sector_ma 口径: 最新K线滞后>3个**交易日**剔除), 返回有效K线列表"""
    out = []
    for c in codes:
        rows = _read_tail(c, kc)
        if not rows:
            continue
        # v2.128: 改走 engine 的共用判据 (交易日口径; 原为 (now - last_dt).days > 3 = 日历日)
        if not E.kline_is_fresh(rows[-1]['date']):
            continue
        out.append(rows)
    return out


USE_OFFICIAL = os.environ.get('QT_BK_SRC', 'fuyao').lower() != 'composite'

# v2.212 (2026-10-24 DEBUG #5): 序列缓存从「落盘 16.5 MB JSON」改成「进程内内存」。
#   原来 IDX_FILE(sector_idx_cache.json) 存 608 个板块 × 1100+ 根 dates/closes,
#   16,485,980 B —— 但 _build_all() 实测只要 4.5 秒(官方指数口径, 不读全市场个股K线),
#   而进程重启后第一次本来也得重算。落盘那份的收益(省 4.5 秒)远不值 16.5 MB 常驻磁盘,
#   更要命的是它和 BK_FILE 两套缓存各存一份序列, 容易不一致。
#   改成内存缓存: 同 kline_date + 同 src_mode 直接命中; 进程重启后重算一次。
_IDX_MEM = {'kline_date': '', 'src_mode': '', 'data': None}


def _official_series(name: str, kind: str = 'sector'):
    """v2.183: 同花顺官方指数日线 → (dates, closes); 不可用返回 None
    走 kline_data/sector_index 里已落盘的 710 个官方指数 CSV (sector_kline 维护),
    名称解析与「行业趋势」面板同一套 resolve() (含 WiFi 6 / AI PC 这类归一化兜底)。
    883 特色统计 / 882 地域 不参与 (resolve 返回其 thscode 但 read_kline 可能为空, 一律当不可用)"""
    if not USE_OFFICIAL:
        return None
    try:
        from quant_terminal import sector_kline as _sk
        d = _sk.official_kline(kind, name)   # 官方点位; 不足 250 根时已把倒推段并进来
        if not d:
            return None
        dates = list(d.get('dates') or [])
        ohlc = d.get('ohlc') or []
        if len(dates) < 30 or len(ohlc) != len(dates):
            return None
        closes = [round(float(x[1]), 4) for x in ohlc]
        return dates, closes
    except Exception:
        return None


def _composite(dates_out: List[str], closes_out: List[float], stock_kl: List[List[dict]]) -> int:
    """涨幅等权合成 (与 analyze_sector_ma 同款): 每日有交易成分股平均涨幅链, 1000点起
    返回参与合成股票数; 空成分或序列<30日 → 不可用 (由调用方按 len(closes) 判定)"""
    d_ret = dd(list)
    for kl in stock_kl:
        for j in range(1, len(kl)):
            prev = kl[j - 1]['close']
            if prev > 0:
                d_ret[kl[j]['date']].append(kl[j]['close'] / prev - 1)
    dates_out.clear()
    closes_out.clear()
    idx = 1000.0
    for d in sorted(d_ret.keys()):
        vals = d_ret[d]
        if len(vals) >= 1:                       # v5.0 (2026-09-12): 每日 ≥1 只即计点 (原 ≥5)
            idx *= 1 + sum(vals) / len(vals)
            dates_out.append(d)
            closes_out.append(round(idx, 2))
    return len(stock_kl)


# ============================================================
# 合成序列全量重建 (首次/快照更新后/force)
# ============================================================
def _build_all() -> dict:
    """全市场K线读一遍, 合成行业+概念全部指数序列
    返回 {'kline_date', 'built_at', 'industry': {name: {dates, closes, stocks}}, 'concept': ...}"""
    ind_map, con_map = E.load_industry_concept_maps()

    # 行业集合口径与 analyze_sector_ma 一致: 有池→池内板块; 无池→全部
    sector_stocks = dd(list)
    for code_num, sec_name in ind_map.items():
        if sec_name:
            sector_stocks[sec_name].append(code_num)
    # v5.0 (2026-09-12): 成分门槛 5 → 1 (与 analyze_sector_ma 同口径, 有几只算几只)
    valid_sectors = {s: c for s, c in sector_stocks.items() if len(c) >= 1}
    pool = E.load_pool()
    if pool:
        pool_sectors = {info.get('sector', '') for info in pool.values()}
        pool_sectors = {s for s in pool_sectors if s in valid_sectors}
        target_sectors = pool_sectors if pool_sectors else set(valid_sectors.keys())
    else:
        target_sectors = set(valid_sectors.keys())

    # 概念: 无条件全量 (analyze_concept_ma 同口径, 反查 pipe 分隔)
    concept_stocks = dd(list)
    for code_num, cons in con_map.items():
        for c in cons.split('|'):
            c = c.strip()
            if c:
                concept_stocks[c].append(code_num)
    # v5.0 (2026-09-12): 概念门槛同步 5 → 1 (与 analyze_concept_ma 口径一致)
    valid_cons = {c: codes for c, codes in concept_stocks.items() if len(codes) >= 1}

    kc = {}  # 进程级K线缓存: 全市场只读一次
    industry, concept = {}, {}
    for sec_name in target_sectors:
        codes = valid_sectors[sec_name]
        off = _official_series(sec_name, 'sector')   # v2.183: 优先官方指数点位
        if off:
            dates, closes = off
            n, src = len(codes), 'fuyao'
        else:
            kl = _valid_stocks(codes, kc)
            if not kl:                            # v5.0: 原 <3
                continue
            dates, closes = [], []
            n = _composite(dates, closes, kl)
            src = 'composite'
        if len(closes) >= 30:
            industry[sec_name] = {'dates': dates, 'closes': closes, 'stocks': n, 'src': src}
    for con_name, codes in valid_cons.items():
        off = _official_series(con_name, 'concept')  # v2.183: 优先官方指数点位
        if off:
            dates, closes = off
            n, src = len(codes), 'fuyao'
        else:
            kl = _valid_stocks(codes, kc)
            if not kl:                            # v5.0: 原 <3
                continue
            dates, closes = [], []
            n = _composite(dates, closes, kl)
            src = 'composite'
        if len(closes) >= 30:
            concept[con_name] = {'dates': dates, 'closes': closes, 'stocks': n, 'src': src}
    return {'kline_date': _kline_date(), 'src_mode': ('fuyao' if USE_OFFICIAL else 'composite'),
            'built_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'industry': industry, 'concept': concept}


# ============================================================
# 三档左峰判定 (并列独立)
# ============================================================
def _window_pk(closes: List[float], period: int) -> Optional[dict]:
    """排除当日+最近5日的 period 交易日窗口最高收盘 (同 LeftPeakEngine 窗口)"""
    L = len(closes)
    if L < period + 6:
        return None
    end = L - 6
    start = end - period
    win = closes[start:end + 1]
    i = start + win.index(max(win))
    return {'price': float(max(win)), 'idx': i}


def _brk_stats(closes: List[float], dates: List[str], pk_price: float) -> tuple:
    """自末向前连续站上峰价的天数 + 首破日 (连续段起点=最近一次真正突破日)"""
    streak = 0
    bdate = None
    for i in range(len(closes) - 1, -1, -1):
        if closes[i] > pk_price:
            streak += 1
            bdate = dates[i]
        else:
            break
    return bdate, streak


def judge_series(dates: List[str], closes: List[float]) -> dict:
    """对一条合成指数序列做三档并列判定 → {major/mid/minor: row|None}
    row = {close, pk, pk_date, dist_pct, bdate, streak, is_new}
    主升>延续>反转 互斥完备; 各档数据不足时为 None"""
    c0 = closes[-1]
    pk100 = _window_pk(closes, 100)
    pk55 = _window_pk(closes, 55)
    out = {'major': None, 'mid': None, 'minor': None}

    def row_for(pk: dict, tier: str) -> dict:
        bdate, streak = _brk_stats(closes, dates, pk['price'])
        return {'tier': tier, 'close': round(c0, 2), 'pk': round(pk['price'], 2),
                'pk_date': dates[pk['idx']],
                'dist_pct': round((c0 / pk['price'] - 1) * 100, 1) if pk['price'] > 0 else 0.0,
                'bdate': bdate, 'streak': streak, 'is_new': streak <= 2}

    if pk100 and c0 > pk100['price']:
        out['major'] = row_for(pk100, 'major')
    elif pk55 and c0 > pk55['price']:
        # 延续: 破55日窗峰, 但100日窗内存在更高点未被突破 (破100峰的已进主升)
        out['mid'] = row_for(pk55, 'mid')
    else:
        # 反转: 破最近孤立峰 (未破55/100日窗峰; pk55缺失时=序列太短, 独立判孤立峰)
        pk2 = E.find_recent_two_peaks(closes).get('peak2')
        if pk2 and c0 > pk2['price'] and (pk55 is None or pk2['price'] < pk55['price']):
            out['minor'] = row_for(pk2, 'minor')
    return out


def _trend_lookup(kind: str) -> dict:
    """trend_data.json 默认档(5102155)按名配对 → {name: {status, bullish_pct}}"""
    d = E._load_trend_data()
    if not d:
        return {}
    key = E._trend_key(TREND_LINES)
    lines = d.get('lines', {}).get(key) or {}
    rows = lines.get('sector', []) if kind == 'industry' else lines.get('concept', [])
    return {r.get('name'): r for r in rows if r.get('name')}


def _judge_all(idx: dict, kind: str) -> dict:
    """全部板块三档判定 + 趋势面板status配对 + 排序"""
    boards = idx.get(kind, {})
    trend = _trend_lookup(kind)
    cols = {'major': [], 'mid': [], 'minor': []}
    for name, b in boards.items():
        d = judge_series(b['dates'], b['closes'])
        for tier, row in d.items():
            if row is None:
                continue
            t = trend.get(name, {})
            row['name'] = name
            row['status'] = t.get('status', '')
            row['bullish_pct'] = t.get('bullish_pct')
            row['stocks'] = b.get('stocks', 0)
            row['src'] = b.get('src', '')          # v2.183: fuyao=官方指数点位 / composite=成分股等权合成
            cols[tier].append(row)
    for tier in cols:
        # 新突破(★≤2天)在前, 同档按距峰%从小到大 (刚破位/破得越实越靠前)
        cols[tier].sort(key=lambda r: (0 if r['is_new'] else 1, r['dist_pct']))
    return cols


# ============================================================
# 对外主入口: board_breakout(kind, force)
# ============================================================
def board_breakout(kind: str, force: bool = False) -> dict:
    """行业/概念突破三档结果. force=False: 读落盘缓存秒回 (kline_date匹配才视为新);
    force=True: 重合成+重判定+落盘 (首次或快照更新后点"启动刷新")"""
    if kind not in ('industry', 'concept'):
        return {'ok': False, 'error': 'kind must be industry|concept'}
    if not _LOCK.acquire(blocking=False):
        return {'ok': False, 'busy': True,
                'error': STATE['kind'] + ' 扫描进行中, 请稍候...'}
    try:
        STATE['running'] = True
        STATE['kind'] = kind
        STATE['started'] = datetime.now().strftime('%H:%M:%S')
        kd = _kline_date()
        # v2.58 (2026-09-05): 概念屏蔽名单 — 落盘缓存在屏蔽生效前已含屏蔽概念, 读路径一律清洗;
        # 名单外一字不差不匹配的概念永不误伤 (行业不受任何影响)
        bl = E.load_concept_blocklist() if kind == 'concept' else frozenset()

        if not force:
            cached = _load_json(BK_FILE)
            if cached and cached.get('kline_date') == kd and kind in cached.get('cols', {}):
                cols = cached['cols'][kind]
                # 结构: {major:[行dict], mid:[...], minor:[...]}; 旧落盘可能含屏蔽概念 → 三层都清
                if bl:
                    if isinstance(cols, dict):
                        cols = {tier: [r for r in lst if r.get('name') not in bl]
                                for tier, lst in cols.items() if isinstance(lst, list)}
                    else:
                        cols = [r for r in cols if r.get('name') not in bl]
                return {'ok': True, 'cached': True, 'kline_date': kd,
                        'ts': cached.get('ts', ''), 'cols': cols}

        # 需要重算: 序列用**进程内**缓存 (v2.212: 不再落盘 16.5 MB)
        _sm = 'fuyao' if USE_OFFICIAL else 'composite'
        idx = _IDX_MEM.get('data')
        _hit = (idx is not None
                and _IDX_MEM.get('kline_date') == kd
                and _IDX_MEM.get('src_mode') == _sm)
        if not _hit or force:
            STATE['last_err'] = ''
            idx = _build_all()
            _IDX_MEM['data'] = idx
            _IDX_MEM['kline_date'] = idx.get('kline_date', '')
            _IDX_MEM['src_mode'] = idx.get('src_mode', '')
            # 旧版落盘文件已废弃(16.5 MB), 首次走到这里顺手清掉
            try:
                if os.path.exists(IDX_FILE):
                    os.remove(IDX_FILE)
            except Exception:
                pass
        # 屏蔽概念一律在**读路径**清洗, 绝不改内存里的原序列
        if bl and isinstance(idx.get('concept'), dict) and \
                any(n in bl for n in idx['concept']):
            idx = dict(idx)
            idx['concept'] = {n: v for n, v in idx['concept'].items() if n not in bl}

        bk = _load_json(BK_FILE) or {}
        cols_all = bk.get('cols', {})
        cols_all[kind] = _judge_all(idx, kind)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _atomic_save(BK_FILE, {'kline_date': kd, 'ts': now, 'cols': cols_all})
        return {'ok': True, 'cached': False, 'kline_date': kd, 'ts': now,
                'cols': cols_all[kind]}
    except Exception as e:
        import traceback
        STATE['last_err'] = traceback.format_exc()
        return {'ok': False, 'error': str(e)}
    finally:
        STATE['running'] = False
        _LOCK.release()


if __name__ == '__main__':
    # 命令行自测: python quant_terminal/breakout.py [industry|concept] [--force]
    import time
    kind = sys.argv[1] if len(sys.argv) > 1 else 'industry'
    force = '--force' in sys.argv
    t0 = time.time()
    r = board_breakout(kind, force=force)
    print('耗时 %.1fs' % (time.time() - t0))
    print('kline_date:', r.get('kline_date'), 'ts:', r.get('ts'), 'cached:', r.get('cached'))
    if not r.get('ok'):
        print('ERR:', r.get('error')); sys.exit(1)
    for tier, zh in (('major', '主升'), ('mid', '延续'), ('minor', '反转')):
        rows = r['cols'].get(tier, [])
        print('\n== %s (%d) ==' % (zh, len(rows)))
        for x in rows[:12]:
            print('  %-10s %-5s %s%% 首破%s 连%d天 %s' % (
                x['name'], x['status'] or '-', x['dist_pct'],
                x['bdate'], x['streak'], '★' if x['is_new'] else ''))
