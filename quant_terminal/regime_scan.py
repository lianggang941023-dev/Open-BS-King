# -*- coding: utf-8 -*-
"""市场环境(regime) —— 照抄 TSP `backend/app/services/regime_builder.py` 的算法。

v2.311 (2026-11-14 用户 m18234): TSP「市场环境」模块移植。用户原话「"市场环境"的功能实现
完全照搬TSP中的"市场环境"模块…完全照抄即可」。

## 算法（与 TSP 逐字一致的部分）
4 维子分各自 `_score(value, low, high)` 线性映射到 0~100（low/high 是 A 股 2022-2026
的 p15/p85 分位，**原样抄 TSP，不许自己调**）：
    profit      = score(up_pct,21,75)*.45 + score(avg_pct*100,-1.2,1.3)*.25
                + score(median_pct*100,-1.2,1.3)*.20 + score(strong_diff_pct,-13,14)*.10
    speculation = score(limit_up,35,97)*.30 + score(seal_rate*100,57,75)*.40
                + score(max_consecutive,4,9)*.30
    resilience  = 100 - score(strong_down_pct, 2, 18)      # 只用大跌占比, 不用 down_pct
    trend       = score(index_pct*100,-2.5,2.5)*.50 + score(above_ma20_pct*100,22,76)*.50
    score       = profit*.35 + speculation*.25 + resilience*.20 + trend*.20
五态（阈值与 TSP 一致）: >=70 强势 / >=55 偏强 / >=45 震荡 / >=30 偏弱 / else 弱势

## 与 TSP 的**数据口径差异**（TSP 走 enriched 表的 signal_* 列，我们只有裸 K 线）
- 涨停/跌停/炸板: 用「按板别算出的涨跌停价」判定，不依赖 TSP 的信号列
  板别: 920xxx/4/8 开头 = 30%(北交), 300/301/302/688/689 = 20%(双创), 其余 10%
  炸板 = 最高价摸到涨停价但收盘没封住 —— 与 TSP signal_broken_limit_up 同义
- 连板高度: 逐票累加「连续收盘涨停」天数，取当日全市场最大值
  v2.317 (2026-11-14 用户 m20719): **跨停牌日断开连板** —— 停牌复牌那根 K 线的上一根
  可能不是紧邻的上一交易日, 原来 `pc = closes[i-1]` 会把停牌前的板数接上来, 实测把
  `002931` 算成 14 板(真实 4 板)、`001331` 13 板、`600825` 11 板、`603221` 10 板。
  现在用 `_calendar()` 的日期序号判紧邻, 不紧邻就把 `consec` 归零再计。
  口径选「保留该行、只断连板」: 当天确实涨停, 不能从涨停家数里消失, 只改连板怎么算。
- 晋级率 promo_rate / 情绪周期 phase: v2.312 起**算**（用户 m18508 要求移植「情绪周期」，
  见 `regime_phase.py`）
- 成交额: volume(手)*100*((high+low+close)/3)，单位元
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ── TSP 原样常量 ──────────────────────────────────────────────
WEIGHTS = {'profit': 0.35, 'speculation': 0.25, 'resilience': 0.20, 'trend': 0.20}
STATE_STRONG = 70
STATE_LEAN_STRONG = 55
STATE_RANGE = 45
STATE_LEAN_WEAK = 30
STATE_LABELS = {
    'strong': '强势', 'lean_strong': '偏强', 'range': '震荡',
    'lean_weak': '偏弱', 'weak': '弱势',
}
STATE_COLORS = {
    'strong': '#ef4444', 'lean_strong': '#f97316', 'range': '#6b7280',
    'lean_weak': '#3b82f6', 'weak': '#10b981',
}

DAYS_MIN, DAYS_MAX, DAYS_DEF = 30, 1000, 250
_WARMUP = 25            # ma20 预热(交易日)
_CACHE_TTL = 600.0      # 10 分钟；盘后数据不变, 盘中一日一行变化不大
_CACHE_FILE = os.path.join(ROOT, 'kline_data', 'regime_cache.json')
# v2.312: 缓存版本号 —— 加字段就 +1。老缓存没有 phase / first_board / ge2_count,
#   混进来会让「情绪周期」把每一天都判成冰点(first_board=0/ge2=0), 而且不报错。
_CACHE_VER = 3   # v2.317: 跨停牌断连板改了口径 ⇒ 老缓存(ver 2)作废, 否则读到旧 max_consecutive

_C = {'ts': 0.0, 'rows': None}
_LOCK = threading.Lock()


# ── 基础设施 ──────────────────────────────────────────────────
def kline_dir():
    try:
        from quant_terminal import datasource as _ds
        p = _ds.resolve(_ds.get('local', 'kline_dir', ''))
        if p and os.path.isdir(p):
            return p
    except Exception:
        pass
    return os.path.join(ROOT, 'kline_data', 'daily')


def index_dir():
    return os.path.join(ROOT, 'kline_data', 'index_daily')


def _limit_pct(code):
    """涨跌停幅度（小数）。code 是 6 位裸码。"""
    c = code or ''
    if c[:3] == '920' or c[:1] in ('4', '8'):
        return 0.30                      # 北交所
    if c[:3] in ('300', '301', '302') or c[:3] in ('688', '689'):
        return 0.20                      # 创业板 / 科创板
    if c[:3] in ('900',):                # B 股沪
        return 0.10
    return 0.10


def _read_tail_rows(path, n):
    """倒读 CSV 末尾 n 行 → [(date, open, close, high, low, volume), ...] 升序。

    只取前 6 列（date/open/close/high/low/volume）—— 第 7~9 列市值对 regime 无用。
    """
    try:
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block, buf, pos = 65536, b'', size
            need = n + 2
            while pos > 0 and buf.count(b'\n') <= need:
                step = min(block, pos)
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
            lines = buf.split(b'\n')
            if pos > 0 and lines:
                lines = lines[1:]
    except OSError:
        return []
    out = []
    for ln in lines:
        s = ln.decode('utf-8', 'ignore').strip()
        if not s:
            continue
        p = s.split(',')
        if len(p) < 6:
            continue
        try:
            out.append((p[0], float(p[1]), float(p[2]), float(p[3]),
                        float(p[4]), float(p[5])))
        except ValueError:
            continue
    return out[-n:] if n and len(out) > n else out


def _calendar():
    """交易日历（升序），来自 index_daily/SH000001.csv。"""
    p = os.path.join(index_dir(), 'SH000001.csv')
    rows = _read_tail_rows(p, 4000)
    return [r[0] for r in rows]


def _index_pct():
    """{date: 上证涨幅(小数)}，用于 trend 维度。"""
    p = os.path.join(index_dir(), 'SH000001.csv')
    rows = _read_tail_rows(p, 4000)
    out = {}
    prev = None
    for d, _o, c, _h, _l, _v in rows:
        if prev and prev > 0:
            out[d] = (c - prev) / prev
        prev = c
    return out


# ── 评分模型（TSP 逐字） ──────────────────────────────────────
def _score(value, low, high):
    """归一化: [low, high] 线性映射到 [0,100] 并取整钳制。"""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 50.0
    if not (v == v) or v in (float('inf'), float('-inf')):
        return 50.0
    if high <= low:
        return 50.0
    x = (v - low) / (high - low) * 100.0
    return float(max(0, min(100, round(x))))


def compute_subscores(m):
    """4 维子分 + 综合分（与 TSP `_compute_subscores` 一字不差）。"""
    profit = (_score(m.get('up_pct', 50), 21, 75) * 0.45
              + _score(m.get('avg_pct', 0) * 100, -1.2, 1.3) * 0.25
              + _score(m.get('median_pct', 0) * 100, -1.2, 1.3) * 0.20
              + _score(m.get('strong_diff_pct', 0), -13, 14) * 0.10)
    seal = m.get('seal_rate')
    seal = 0.5 if seal is None else seal
    speculation = (_score(m.get('limit_up', 0), 35, 97) * 0.30
                   + _score((seal or 0.5) * 100, 57, 75) * 0.40
                   + _score(m.get('max_consecutive', 0), 4, 9) * 0.30)
    resilience = 100 - _score(m.get('strong_down_pct', 0), 2, 18)
    above = m.get('above_ma20_pct')
    above = 0.5 if above is None else above
    trend = (_score(m.get('index_pct', 0) * 100, -2.5, 2.5) * 0.50
             + _score((above or 0.5) * 100, 22, 76) * 0.50)
    score = (profit * WEIGHTS['profit'] + speculation * WEIGHTS['speculation']
             + resilience * WEIGHTS['resilience'] + trend * WEIGHTS['trend'])
    return {'profit': profit, 'speculation': speculation,
            'resilience': resilience, 'trend': trend, 'score': score}


def classify_state(m):
    sub = compute_subscores(m)
    score = int(max(0, min(100, round(sub['score']))))
    if score >= STATE_STRONG:
        state = 'strong'
    elif score >= STATE_LEAN_STRONG:
        state = 'lean_strong'
    elif score >= STATE_RANGE:
        state = 'range'
    elif score >= STATE_LEAN_WEAK:
        state = 'lean_weak'
    else:
        state = 'weak'
    return state, score


# ── 主计算 ────────────────────────────────────────────────────
def _compute(days):
    """扫描全市场 CSV，按日聚合 → 逐日五态。返回升序 rows。"""
    cal = _calendar()
    if not cal:
        return []
    want = cal[-(days + _WARMUP):]
    want_set = set(want)
    # v2.317 (2026-11-14 用户 m20719 选 A「直接修 + 历史重算」):
    #   交易日历日期 → 序号, 用来判断「上一根 K 线是不是紧邻的上一交易日」。
    #   停牌复牌后 rows[i-1] 可能是几天/几周前那根, pc 就是多个交易日的昨收,
    #   chg 变成跨多日涨跌幅、consec 把停牌前的板数接上来。
    _cal_idx = {d: i for i, d in enumerate(cal)}
    ipct = _index_pct()

    # 累计器（按日期键，值用 list 便于就地累加）
    acc = {}
    for d in want:
        acc[d] = {
            'n': 0, 'up': 0, 'down': 0, 'su': 0, 'sd': 0,
            'lu': 0, 'ld': 0, 'brk': 0, 'maxc': 0,
            'ma_ok': 0, 'ma_up': 0, 'amt': 0.0, 'pcts': [],
            # v2.312 (2026-11-14 用户 m18508): 情绪周期 / 主线排行要的驱动量
            #   fb = 首板家数(consec==1) / ge2 = 2板+家数 / rungs = 2..height 的非空档位
            #   lus = 当日涨停股 [(6位码, 连板数, 成交额)] —— 主线排行靠它归组
            'fb': 0, 'ge2': 0, 'rungs': set(), 'lus': [],
        }

    d = kline_dir()
    try:
        names = os.listdir(d)
    except OSError:
        return []

    need = len(want) + 25
    for fn in names:
        if not fn.endswith('.csv'):
            continue
        code = fn[:-4]
        if code[:2].upper() in ('SH', 'SZ', 'BZ', 'BJ'):
            code = code[2:]
        if not code.isdigit() or len(code) != 6:
            continue
        rows = _read_tail_rows(os.path.join(d, fn), need)
        if len(rows) < 25:
            continue
        lp = _limit_pct(code)
        consec = 0
        ma_sum = 0.0
        # 预扫：算 ma20 需要滑动窗口
        closes = [r[2] for r in rows]
        # i 从 20 起才有 ma20
        for i in range(1, len(rows)):
            dt, _op, cl, hi, _lo, vol = rows[i]
            pc = closes[i - 1]
            if not (pc > 0):
                continue
            if dt not in want_set:
                # 仍需维护连板与 ma 窗口
                pass
            chg = (cl - pc) / pc
            lim_up = round(pc * (1.0 + lp) + 1e-9, 2)
            lim_dn = round(pc * (1.0 - lp) + 1e-9, 2)
            is_lu = cl >= lim_up - 0.005
            is_ld = cl <= lim_dn + 0.005
            is_brk = (not is_lu) and hi >= lim_up - 0.005
            # v2.317 (用户 m20719): 跨停牌日 —— 上一根不是紧邻的上一交易日。
            #   口径选 **C**: 这一根**照常计入涨停/跌停/炸板/涨跌家数**(它当天确实涨停了),
            #   只是**把连板序列断开** —— 停牌前的板数不能接上来。
            #   (B「整行跳过」会把当天的涨停也从涨停家数里抹掉, 那是错的)
            #   实测: 只动 max_consecutive(26天/合计117/最大13) 与
            #   first_board/ge2_count(各26天/合计29) + score(20天/合计75/最大7),
            #   涨停家数/涨跌家数/炸板数 全部不变。
            _pd = rows[i - 1][0]
            if not (dt in _cal_idx and _pd in _cal_idx
                    and (_cal_idx[dt] - _cal_idx[_pd]) == 1):
                consec = 0
            consec = (consec + 1) if is_lu else 0
            # ma20
            ma20 = None
            if i >= 19:
                s = 0.0
                for k in range(i - 19, i + 1):
                    s += closes[k]
                ma20 = s / 20.0
            if dt in want_set:
                a = acc[dt]
                a['n'] += 1
                if chg > 0:
                    a['up'] += 1
                elif chg < 0:
                    a['down'] += 1
                if chg >= 0.03:
                    a['su'] += 1
                elif chg <= -0.03:
                    a['sd'] += 1
                if is_lu:
                    a['lu'] += 1
                if is_ld:
                    a['ld'] += 1
                if is_brk:
                    a['brk'] += 1
                if consec > a['maxc']:
                    a['maxc'] = consec
                if ma20 and ma20 > 0:
                    a['ma_ok'] += 1
                    if cl > ma20:
                        a['ma_up'] += 1
                a['amt'] += vol * 100.0 * ((hi + _lo + cl) / 3.0)
                a['pcts'].append(chg)
                # v2.312: 情绪周期驱动量 + 主线归组用的涨停股清单
                if is_lu:
                    if consec == 1:
                        a['fb'] += 1
                    else:
                        a['ge2'] += 1
                        a['rungs'].add(consec)
                    a['lus'].append((code, consec,
                                     vol * 100.0 * ((hi + _lo + cl) / 3.0)))

    out = []
    lu_by_day = {}
    prev_lu = None
    for dt in want:
        a = acc[dt]
        n = a['n']
        if n <= 0:
            continue
        up_pct = a['up'] / n * 100.0
        su_pct = a['su'] / n * 100.0
        sd_pct = a['sd'] / n * 100.0
        pcts = sorted(a['pcts'])
        med = pcts[len(pcts) // 2] if pcts else 0.0
        avg = (sum(pcts) / len(pcts)) if pcts else 0.0
        lu, brk = a['lu'], a['brk']
        seal = (lu / (lu + brk)) if (lu + brk) > 0 else 0.5
        above = (a['ma_up'] / a['ma_ok']) if a['ma_ok'] > 0 else 0.0
        # v2.312: 晋级率 = 今日 2板+ 家数 / 昨日涨停家数（等价推导，见 regime_phase 模块头）
        #   昨日池 < PROMO_MIN_POOL(10) 记 None —— 小样本噪声
        mxc = a['maxc']
        promo_pool = prev_lu if prev_lu is not None else 0
        promo = (a['ge2'] / float(promo_pool)) if promo_pool >= 10 else None
        ladder = (len(a['rungs']) / float(mxc - 1)) if mxc >= 3 else 0.0
        lu_by_day[dt] = a['lus']
        prev_lu = lu
        m = {
            'up_pct': up_pct, 'down_pct': a['down'] / n * 100.0,
            'avg_pct': avg, 'median_pct': med,
            'strong_up_pct': su_pct, 'strong_down_pct': sd_pct,
            'strong_diff_pct': su_pct - sd_pct,
            'limit_up': lu, 'limit_down': a['ld'], 'broken_limit': brk,
            'seal_rate': seal, 'max_consecutive': a['maxc'],
            'index_pct': ipct.get(dt, 0.0), 'above_ma20_pct': above,
        }
        sub = compute_subscores(m)
        state, score = classify_state(m)
        out.append({
            'date': dt,
            'state': state,
            'score': score,
            'limit_up': lu,
            'limit_down': a['ld'],
            'broken_limit': brk,
            'max_consecutive': a['maxc'],
            'seal_rate': round(seal, 4),
            'up_count': a['up'],
            'down_count': a['down'],
            'up_ratio': round((a['up'] / a['down']) if a['down'] > 0
                              else (float(a['up']) if a['up'] > 0 else 1.0), 4),
            'index_pct': round(m['index_pct'], 4),
            'above_ma20_pct': round(above, 4),
            'total_amount': round(a['amt'], 0),
            'avg_turnover': round(a['amt'] / n, 0),
            'avg_pct': round(avg, 4),
            'median_pct': round(med, 4),
            'strong_up_pct': round(su_pct, 4),
            'strong_down_pct': round(sd_pct, 4),
            'profit_score': round(sub['profit']),
            'speculation_score': round(sub['speculation']),
            'resilience_score': round(sub['resilience']),
            'trend_score': round(sub['trend']),
            'first_board': a['fb'],
            'ge2_count': a['ge2'],
            # v2.312 情绪周期驱动量
            'promo_rate': (None if promo is None else round(promo, 4)),
            'promo_pool': promo_pool,
            'ladder_completeness': round(ladder, 4),
        })
    # v2.312: 阶段判定（EMA 平滑 + 持续 2 日 + 大盘否决）—— 就地写 phase/phase_day
    try:
        from quant_terminal import regime_phase as _rp
        _rp.classify_phase_series(out)
    except Exception as _e:      # 阶段算不出来不许拖垮市场环境（五态照样能看）
        for r in out:
            r.setdefault('phase', '')
            r.setdefault('phase_label', '')
            r.setdefault('phase_day', 0)
        print('[regime_scan] classify_phase_series failed: %s' % _e)
    _C['lu_by_day'] = lu_by_day
    return out


# ── 磁盘缓存 ──────────────────────────────────────────────────
def _load_disk():
    try:
        with open(_CACHE_FILE, 'r', encoding='utf-8') as f:
            o = json.load(f)
        if isinstance(o, dict) and isinstance(o.get('rows'), list) and o['rows']:
            # v2.312: 老缓存没有 phase/first_board/ge2_count(恒 0) —— 必须作废重算,
            #   否则「情绪周期」页会拿一堆 first_board=0 的日子判出全冰点。
            if int(o.get('ver') or 0) != _CACHE_VER:
                return None
            return o
    except Exception:
        pass
    return None

def _save_disk(rows, latest):
    try:
        os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
        tmp = _CACHE_FILE + '.tmp'
        # v2.312: 连 lu_by_day 一起落盘 —— 主线排行靠它归组, 不落盘的话每次开机
        #   第一次点「情绪周期」要重扫 5589 个 CSV(约 20 秒)。
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'ts': time.time(), 'ver': _CACHE_VER, 'latest': latest,
                       'rows': rows, 'lu': _C.get('lu_by_day') or {}},
                      f, ensure_ascii=False)
        os.replace(tmp, _CACHE_FILE)
    except Exception:
        pass


def _ensure(days, force=False):
    """取 rows（升序）。缓存够用就直接切，不够就重算。"""
    with _LOCK:
        now = time.time()
        rows = _C.get('rows')
        if rows is None:
            disk = _load_disk()
            if disk and not force and (now - float(disk.get('ts') or 0)) < _CACHE_TTL:
                rows = disk['rows']
                _C['rows'] = rows
                _C['ts'] = float(disk.get('ts') or now)
                # v2.312: 主线排行要的 lu_by_day 也在磁盘缓存里, 一起接住
                if isinstance(disk.get('lu'), dict) and disk['lu']:
                    _C['lu_by_day'] = disk['lu']
        if rows is not None and not force and (now - _C['ts']) < _CACHE_TTL \
                and len(rows) >= days:
            return rows[-days:]
        rows = _compute(max(days, DAYS_DEF))
        if not rows:
            rows = (_C.get('rows') or [])
            return rows[-days:] if rows else []
        _C['rows'] = rows
        _C['ts'] = now
        _save_disk(rows, rows[-1]['date'])
        return rows[-days:]


# ── 对外接口（对齐 TSP /api/regime 的三个只读端点） ──────────
def history(days=DAYS_DEF, force=False):
    try:
        d = int(days)
    except (TypeError, ValueError):
        d = DAYS_DEF
    d = max(DAYS_MIN, min(DAYS_MAX, d))
    t0 = time.time()
    rows = _ensure(d, force)
    return {'ok': True, 'rows': rows, 'total': len(rows), 'days': d,
            'elapsed': round(time.time() - t0, 2)}


def latest(force=False):
    r = _ensure(DAYS_DEF, force)
    return {'ok': True, 'row': (r[-1] if r else None)}


def states(days=60, force=False):
    """状态分布（TSP /states 同口径：最近 days 天）。"""
    d = max(1, min(DAYS_MAX, int(days or 60)))
    rows = _ensure(max(d, DAYS_DEF), force)[-d:]
    total = len(rows)
    cnt = {}
    for r in rows:
        cnt[r['state']] = cnt.get(r['state'], 0) + 1
    order = ['strong', 'lean_strong', 'range', 'lean_weak', 'weak']
    dist = []
    for s in order:
        c = cnt.get(s, 0)
        if c <= 0:
            continue
        dist.append({'state': s, 'label': STATE_LABELS.get(s, s), 'count': c,
                     'pct': round(c / total * 100, 1) if total else 0})
    return {'ok': True, 'distribution': dist, 'days': total}


def coverage(force=False):
    rows = _C.get('rows') or []
    if not rows:
        rows = _ensure(DAYS_MAX, force)
    if not rows:
        return {'ok': True, 'earliest_date': None, 'latest_date': None, 'rows': 0}
    return {'ok': True, 'earliest_date': rows[0]['date'],
            'latest_date': rows[-1]['date'], 'rows': len(rows)}


# ── v2.312 情绪周期 / 主线排行（用户 m18508） ────────────────
def phases(days=DAYS_DEF, force=False, top=3):
    """阶段段列表 —— 对齐 TSP `/api/regime/phases`。"""
    d = max(DAYS_MIN, min(DAYS_MAX, int(days or DAYS_DEF)))
    rows = _ensure(d, force)
    ml = _mainline_rows(d, False)
    segs = regime_phase().phase_segments(rows, ml, top=top)
    return {'ok': True, 'segments': segs, 'total': len(segs),
            'rows': len(rows), 'days': d}


def phase_summary(days=DAYS_DEF, force=False):
    """情绪周期「阶段概览 + 阶段规律」要的一块数据。

    包含: 当前阶段/第N天/指导语、5 个驱动量（含历史分位 P）、
          当前这一段的持续天数、同阶段历史段数/平均/最长、历史去向分布。
    """
    d = max(DAYS_MIN, min(DAYS_MAX, int(days or DAYS_DEF)))
    rows = _ensure(d, force)
    if not rows:
        return {'ok': True, 'latest': None, 'cur': None, 'metrics': [],
                'hist': {}, 'next': [], 'dist': []}
    rp = regime_phase()
    ml = _mainline_rows(d, False)
    segs = rp.phase_segments(rows, ml, top=3)
    cur = segs[-1] if segs else None
    ph = (cur or {}).get('phase') or ''
    same = [s for s in segs if s['phase'] == ph]
    lasts = [s['days'] for s in same]
    # 当前段还没走完, 平均/最长按"历史已完结的段"算（TSP 同口径: 不把当前段算进去）
    done = [s['days'] for s in same[:-1]] or lasts
    avg_days = round(sum(done) / len(done), 1) if done else 0
    # 同阶段的下一阶段去向（用上一段之后接的那一段统计）
    nxt = {}
    for k in range(len(segs) - 1):
        if segs[k]['phase'] == ph:
            n2 = segs[k + 1]['phase']
            nxt[n2] = nxt.get(n2, 0) + 1
    tot = sum(nxt.values()) or 1
    nxt_list = sorted([{'phase': k, 'label': rp.PHASE_LABELS[k],
                        'pct': round(v / tot * 100, 1)}
                       for k, v in nxt.items()], key=lambda x: -x['pct'])
    last = rows[-1]
    metrics = _drivers(rows, last, rp)
    return {
        'ok': True,
        'latest': {'date': last['date'], 'phase': ph,
                   'label': rp.PHASE_LABELS.get(ph, ph),
                   'day': (cur or {}).get('days') or last.get('phase_day') or 0,
                   'guide': rp.PHASE_GUIDE.get(ph) or {},
                   'state': last.get('state'), 'score': last.get('score')},
        'cur': cur,
        'metrics': metrics,
        'hist': {'segments': len(same), 'avg_days': avg_days,
                 'max_days': max(lasts) if lasts else 0,
                 'over_avg': bool(cur and avg_days and cur['days'] > avg_days)},
        'next': nxt_list,
        'dist': rp.phase_distribution(rows),
        'rows': len(rows), 'days': d,
    }


def _drivers(rows, last, rp):
    """5 个驱动量 + 历史分位（P 值 = 该值在区间内的百分位，0~100）。"""
    def pctl(v, key):
        xs = [r.get(key) for r in rows if r.get(key) is not None]
        if not xs or v is None:
            return None
        k = sum(1 for x in xs if x <= v) / float(len(xs)) * 100.0
        return int(round(k))

    pr = last.get('promo_rate')
    lc = last.get('ladder_completeness')
    return [
        {'key': 'height', 'label': '市场高度', 'v': last.get('max_consecutive'),
         'txt': '%d 板' % int(last.get('max_consecutive') or 0),
         'p': pctl(last.get('max_consecutive'), 'max_consecutive'),
         'note': '全市场最高连板数'},
        {'key': 'first_board', 'label': '首板宽度', 'v': last.get('first_board'),
         'txt': '%d 家' % int(last.get('first_board') or 0),
         'p': pctl(last.get('first_board'), 'first_board'),
         'note': '当日首次封板(1 连板)家数'},
        {'key': 'ge2_count', 'label': '2板+宽度', 'v': last.get('ge2_count'),
         'txt': '%d 家' % int(last.get('ge2_count') or 0),
         'p': pctl(last.get('ge2_count'), 'ge2_count'),
         'note': '连板 2 及以上的家数'},
        {'key': 'promo_rate', 'label': '晋级率',
         'v': pr, 'txt': ('—' if pr is None else '%.1f%%' % (pr * 100)),
         'p': pctl(pr, 'promo_rate'),
         'note': '今日 2板+ ÷ 昨日涨停家数(池<10 记 —)'},
        {'key': 'ladder_completeness', 'label': '梯队完整度',
         'v': lc, 'txt': ('—' if lc is None else '%.0f%%' % (lc * 100)),
         'p': pctl(lc, 'ladder_completeness'),
         'note': '2..高度 各档位非空占比'},
    ]


def _mainline_rows(days, force=False, kind='concept'):
    """主线排行（当日现算，缓存按 (kind, days) 存内存 + 磁盘）。"""
    key = 'ml:%s:%d' % (kind, int(days))
    now = time.time()
    hit = _ML.get(key)
    if hit and not force and (now - hit[0]) < _CACHE_TTL:
        return hit[1]
    lu = _C.get('lu_by_day') or {}
    if not lu:
        disk = _load_disk()
        if isinstance(disk, dict) and isinstance(disk.get('lu'), dict):
            lu = disk['lu']
            _C['lu_by_day'] = lu
    rows = _ensure(days, force)
    if not lu or not rows:
        return []
    use = set(r['date'] for r in rows)
    lu2 = {d: v for d, v in lu.items() if d in use}
    try:
        from quant_terminal import board_rank as _br
        groups = _br._groups(kind, 2)
    except Exception:
        return []
    # v2.312 (2026-11-14 用户 m18508「查漏补缺」): 主线排行必须按**成分股家数**过滤。
    #   TSP market_mainline._resolve_filter_config() 默认 min_members=4 / max_members=600,
    #   compute_mainline_range 里先 group_by(kind).len() 再 semi join 掉越界板块, 理由是
    #   「宽基/风格标签(融资融券/沪深股通等数千成分)按成员数上限过滤」。
    #   不这么做的话「芯片概念(940 只)」「储能(908)」「一带一路(800)」这类超级宽基
    #   天天霸榜 —— 实测 2026-11-13 未过滤第 1 名是「芯片概念」(8 家涨停), 而 TSP 里
    #   同样是这一天, 第 1 名是「长安汽车概念」(5 家), 两边对不上就是这个原因。
    #   本地 concept map 成员数 min7/中位89/max940, 4~600 过滤后 351 → 341 个板块;
    #   industry 257 → 242(剔掉 15 个成员 <4 的极小行业)。过滤后前 10 名与 TSP 基本一致。
    rp = regime_phase()
    _fl = rp.MAINLINE_FILTER
    _mn = int(_fl.get('min_members') or 1)
    _mx = int(_fl.get('max_members') or 99999)
    _bl = set(_fl.get('blacklist') or [])
    if _mn > 1 or _mx < 99999 or _bl:
        groups = {m: v for m, v in groups.items()
                  if _mn <= len(v) <= _mx and m not in _bl}
    ml = rp.build_mainline(lu2, groups, kind=kind)
    _ML[key] = (now, ml)
    return ml


_ML = {}


def mainline(days=DAYS_DEF, kind='concept', top=10, force=False):
    """主线排行 —— 对齐 TSP `/api/regime/mainline`。"""
    d = max(DAYS_MIN, min(DAYS_MAX, int(days or DAYS_DEF)))
    kind = 'industry' if str(kind) == 'industry' else 'concept'
    rows = _mainline_rows(d, force, kind)
    keep = [r for r in rows if r.get('rank', 99) <= max(1, int(top or 10))]
    rp = regime_phase()
    return {'ok': True, 'rows': keep, 'leaders': rp.mainline_leaders(rows, top=10),
            'membership_note': rp.MEMBERSHIP_NOTE,
            'filter': rp.MAINLINE_FILTER, 'kind': kind,
            'total': len(keep), 'days': d}


def invalidate():
    with _LOCK:
        _C['rows'] = None
        _C['ts'] = 0.0
        _C.pop('lu_by_day', None)
    _ML.clear()


def cache_info():
    return {'ts': _C.get('ts') or 0, 'n': len(_C.get('rows') or []),
            'latest': (_C['rows'][-1]['date'] if _C.get('rows') else '')}


def regime_phase():
    """延迟导入 —— 免得 regime_phase 反过来 import 本模块时循环。"""
    from quant_terminal import regime_phase as _rp
    return _rp


# ── 自测 ──────────────────────────────────────────────────────
if __name__ == '__main__':
    t = time.time()
    force = '--force' in sys.argv
    h = history(250, force=force)
    print('rows=%d elapsed=%s total=%.1fs' % (h['total'], h['elapsed'], time.time() - t))
    if h['rows']:
        r = h['rows'][-1]
        print('latest %s %s(%d) 涨停%d 炸板%d 连板%d 封板%.3f 涨占比%.1f%% MA20上%.0f%% 指数%.2f%%'
              % (r['date'], STATE_LABELS[r['state']], r['score'], r['limit_up'],
                 r['broken_limit'], r['max_consecutive'], r['seal_rate'],
                 r['up_count'] / max(1, r['up_count'] + r['down_count']) * 100,
                 r['above_ma20_pct'] * 100, r['index_pct'] * 100))
        print('子分 赚钱%d 投机%d 抗跌%d 趋势%d'
              % (r['profit_score'], r['speculation_score'],
                 r['resilience_score'], r['trend_score']))
        cnt = {}
        for x in h['rows']:
            cnt[x['state']] = cnt.get(x['state'], 0) + 1
        print('区间分布', {STATE_LABELS[k]: v for k, v in cnt.items()})
