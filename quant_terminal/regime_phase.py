# -*- coding: utf-8 -*-
"""情绪周期(phase) + 主线排行(mainline) —— 照抄 TSP 的判定引擎。

v2.312 (2026-11-14 用户 m18508): TSP「情绪周期」整页 + 「主线排行」移植。
用户原话:「TSP还有个"情绪周期"模块，也做进来……你说的主线排行在情绪周期页面的下半部分。
自然也要坐进来」

## 一、阶段判定（逐字照抄 `TSP/backend/app/services/market_phase.py`）
六阶段 冰点/启动/主升/高潮/退潮/修复，优先级
    climax > rally > ebb > ignite > ice > repair(兜底)
阈值全部是 TSP 用 2020-08~2026-08 全市场 1454 个交易日 p10/p60/p90 标定出来的，
**一个字都不许自己调**（改了就等于换了一套判据）。

驱动量 5 个（TSP 从 enriched 表的 consecutive_limit_ups 派生，我们从裸 K 线算连板数）：
    height               最高连板（= 全市场 consec 最大值）
    first_board          首板家数（consec == 1）
    ge2_count            2 板以上家数（consec >= 2）
    promo_rate           晋级率 = 今日 2 板以上家数 / 昨日涨停家数
                         （昨日连板池不足 10 家记 None —— 小样本噪声）
    ladder_completeness  梯队完整度 = （2..height 档位中非空档数）/ (height-1)，height>=3 才算

**晋级率的等价推导（重要，省一次全量重扫）**：
TSP 原文 `promo_ok = (prev_consec >= 1) & (consec == prev_consec + 1)`。
只要昨天涨停、今天涨停，consec 必然 +1 ⇒ `promo_ok` 恒等于「今日 consec >= 2 的家数」；
`promo_pool = prev_consec >= 1 家数` 恒等于「昨日涨停家数」。两条都对得上，见自测。

平滑与持续性：所有驱动量先过 EMA(alpha=1/3，约 5 日)；切换阶段要连续 2 日同标签
（`_CONFIRM_DAYS`），且**大盘五态落在 弱势/偏弱 时，正面阶段(高潮/主升/启动)一律否决成修复**。

## 二、主线排行（照抄 `market_mainline.py`）
每日按概念/行业把「当日涨停股」归组，算四维截面分：
    limit_up_count .35 / max_boards .25 / rungs_filled .25 / ge2_count .15
每一维先做**当日截面 rank 归一(0~1)**（分母 `max(n-1,1)`），再加权成 0~100 分；
单概念当日涨停 < 3 家不参与排名（`_MIN_LIMIT_UP`）；每日只留前 30 名。
行业主线取前两级（`计算机-软件开发` 这种），我们本地 map 已是细分名，直接用。

与 TSP 的差异（**不改，只说明**）：
- TSP 的 `exclude_st` 按维表名称含 ST 剔除；我们本地没有名称维表，**不剔 ST**。
- TSP 的 `membership_note` 说的是「概念成分为当前快照回看历史」。我们同理 ——
  `kline_data` 里的行业/概念 map 只有一份当前快照，历史归属会漂移。
"""
from __future__ import annotations

import math

# ───────────────────────── 阶段词汇 ─────────────────────────
PHASE_ICE = 'ice'
PHASE_IGNITE = 'ignite'
PHASE_RALLY = 'rally'
PHASE_CLIMAX = 'climax'
PHASE_EBB = 'ebb'
PHASE_REPAIR = 'repair'

PHASE_LABELS = {
    PHASE_ICE: '冰点',
    PHASE_IGNITE: '启动',
    PHASE_RALLY: '主升',
    PHASE_CLIMAX: '高潮',
    PHASE_EBB: '退潮',
    PHASE_REPAIR: '修复',
}
PHASE_COLORS = {
    PHASE_ICE: '#10b981',
    PHASE_IGNITE: '#3b82f6',
    PHASE_RALLY: '#ef4444',
    PHASE_CLIMAX: '#a855f7',
    PHASE_EBB: '#f59e0b',
    PHASE_REPAIR: '#6b7280',
}
# 展示顺序（图例 / 时间轴色条 / 阶段分布都用它）
PHASE_ORDER = (PHASE_ICE, PHASE_IGNITE, PHASE_RALLY, PHASE_CLIMAX, PHASE_EBB, PHASE_REPAIR)

# 阶段含义与应对提示 —— 照抄 TSP Regime.tsx 的 MARKET_PHASE_GUIDE
PHASE_GUIDE = {
    PHASE_ICE: {'meaning': '高度/宽度/首板同时贴地, 亏钱效应极致',
                'action': '观望 · 跟踪率先异动股'},
    PHASE_IGNITE: {'meaning': '低位放量扩张, 晋级率回升',
                   'action': '试仓主线 · 快进快出'},
    PHASE_RALLY: {'meaning': '高度+宽度+晋级率共振',
                  'action': '持股 · 顺势而为'},
    PHASE_CLIMAX: {'meaning': '情绪极端宣泄, 批量二板+',
                   'action': '逐步兑现 · 不追高'},
    PHASE_EBB: {'meaning': '自高位回落, 晋级率坍塌',
                'action': '防守 · 不接力'},
    PHASE_REPAIR: {'meaning': '多空拉锯, 无明确方向',
                   'action': '轻仓试错 · 控回撤'},
}

# ───────────────────────── 阈值（TSP 原样） ─────────────────────────
CLIMAX_GE2 = 50
CLIMAX_FIRST_BOARD = 220
RALLY_HEIGHT = 7
RALLY_GE2 = 15
RALLY_PROMO = 0.23
RALLY_PROMO_ALT = 0.30
RALLY_GE2_ALT = 12
RALLY_HEIGHT_ALT = 5
EBB_PROMO = 0.15
EBB_PROMO_STRICT = 0.13
EBB_SEAL = 0.57
EBB_RECENT_GE2 = 12
EBB_RECENT_HEIGHT = 6
IGNITE_GE2_DELTA = 3
IGNITE_GE2 = 8
IGNITE_PROMO = 0.20
IGNITE_HEIGHT_DELTA = 1
IGNITE_HEIGHT = 5
IGNITE_PROMO_SOFT = 0.19
ICE_HEIGHT = 4
ICE_GE2 = 6
ICE_FIRST_BOARD = 24
PROMO_MIN_POOL = 10

_EMA_ALPHA = 1.0 / 3.0
_CONFIRM_DAYS = 2
_POSITIVE_PHASES = (PHASE_CLIMAX, PHASE_RALLY, PHASE_IGNITE)
_VETO_STATES = ('weak', 'lean_weak')

# ───────────────────────── 主线排行常量（TSP 原样） ─────────────────────────
MEMBERSHIP_NOTE = ('概念成分为当前快照回看历史(本地只有一份当前 map, 无历史版本), '
                   '早年主线存在归属漂移, 越近越准')
MAINLINE_TOP_PER_DAY = 30      # 每日持久化的主线数(按分数截断)
MIN_LIMIT_UP = 3               # 单概念当日最少涨停家数
SCORE_WEIGHTS = {
    'limit_up_count': 0.35,
    'max_boards': 0.25,
    'rungs_filled': 0.25,
    'ge2_count': 0.15,
}
MAINLINE_FILTER = {'min_members': 4, 'max_members': 600, 'blacklist': []}


# ───────────────────────── EMA / 阶段判定 ─────────────────────────
def ema(values, alpha=_EMA_ALPHA):
    """TSP `_ema` 逐位实现：None 用上一平滑值 ffill；末尾把首个有效值前向回填。"""
    out = []
    cur = None
    for v in values:
        bad = (v is None) or (isinstance(v, float) and v != v)
        if bad:
            out.append(cur)          # cur 仍是 None 时就是 None，末尾统一回填
        else:
            cur = v if cur is None else cur + alpha * (v - cur)
            out.append(cur)
    first = None
    for i, v in enumerate(out):
        if v is not None:
            first = i
            break
    if first is None:
        return [0.0] * len(values)
    for i in range(first):
        out[i] = out[first]
    return out


def _raw_phase(i, ge2, height, fb, promo, seal, g2_prev, h_prev):
    """单日原始阶段（未做持续性/大盘否决）—— 判定顺序与 TSP 一字不差。"""
    if ge2 >= CLIMAX_GE2 or fb >= CLIMAX_FIRST_BOARD:
        return PHASE_CLIMAX
    if height >= RALLY_HEIGHT and ge2 >= RALLY_GE2 and promo is not None and promo >= RALLY_PROMO:
        return PHASE_RALLY
    if promo is not None and promo >= RALLY_PROMO_ALT and ge2 >= RALLY_GE2_ALT and height >= RALLY_HEIGHT_ALT:
        return PHASE_RALLY
    # 冰点必须排在退潮之前 —— 贴地的日子不能判成"自高位退潮"
    if height <= ICE_HEIGHT and ge2 <= ICE_GE2 and fb <= ICE_FIRST_BOARD:
        return PHASE_ICE
    from_high = (g2_prev >= EBB_RECENT_GE2) or (h_prev >= EBB_RECENT_HEIGHT)
    if from_high and promo is not None and promo <= EBB_PROMO and ge2 < g2_prev:
        return PHASE_EBB
    if promo is not None and promo <= EBB_PROMO_STRICT and seal is not None and seal <= EBB_SEAL:
        return PHASE_EBB
    if ge2 - g2_prev >= IGNITE_GE2_DELTA and ge2 >= IGNITE_GE2 and promo is not None and promo >= IGNITE_PROMO:
        return PHASE_IGNITE
    if height - h_prev >= IGNITE_HEIGHT_DELTA and height >= IGNITE_HEIGHT and promo is not None and promo >= IGNITE_PROMO_SOFT:
        return PHASE_IGNITE
    return PHASE_REPAIR


def classify_phase_series(rows):
    """给升序 rows（含 first_board/ge2_count/max_consecutive/promo_rate/seal_rate/state）
    就地写入 `phase` / `phase_day`（当前阶段第几天），返回 rows。

    `phase_day` 是我方为了前端「第 N 天」现算的（TSP 前端自己数），口径一致：
    同一阶段连续出现记 1,2,3…，换阶段从 1 重新数。
    """
    n = len(rows)
    if n <= 0:
        return rows
    ge2_s = ema([r.get('ge2_count') or 0 for r in rows])
    h_s = ema([r.get('max_consecutive') or 0 for r in rows])
    fb_s = ema([r.get('first_board') or 0 for r in rows])
    pr_s = ema([r.get('promo_rate') for r in rows])

    cur = None
    pending = None
    pending_run = 0
    day = 0
    for i in range(n):
        r = rows[i]
        ge2 = ge2_s[i] or 0.0
        h = h_s[i] or 0.0
        fb = fb_s[i] or 0.0
        pr = pr_s[i]
        seal = r.get('seal_rate')
        g2_prev = ge2_s[max(0, i - 5)] or 0.0
        h_prev = h_s[max(0, i - 5)] or 0.0
        raw = _raw_phase(i, ge2, h, fb, pr, seal, g2_prev, h_prev)

        # ① 大盘否决：五态落在弱势/偏弱时，正面阶段一律按修复处理
        if r.get('state') in _VETO_STATES and raw in _POSITIVE_PHASES:
            raw = PHASE_REPAIR
        # ② 持续性：连续 _CONFIRM_DAYS 日同标签才切换
        if cur is None:
            cur = raw
            pending = None
            pending_run = 0
        elif raw == cur:
            pending = None
            pending_run = 0
        else:
            if pending == raw:
                pending_run += 1
            else:
                pending = raw
                pending_run = 1
            if pending_run >= _CONFIRM_DAYS:
                cur = raw
                pending = None
                pending_run = 0

        if i > 0 and rows[i - 1].get('phase') == cur:
            day = (rows[i - 1].get('phase_day') or 0) + 1
        else:
            day = 1
        r['phase'] = cur
        r['phase_label'] = PHASE_LABELS.get(cur, cur)
        r['phase_day'] = day
    return rows


# ───────────────────────── 阶段段（阶段概览 / 阶段×主线表） ─────────────────────────
def _avg(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else None


def phase_segments(rows, ml_rows=None, top=3):
    """连续同 phase 合段。字段对齐 TSP `/api/regime/phases` 的 segments。"""
    segs = []
    i = 0
    n = len(rows)
    while i < n:
        ph = rows[i].get('phase')
        j = i
        while j + 1 < n and rows[j + 1].get('phase') == ph:
            j += 1
        part = rows[i:j + 1]
        start, end = part[0]['date'], part[-1]['date']
        segs.append({
            'phase': ph,
            'label': PHASE_LABELS.get(ph, ph),
            'start': start,
            'end': end,
            'days': len(part),
            'avg_height': _r1(_avg([x.get('max_consecutive') for x in part])),
            'avg_first_board': _r1(_avg([x.get('first_board') for x in part])),
            'avg_ge2': _r1(_avg([x.get('ge2_count') for x in part])),
            'avg_promo': _r3(_avg([x.get('promo_rate') for x in part])),
            'avg_seal_rate': _r3(_avg([x.get('seal_rate') for x in part])),
            'top_mainlines': segment_mainlines(ml_rows, start, end, top=top),
        })
        i = j + 1
    return segs


def segment_mainlines(ml_rows, start, end, top=3):
    """段内 top 主线：只看 rank<=5，按 (top5 天数, 分数和) 降序取 top。"""
    if not ml_rows:
        return []
    bucket = {}
    for r in ml_rows:
        if r.get('rank', 99) > 5:
            continue
        d = r.get('date') or ''
        if not (start <= d <= end):
            continue
        m = r['member']
        b = bucket.get(m)
        if b is None:
            b = bucket[m] = {'member': m, 'dates': set(), 'score_sum': 0.0,
                             'max_boards': 0, 'leader': r.get('leader_symbol') or ''}
        b['dates'].add(d)
        b['score_sum'] += float(r.get('score') or 0.0)
        if (r.get('max_boards') or 0) > b['max_boards']:
            b['max_boards'] = r.get('max_boards') or 0
    items = sorted(bucket.values(), key=lambda b: (-len(b['dates']), -b['score_sum']))[:top]
    return [{'member': b['member'], 'top5_days': len(b['dates']),
             'score_sum': round(b['score_sum'], 1),
             'max_boards': b['max_boards'],
             'leader_symbol': b['leader']} for b in items]


def _r1(v):
    return None if v is None else round(v, 1)


def _r3(v):
    return None if v is None else round(v, 3)


# ───────────────────────── 主线排行 ─────────────────────────
def build_mainline(lu_by_day, groups, kind='concept', days=None):
    """lu_by_day: {date: [(code, consec, amount), ...]}；groups: {板块名: [code...]}

    返回每日 rank<=MAINLINE_TOP_PER_DAY 的行（升序 date、rank 升序）。
    四维先做**当日截面 rank 归一**再加权 —— 与 TSP 的 polars 实现同口径。
    """
    code2m = {}
    for m, codes in groups.items():
        for c in codes:
            code2m.setdefault(c, []).append(m)

    out = []
    dates = sorted(lu_by_day.keys())
    if days:
        dates = dates[-int(days):]
    for d in dates:
        lst = lu_by_day.get(d) or []
        if not lst:
            continue
        agg = {}
        for code, consec, amt in lst:
            for m in code2m.get(code, ()):
                a = agg.get(m)
                if a is None:
                    a = agg[m] = {'n': 0, 'ge2': 0, 'maxb': 0, 'rungs': set(),
                                  'leader': '', 'leader_key': (-1, -1.0)}
                a['n'] += 1
                if consec >= 2:
                    a['ge2'] += 1
                    a['rungs'].add(consec)
                if consec > a['maxb']:
                    a['maxb'] = consec
                k = (consec, amt)
                if k > a['leader_key']:
                    a['leader_key'] = k
                    a['leader'] = code
        rows = [{'member': m, 'limit_up_count': a['n'], 'ge2_count': a['ge2'],
                 'max_boards': a['maxb'], 'rungs_filled': len(a['rungs']),
                 'leader_symbol': a['leader']}
                for m, a in agg.items() if a['n'] >= MIN_LIMIT_UP]
        if not rows:
            continue
        cnt = len(rows)
        denom = max(cnt - 1, 1)
        # 截面 rank（average，1 起），归一成 0~1
        for col in SCORE_WEIGHTS:
            vals = sorted(r[col] for r in rows)
            rank_of = {}
            i = 0
            while i < len(vals):
                j = i
                while j + 1 < len(vals) and vals[j + 1] == vals[i]:
                    j += 1
                avg_rank = (i + j) / 2.0 + 1.0
                rank_of[vals[i]] = (avg_rank - 1.0) / denom
                i = j + 1
            for r in rows:
                r['_' + col] = rank_of[r[col]]
        for r in rows:
            r['score'] = round(100.0 * sum(SCORE_WEIGHTS[c] * r['_' + c]
                                           for c in SCORE_WEIGHTS), 2)
        rows.sort(key=lambda r: (-r['score'], r['member']))
        keep = rows[:MAINLINE_TOP_PER_DAY]
        for k, r in enumerate(keep, 1):
            out.append({
                'date': d, 'kind': kind, 'member': r['member'],
                'limit_up_count': r['limit_up_count'], 'ge2_count': r['ge2_count'],
                'max_boards': r['max_boards'], 'rungs_filled': r['rungs_filled'],
                'leader_symbol': r['leader_symbol'], 'score': r['score'], 'rank': k,
            })
    return out


def mainline_leaders(ml_rows, top=10):
    """rank==1 的板块按 top1 天数聚合 —— TSP `/api/regime/mainline` 的 leaders。"""
    bucket = {}
    for r in ml_rows:
        if r.get('rank') != 1:
            continue
        m = r['member']
        b = bucket.get(m)
        if b is None:
            b = bucket[m] = {'member': m, 'top1_days': 0, 'score_sum': 0.0, 'max_boards': 0}
        b['top1_days'] += 1
        b['score_sum'] += float(r.get('score') or 0.0)
        if (r.get('max_boards') or 0) > b['max_boards']:
            b['max_boards'] = r.get('max_boards') or 0
    items = sorted(bucket.values(), key=lambda b: (-b['top1_days'], -b['score_sum']))[:top]
    return [{'member': b['member'], 'top1_days': b['top1_days'],
             'avg_score': round(b['score_sum'] / max(1, b['top1_days']), 1),
             'max_boards': b['max_boards']} for b in items]


def phase_distribution(rows):
    """六阶段分布（概率/天数），用于「阶段规律 · 历史去向」。"""
    cnt = {}
    for r in rows:
        p = r.get('phase')
        cnt[p] = cnt.get(p, 0) + 1
    total = len(rows) or 1
    return [{'phase': p, 'label': PHASE_LABELS[p], 'count': cnt.get(p, 0),
             'pct': round(cnt.get(p, 0) / total * 100, 1)}
            for p in PHASE_ORDER if cnt.get(p, 0) > 0]


if __name__ == '__main__':
    # 自测：造 12 天假数据，看阶段判定与晋级率等价推导是否成立
    demo = []
    for i in range(12):
        demo.append({
            'date': '2026-01-%02d' % (i + 1),
            'first_board': 40 + i * 3, 'ge2_count': 5 + i,
            'max_consecutive': 3 + (i // 4), 'promo_rate': 0.10 + i * 0.02,
            'seal_rate': 0.6, 'state': 'range',
        })
    classify_phase_series(demo)
    for r in demo:
        print(r['date'], r['phase_label'], '第%d天' % r['phase_day'])
    print('ema([1,None,3]) =', ema([1, None, 3.0]))
    print('segments =', [(s['label'], s['start'], s['end'], s['days'])
                         for s in phase_segments(demo)])
