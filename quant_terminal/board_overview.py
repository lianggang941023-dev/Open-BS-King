# -*- coding: utf-8 -*-
"""
board_overview.py —— 「市场总览」大窗口的数据层 (v2.285, 2026-11-12 用户 m11544)

用户原话:
  「把TSP的"看板"界面也复刻过来，重命名为"市场总览"…… 舍弃界面中的四大指数因为我们
   顶栏有七大指数的数据了，然后监控中心也是个新功能……其他所有需要的代码，本地文件，
   实现方式等等你都可以照抄TSP」

从 TSP 照抄的:
  backend/app/services/market_overview_builder.py  (build_market_overview)
  backend/app/services/emotion_score.py            (compute 六维雷达)

不复刻的:
  ① 四大指数那一格 —— 用户明确说顶栏已有七大指数, 舍弃
  ② 个股/板块的预览弹窗链 —— 8799 已有 stk-win 居中窗, 复用

数据口径 (全部本地 + 已有实时快照, 不新增网络源):
  涨跌幅/现价    全市场实时快照 (datasource.fetch_realtime, 并发 8 批)
  量/额/换手/量比 本地 CSV 末 6 根 (volume 单位=手, 第 9 列=自由流通股本万股)
  MA5/20/60 + 60 日新高新低   本地 CSV 末 65 根
  涨停/跌停/炸板/连板梯队      fuyao_limit (同花顺涨停池)
  概念/行业热度    board_rank.rank() (当日涨幅口径, 已带启动预热)
  六大指数         get_market_indices() (只喂雷达的"指数"维度, 不在界面上显示)

  换手率 = 成交量 / 自由流通股本   (手 ÷ 万股 = %, 详见 auction_scan.turnover_pct)
  成交额 = 量(手) × 100 × (H+L+C)/3
  量比   = 今日量 / 前 5 日均量

缓存: 60 秒 (TSP staleTime 5s, 但 8799 是本地算, 60 秒足够且省 CPU)
      冷启动约 4 秒 (读 5589 个 CSV 各 65 行), 之后 0.01 秒
"""
import os
import sys
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from quant_terminal import datasource as _ds_mod  # noqa: E402

# v2.294 (2026-11-13 用户 m14431): 60 -> 1800。原来预热完 60 秒就过期,
#   用户过两分钟再点等于每次冷启(实测 8.5s)。本模块数据靠 CSV mtime / 交易日
#   切换自然失效, TTL 只防"长期无变化时用陈旧值"; 30 分钟足够。
#   注意: 盘中要实时的**价格**走 _realtime() 自己的 60 秒 TTL, 不受这里影响。
_TTL = 1800.0
_CACHE = {}
_LOCK = threading.Lock()

# 8 档涨跌分布 (与 TSP _pct_band_rows 一致; i>=4 是红)
BANDS = [
    ('\u2264-5%', -999.0, -5.0),
    ('-5~-3%', -5.0, -3.0),
    ('-3~-1%', -3.0, -1.0),
    ('-1~0%', -1.0, 0.0),
    ('0~1%', 0.0, 1.0),
    ('1~3%', 1.0, 3.0),
    ('3~5%', 3.0, 5.0),
    ('>5%', 5.0, 999.0),
]


# ---------------------------------------------------------------- 基础工具
def kline_dir():
    try:
        d = _ds_mod.get('local', 'kline_dir', '')
        if d:
            d = _ds_mod.resolve(d)
            if d and os.path.isdir(d):
                return d
    except Exception:
        pass
    return os.path.join(ROOT, 'kline_data', 'daily')


def _num(s):
    try:
        return float(s)
    except Exception:
        return 0.0


def _pref(code):
    """6 位码 -> 本地 CSV 前缀 (SH/SZ/BZ)"""
    c = str(code or '').strip()
    if len(c) > 2 and c[:2].upper() in ('SH', 'SZ', 'BJ', 'BZ'):
        c = c[2:]
    if c.startswith('920') or c[:1] in ('4', '8'):
        return 'BZ'
    if c[:1] in ('6', '9'):
        return 'SH'
    return 'SZ'


def _read_tail(path, n):
    """seek 倒读, 返回最后 n 行 (list[str]); 显式丢半行"""
    try:
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size <= 0:
                return []
            pos = size
            buf = b''
            while pos > 0 and buf.count(b'\n') <= n + 1:
                step = 65536 if pos >= 65536 else pos
                pos -= step
                f.seek(pos)
                buf = f.read(step) + buf
            lines = buf.decode('utf-8', 'ignore').split('\n')
            lines = [x.rstrip('\r') for x in lines if x.strip()]
            if pos > 0 and lines:
                lines = lines[1:]
            return lines[-n:] if len(lines) > n else lines
    except Exception:
        return []


def _avg(a):
    return (sum(a) / float(len(a))) if a else 0.0


def _median(a):
    if not a:
        return 0.0
    s = sorted(a)
    m = len(s) // 2
    return s[m] if len(s) % 2 else (s[m - 1] + s[m]) / 2.0


# ---------------------------------------------------------------- 实时快照
def _realtime():
    """{6位码: {name, price, pct, qdate}} —— 直接复用 board_rank 的 60 秒缓存快照。

    🔴 v2.285 踩坑: 我第一版自己拼裸 6 位码传给 ds.fetch_realtime, 而它要求
    `sh600206 / sz002973 / bj920157` 这种带交易所前缀的形式 (北交所必须 bj,
    920xxx 首位是 9 拼 sh 会返回空) ⇒ 112 批全部拿空, 且每批都要等超时,
    整体从 4 秒劣化到 21 秒, 而且 rt=0 让名字/涨幅全退回 CSV。
    board_rank._realtime_quotes() 已经把这套前缀规则 + 简称兜底 + 60 秒缓存
    都做对了 (v2.279 修过北交所), 直接复用, 不要再写第二份。
    """
    try:
        from quant_terminal import board_rank as _brd
        return _brd._realtime_quotes() or {}
    except Exception:
        return {}


# ---------------------------------------------------------------- 逐只事实
def _facts_one(fn):
    """单个 CSV -> 事实 dict (None 表示跳过)"""
    try:
        base = fn[:-4]
        if len(base) < 3:
            return None
        code = base[2:]
        if not code.isdigit():
            return None
        rows = _read_tail(os.path.join(kline_dir(), fn), 65)
        if len(rows) < 6:
            return None
        parsed = []
        for ln in rows:
            p = ln.split(',')
            if len(p) < 6:
                continue
            parsed.append((
                p[0], _num(p[1]), _num(p[2]), _num(p[3]), _num(p[4]), _num(p[5]),
                _num(p[7]) if len(p) > 7 else 0.0,
                _num(p[8]) if len(p) > 8 else 0.0,
            ))
        if len(parsed) < 6:
            return None
        last = parsed[-1]
        date = last[0]
        close = last[2]
        high = last[3]
        low = last[4]
        vol = last[5]
        if close <= 0 or vol <= 0:
            return None
        # 自由流通股本: 只在最新一行有值, 往前找最近一次非空
        free = 0.0
        for r in reversed(parsed):
            if r[7] > 0:
                free = r[7]
                break
        # 成交额 (元) = 量(手) × 100 × 均价
        amt = vol * 100.0 * ((high + low + close) / 3.0)
        # 换手率 % = 手 / 万股
        turn = (vol / free) if free > 0 else None
        # 量比 = 今日量 / 前 5 日均量 (不含今日)
        p5 = [r[5] for r in parsed[-6:-1]]
        v5 = _avg(p5)
        vr = (vol / v5) if v5 > 0 else None
        # 均线
        cl = [r[2] for r in parsed]
        ma5 = _avg(cl[-5:]) if len(cl) >= 5 else 0.0
        ma20 = _avg(cl[-20:]) if len(cl) >= 20 else 0.0
        ma60 = _avg(cl[-60:]) if len(cl) >= 60 else 0.0
        # 60 日新高/新低 —— 两种口径都算 (v2.400, 用户 m08806)
        #   ① 最高/最低价口径 (原有): 今日 high >= 前 60 根最高 / 今日 low <= 前 60 根最低
        #   ② 收盘价口径 (与 TSP 一致): 今日 close >= 前 60 根最高收盘 / close <= 前 60 根最低收盘
        prev = parsed[:-1]
        hi_new = lo_new = False
        hi_cls = lo_cls = False
        if len(prev) >= 20:
            ph = max(r[3] for r in prev[-60:])
            pl = min(r[4] for r in prev[-60:])
            if ph > 0 and high >= ph:
                hi_new = True
            if pl > 0 and low <= pl:
                lo_new = True
            ch = max(r[2] for r in prev[-60:])
            cl_ = min(r[2] for r in prev[-60:])
            if ch > 0 and close >= ch:
                hi_cls = True
            if cl_ > 0 and close <= cl_:
                lo_cls = True
        # 今日涨幅 (CSV 自己算, 快照缺失时兜底)
        pct_csv = 0.0
        if len(parsed) >= 2:
            pc = parsed[-2][2]
            if pc > 0:
                pct_csv = (close - pc) / pc * 100.0
        return {
            'code': code, 'date': date, 'close': close, 'vol': vol,
            'amt': amt, 'turn': turn, 'vr': vr, 'free': free,
            'ma5': ma5, 'ma20': ma20, 'ma60': ma60,
            'hi_new': hi_new, 'lo_new': lo_new,
            'hi_cls': hi_cls, 'lo_cls': lo_cls, 'pct_csv': pct_csv,
        }
    except Exception:
        return None


# ---------------------------------------------------------------- 情绪评分 (照抄 TSP emotion_score.py)
NEUTRAL = 50
SCORE_THRESHOLDS = {'strong': 70, 'warm': 55, 'neutral': 45, 'cold': 30}
SCORE_LABELS = (('strong', '强势'), ('warm', '偏暖'), ('neutral', '震荡'), ('cold', '偏冷'))

DIM_DEFS = [
    ('index', '指数', False, [('avg_index_pct', 1.0, -2.5, 2.5)]),
    ('profit', '赚钱', False, [
        ('up_pct', 0.45, 20.0, 80.0),
        ('avg_pct', 0.25, -2.0, 2.0),
        ('median_pct', 0.20, -2.0, 2.0),
        ('strong_diff_pct', 0.10, -8.0, 8.0),
    ]),
    ('money', '量能', False, [
        ('avg_vol_ratio', 0.70, 0.6, 1.8),
        ('high_vol_pct', 0.30, 2.0, 12.0),
    ]),
    ('speculation', '投机', False, [
        ('limit_up', 0.25, 5.0, 90.0),
        ('seal_rate', 0.35, 30.0, 85.0),
        ('max_boards', 0.25, 1.0, 8.0),
        ('tier2_count', 0.15, 0.0, 30.0),
    ]),
    ('resilience', '抗跌', True, [
        ('down_pct', 0.55, 20.0, 80.0),
        ('strong_down_pct', 0.45, 1.0, 12.0),
    ]),
    ('mainline', '主线', False, [
        ('mainline_avg', 0.65, -0.5, 3.0),
        ('mainline_cover_pct', 0.35, 1.0, 12.0),
    ]),
]


def _clamp(v, lo, hi):
    return lo if v < lo else (hi if v > hi else v)


def score_of(v, lo, hi):
    if v is None:
        return NEUTRAL
    if hi == lo:
        return NEUTRAL
    return int(round(_clamp((float(v) - lo) / (hi - lo) * 100.0, 0, 100)))


def _dim_value(children, metrics, invert):
    tot_w = 0.0
    tot = 0.0
    for key, w, lo, hi in children:
        val = metrics.get(key)
        if val is None:
            continue
        tot_w += w
        tot += w * score_of(val, lo, hi)
    if tot_w <= 0:
        v = NEUTRAL
    else:
        v = int(round(tot / tot_w))
    return (100 - v) if invert else v


def emotion(metrics):
    radar = []
    dims = []
    vals = []
    weights = []
    for key, label, invert, children in DIM_DEFS:
        v = _dim_value(children, metrics, invert)
        radar.append({'key': key, 'label': label, 'value': v})
        dims.append({'key': key, 'label': label, 'value': v})
        vals.append(v)
        weights.append(1.0)
    if len(set(weights)) == 1:
        score = int(round(sum(vals) / float(len(vals))))
    else:
        tw = sum(weights)
        score = int(round(sum(v * w for v, w in zip(vals, weights)) / tw)) if tw > 0 else NEUTRAL
    score = _clamp(score, 0, 100)
    label = '冰点'
    for k, tx in SCORE_LABELS:
        if score >= SCORE_THRESHOLDS[k]:
            label = tx
            break
    return {'score': score, 'label': label, 'radar': radar, 'dimensions': dims,
            'thresholds': SCORE_THRESHOLDS}


# ---------------------------------------------------------------- 涨停梯队
def _limit_block():
    """涨停/跌停/炸板/连板梯队 —— 全部容错, 拿不到就记进 errors

    🔴 v2.285 踩坑: 第一版 except 里只写 pass, 结果服务进程里 broken 恒为 0
    而命令行单测是 19 —— 因为异常被吞了, 界面上就是"炸板 0 / 封板率 100%",
    看着像数据正确其实整块错。**容错可以, 静默不行**: 失败原因必须带出去。
    """
    out = {'limit_up': 0, 'broken': 0, 'failed': 0, 'limit_down': 0,
           'max_boards': 0, 'seal_rate': 0.0, 'tiers': [], 'sealed_ready': False,
           'errors': []}
    try:
        from quant_terminal import fuyao_limit as _fl
    except Exception as e:
        out['errors'].append('import: %s' % e)
        return out
    date = ''
    try:
        date = _fl.default_pool_date()
    except Exception as e:
        out['errors'].append('date: %s' % e)
        return out
    out['date'] = date

    def _pull(fn, label):
        try:
            r = getattr(_fl, fn)(date)
        except Exception as e:
            out['errors'].append('%s: %s: %s' % (label, type(e).__name__, e))
            return []
        if isinstance(r, tuple):
            r = r[0]
        return r or []

    ups = _pull('limit_up_pool', 'up')
    brk = _pull('limit_break_pool', 'break')
    dwn = _pull('limit_down_pool', 'down')
    out['broken'] = len(brk)
    out['limit_down'] = len(dwn)
    out['limit_up'] = len(ups)
    up_n = out['limit_up']
    bd_n = out['broken']
    out['seal_rate'] = round(up_n / float(up_n + bd_n) * 100.0, 1) if (up_n + bd_n) > 0 else 0.0
    tiers = {}
    for s in ups:
        if not isinstance(s, dict):
            continue
        try:
            b = int(s.get('lianban') or 1)
        except Exception:
            b = 1
        if b < 1:
            b = 1
        tiers.setdefault(b, []).append({
            'code': str(s.get('code') or ''),
            'name': s.get('name') or '',
            'pct': _num(s.get('pct')),
        })
    out['max_boards'] = max(tiers.keys()) if tiers else 0
    out['tiers'] = [{'boards': b, 'count': len(tiers[b]),
                     'stocks': sorted(tiers[b], key=lambda x: -x['pct'])}
                    for b in sorted(tiers.keys(), reverse=True)]
    out['sealed_ready'] = up_n > 0
    return out


# ---------------------------------------------------------------- 板块热度
def _rank_block():
    """概念/行业热度 领涨领跌 —— 复用 board_rank (当日涨幅口径, 已预热)"""
    out = {
        'concept_rank': {'leading': [], 'lagging': []},
        'industry_rank': {'leading': [], 'lagging': []},
    }
    try:
        from quant_terminal import board_rank as _brd
    except Exception:
        return out
    for kind, key in (('concept', 'concept_rank'), ('industry', 'industry_rank')):
        try:
            r = _brd.rank(kind, 2, 'avgPct')
            rows = (r or {}).get('rows') or []
        except Exception:
            rows = []
        if not rows:
            continue
        up = [x for x in rows if (x.get('avg_pct') or 0) > 0]
        dn = [x for x in rows if (x.get('avg_pct') or 0) < 0]
        up.sort(key=lambda x: -(x.get('avg_pct') or 0))
        dn.sort(key=lambda x: (x.get('avg_pct') or 0))
        def _mk(x):
            top3 = x.get('top3') or []
            return {
                'name': x.get('name') or '',
                'count': x.get('count') or 0,
                'avg_pct': round(x.get('avg_pct') or 0, 2),
                'up_count': x.get('up_count') or 0,
                'down_count': x.get('down_count') or 0,
                'amount_yi': round(x.get('total_amt_yi') or 0, 1),
                'leader': (top3[0] if top3 else None),
                'top3': top3,
            }
        # v2.290 (用户 m13369 红线①): 领涨/领跌 由 top5 加到 top8
        out[key] = {'leading': [_mk(x) for x in up[:8]],
                    'lagging': [_mk(x) for x in dn[:8]]}
    return out


# ---------------------------------------------------------------- 指数 (只喂雷达)
def _index_avg():
    try:
        from quant_terminal import server as _srv  # 循环导入风险 -> 用函数内拿
        idx = _srv.get_market_indices()
    except Exception:
        idx = None
    if not idx:
        try:
            import importlib
            m = sys.modules.get('__main__')
            idx = getattr(m, 'get_market_indices', lambda: None)() if m else None
        except Exception:
            idx = None
    if not idx:
        return None
    want = ('sh000001', 'sz399001', 'sz399006', 'sh000688')
    vals = []
    for k in want:
        v = idx.get(k) if isinstance(idx, dict) else None
        if isinstance(v, dict) and v.get('change_pct') is not None:
            try:
                vals.append(float(v['change_pct']))
            except Exception:
                pass
    return _avg(vals) if vals else None


# ---------------------------------------------------------------- 主装配
def _build():
    d = kline_dir()
    try:
        files = [fn for fn in os.listdir(d) if fn.endswith('.csv')]
    except Exception:
        files = []
    facts = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(_facts_one, files):
            if r:
                facts.append(r)
    if not facts:
        return {'ok': False, 'error': '\u65e0\u672c\u5730 K \u7ebf'}

    rt = _realtime()
    date = max((f['date'] for f in facts), default='')

    # ---- 逐只合并 (涨跌幅/现价优先用实时快照, 拿不到用 CSV 自算)
    rows = []
    for f in facts:
        q = rt.get(f['code']) or {}
        pct = q.get('pct') if q.get('pct') is not None and q.get('name') else None
        if pct is None:
            pct = f['pct_csv']
        price = q.get('price') if q.get('price') else f['close']
        rows.append({
            'code': f['code'],
            'name': q.get('name') or '',
            'pct': float(pct or 0.0),
            'price': float(price or 0.0),
            'amt': f['amt'],
            'turn': f['turn'],
            'vr': f['vr'],
            'ma5': f['ma5'], 'ma20': f['ma20'], 'ma60': f['ma60'],
            'hi_new': f['hi_new'], 'lo_new': f['lo_new'],
            'hi_cls': f['hi_cls'], 'lo_cls': f['lo_cls'],
        })
    # 名字补齐 (快照没返回的, 用 sector_kline.tickers 兜底)
    miss = [r for r in rows if not r['name']]
    if miss:
        try:
            from quant_terminal import sector_kline as _sk
            tick = _sk.tickers() or {}
            for r in miss:
                nm = tick.get(r['code'])
                if nm:
                    r['name'] = nm
        except Exception:
            pass

    n = len(rows)
    pcts = [r['pct'] for r in rows]
    up = sum(1 for v in pcts if v > 0)
    down = sum(1 for v in pcts if v < 0)
    flat = n - up - down
    up_pct = up / float(n) * 100.0 if n else 0.0
    down_pct = down / float(n) * 100.0 if n else 0.0
    strong_up = sum(1 for v in pcts if v >= 3.0)
    strong_down = sum(1 for v in pcts if v <= -3.0)
    strong_diff = (strong_up - strong_down) / float(n) * 100.0 if n else 0.0

    # 8 档分布
    dist = []
    for label, lo, hi in BANDS:
        if label.startswith('\u2264'):
            c = sum(1 for v in pcts if v <= hi)
        elif label.startswith('>'):
            c = sum(1 for v in pcts if v > lo)
        else:
            c = sum(1 for v in pcts if lo < v <= hi)
        dist.append({'label': label, 'count': c,
                     'pct': round(c / float(n) * 100.0, 1) if n else 0.0})

    # 成交额 (亿)
    amts = [r['amt'] for r in rows if r['amt'] > 0]
    total_amt = sum(amts) / 1e8 if amts else 0.0
    avg_amt = _avg(amts) / 1e8 if amts else 0.0

    # 趋势强度
    a5 = sum(1 for r in rows if r['ma5'] > 0 and r['price'] >= r['ma5'])
    a20 = sum(1 for r in rows if r['ma20'] > 0 and r['price'] >= r['ma20'])
    a60 = sum(1 for r in rows if r['ma60'] > 0 and r['price'] >= r['ma60'])
    n5 = sum(1 for r in rows if r['ma5'] > 0)
    n20 = sum(1 for r in rows if r['ma20'] > 0)
    n60 = sum(1 for r in rows if r['ma60'] > 0)
    hi_new = sum(1 for r in rows if r['hi_new'])
    lo_new = sum(1 for r in rows if r['lo_new'])
    hi_cls = sum(1 for r in rows if r['hi_cls'])
    lo_cls = sum(1 for r in rows if r['lo_cls'])

    # 活跃度
    turns = [r['turn'] for r in rows if r['turn'] is not None]
    vrs = [r['vr'] for r in rows if r['vr'] is not None]
    high_turn = sum(1 for v in turns if v >= 5.0)
    high_vol = sum(1 for v in vrs if v >= 1.5)
    avg_turn = _avg(turns)
    avg_vr = _avg(vrs)
    high_vol_pct = high_vol / float(len(vrs)) * 100.0 if vrs else 0.0

    lim = _limit_block()
    rk = _rank_block()

    # 主线: 行业涨幅前 5 的成分股加权均涨 + 覆盖占比
    mainline_avg = None
    mainline_cover = None
    try:
        lead5 = rk.get('industry_rank', {}).get('leading') or []
        if lead5:
            cov = sum(x['count'] for x in lead5)
            wa = sum((x['avg_pct'] or 0) * (x['count'] or 0) for x in lead5)
            mainline_avg = (wa / float(cov)) if cov > 0 else None
            mainline_cover = cov / float(n) * 100.0 if n else None
    except Exception:
        pass

    metrics = {
        'avg_index_pct': _index_avg(),
        'up_pct': up_pct,
        'avg_pct': _avg(pcts),
        'median_pct': _median(pcts),
        'strong_diff_pct': strong_diff,
        'avg_vol_ratio': avg_vr if vrs else None,
        'high_vol_pct': high_vol_pct,
        'limit_up': lim['limit_up'],
        'seal_rate': lim['seal_rate'],
        'max_boards': lim['max_boards'],
        'tier2_count': sum(t['count'] for t in lim['tiers'] if t['boards'] >= 2),
        'down_pct': down_pct,
        'strong_down_pct': strong_down / float(n) * 100.0 if n else 0.0,
        'mainline_avg': mainline_avg,
        'mainline_cover_pct': mainline_cover,
    }
    emo = emotion(metrics)

    # v2.290 (用户 m13369 红线③): 榜单 top8 -> top10
    def _pick(key, desc, limit=10):
        rs = [r for r in rows if r['name']]
        rs.sort(key=(lambda x: -x[key]) if desc else (lambda x: x[key]))
        out = []
        for r in rs[:limit]:
            out.append({
                'code': r['code'], 'name': r['name'], 'price': round(r['price'], 2),
                'pct': round(r['pct'], 2),
                'amt_yi': round(r['amt'] / 1e8, 2) if r['amt'] else 0.0,
                'turn': round(r['turn'], 2) if r['turn'] is not None else None,
                'vr': round(r['vr'], 2) if r['vr'] is not None else None,
            })
        return out

    active = [r for r in rows if r['turn'] is not None and r['name']]
    active.sort(key=lambda x: -x['turn'])

    return {
        'ok': True,
        'date': date,
        'n_stock': n,
        'n_realtime': len(rt),
        'breadth': {
            'total': n, 'up': up, 'down': down, 'flat': flat,
            'up_pct': round(up_pct, 1), 'down_pct': round(down_pct, 1),
            'avg_pct': round(_avg(pcts), 2), 'median_pct': round(_median(pcts), 2),
            'strong_up': strong_up, 'strong_down': strong_down,
        },
        'amount': {'total': round(total_amt, 0), 'avg': round(avg_amt, 2)},
        'limit': lim,
        'distribution': dist,
        'trend': {
            'above_ma5': a5, 'above_ma20': a20, 'above_ma60': a60,
            'above_ma5_pct': round(a5 / float(n5) * 100.0, 1) if n5 else 0.0,
            'above_ma20_pct': round(a20 / float(n20) * 100.0, 1) if n20 else 0.0,
            'above_ma60_pct': round(a60 / float(n60) * 100.0, 1) if n60 else 0.0,
            'new_high': hi_new, 'new_low': lo_new,
            'new_high_close': hi_cls, 'new_low_close': lo_cls,   # v2.400 收盘价口径
        },
        'activity': {
            'avg_turnover': round(avg_turn, 1) if turns else 0.0,
            'high_turnover': high_turn,
            'high_vol_ratio': round(high_vol_pct, 1),
            'vol_ratio': round(avg_vr, 2) if vrs else 0.0,
        },
        'radar': emo['radar'],
        'emotion': {'score': emo['score'], 'label': emo['label'],
                    'dimensions': emo['dimensions'], 'thresholds': emo['thresholds']},
        'top_gainers': _pick('pct', True),
        'top_losers': _pick('pct', False),
        'turnover_leaders': _pick('amt', True),
        'active_leaders': [{
            'code': r['code'], 'name': r['name'], 'price': round(r['price'], 2),
            'pct': round(r['pct'], 2),
            'amt_yi': round(r['amt'] / 1e8, 2) if r['amt'] else 0.0,
            'turn': round(r['turn'], 2), 'vr': round(r['vr'], 2) if r['vr'] is not None else None,
        } for r in active[:10]],   # v2.290: 活跃换手 top8 -> top10
        'concept_rank': rk['concept_rank'],
        'industry_rank': rk['industry_rank'],
    }


def snapshot(force=False):
    now = time.time()
    with _LOCK:
        c = _CACHE.get('snap')
        if not force and c and (now - c['ts']) < _TTL:
            return c['d']
    t0 = time.time()
    try:
        d = _build()
    except Exception as e:
        d = {'ok': False, 'error': '%s: %s' % (type(e).__name__, e)}
    d['elapsed'] = round(time.time() - t0, 2)
    d['ts'] = time.strftime('%H:%M:%S')
    with _LOCK:
        _CACHE['snap'] = {'ts': time.time(), 'd': d}
    return d


def invalidate():
    with _LOCK:
        _CACHE.pop('snap', None)


def cache_info():
    with _LOCK:
        c = _CACHE.get('snap')
        if not c:
            return {'ready': False, 'ttl': _TTL}
        d = c['d']
        return {'ready': True, 'date': d.get('date'), 'n': d.get('n_stock'),
                'age': round(time.time() - c['ts'], 1), 'ttl': _TTL}


if __name__ == '__main__':
    import pprint
    t = time.time()
    d = snapshot(force=True)
    print('\u8017\u65f6 %.2fs  ok=%s' % (time.time() - t, d.get('ok')))
    print('date=%s n=%s rt=%s' % (d.get('date'), d.get('n_stock'), d.get('n_realtime')))
    print('\u6da8\u8dcc \u4e2a\u80a1 %s/%s/%s  \u4e0a\u6da8\u7387 %.1f%%  \u5747 %.2f%%  \u4e2d\u4f4d %.2f%%'
          % (d['breadth']['up'], d['breadth']['flat'], d['breadth']['down'],
             d['breadth']['up_pct'], d['breadth']['avg_pct'], d['breadth']['median_pct']))
    print('\u5f3a\u52bf/\u5f31\u52bf %s/%s' % (d['breadth']['strong_up'], d['breadth']['strong_down']))
    print('\u6210\u4ea4\u989d %.0f\u4ebf  \u5747\u989d %.2f\u4ebf' % (d['amount']['total'], d['amount']['avg']))
    print('\u6da8\u505c/\u8dcc\u505c %s/%s  \u5c01\u677f\u7387 %.1f%%  \u6700\u9ad8 %s\u677f'
          % (d['limit']['limit_up'], d['limit']['limit_down'], d['limit']['seal_rate'], d['limit']['max_boards']))
    print('\u8d8b\u52bf \u7ad9\u4e0aMA5 %.1f%% MA20 %.1f%% MA60 %.1f%%  \u65b0\u9ad8/\u65b0\u4f4e %s/%s'
          % (d['trend']['above_ma5_pct'], d['trend']['above_ma20_pct'], d['trend']['above_ma60_pct'],
             d['trend']['new_high'], d['trend']['new_low']))
    print('\u6d3b\u8dc3 \u5747\u6362\u624b %.1f%%  \u9ad8\u6362\u624b %s  \u653e\u91cf\u5360\u6bd4 %.1f%%  \u5747\u91cf\u6bd4 %.2f'
          % (d['activity']['avg_turnover'], d['activity']['high_turnover'],
             d['activity']['high_vol_ratio'], d['activity']['vol_ratio']))
    print('\u60c5\u7eea %s %s   \u96f7\u8fbe %s' % (d['emotion']['score'], d['emotion']['label'],
          ' '.join('%s%d' % (x['label'], x['value']) for x in d['radar'])))
    print('\u5206\u5e03 %s' % ' '.join('%s:%s' % (x['label'], x['count']) for x in d['distribution']))
    for key, nm in (('top_gainers', '\u6da8\u5e45\u699c'), ('top_losers', '\u8dcc\u5e45\u699c'),
                    ('turnover_leaders', '\u6210\u4ea4\u989d\u699c'), ('active_leaders', '\u6d3b\u8dc3\u6362\u624b')):
        print('%s %s' % (nm, ' | '.join('%s %s %+.2f%%' % (x['name'], x['code'], x['pct']) for x in d[key][:5])))
    for key, nm in (('concept_rank', '\u6982\u5ff5'), ('industry_rank', '\u884c\u4e1a')):
        ld = d[key]['leading']
        print('%s\u9886\u6da8 %s' % (nm, ' | '.join('%s %+.2f%%' % (x['name'], x['avg_pct']) for x in ld[:5])))
    print('\u7f13\u5b58 %s' % cache_info())
