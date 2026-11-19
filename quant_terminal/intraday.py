# -*- coding: utf-8 -*-
"""v2.152 (2026-10-18): 盘中/盘后临时快照 —— K线 CSV 之上的「当日覆盖层」

为什么要有它（用户 2026-10-18 定案 m07888）:
  通达信自动任务在 11:35 / 15:35 拉全市场快照。11:35 那次**不能写 daily/*.csv**
  （盘中写盘会毒化历史文件，且收盘前数据还会变），但「更新板块」又必须用最新数据。
  ⇒ 落这个独立快照文件，由板块/选股计算时作为**临时覆盖层**读入，收盘后自然被
    CSV 取代（15:35 那次同时写 CSV，此后快照与 CSV 等价）。

除权的处理（关键）:
  本地 CSV 是**前复权**的（历史价格已按过去的各次除权缩放过），但它只反映**已经发生过**
  的除权 —— 今天新发生的这次它还不知道。若今天有除权，直接追加今日这根K线会在序列里
  留一个假跳空（10送10 就是从 20 元「跌」到 10 元，-50%），板块涨幅会全错。
  ⇒ 用当日的除权因子 f = 昨收(除权后) / CSV末行收盘(除权前的旧尺度)，
  把**历史**OHLC 全乘 f，把历史换到今日尺度，再追加今日这根。
  ⚠️ 成交量**不换算**（v2.159 定案）：与 server.py 写 CSV 的口径一致，通达信官方前复权
  导出也只调价格不动成交量。这里除 f 会让 11:35 与 15:35 两个面板的量比不一致。

三种日期情形（都不会静默出错）:
  快照日期 > CSV末行  → 追加今日这根（11:35 / 15:35 的正常情形）
  快照日期 = CSV末行  → 替换最后一根（CSV 已写过今天，再刷一次）
  快照日期 < CSV末行  → 原样返回（快照过期，绝不回退数据）
"""
import json
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FILE = os.path.join(HERE, 'intraday_snapshots.json')
# v2.159: 除权事件日志 —— {日期: {代码: {factor, prev, last_close}}}
LOG_FILE = os.path.join(HERE, 'exrights_log.json')

# 快照有效期: 超过这个时长就不认（防「昨天抓的快照」被当成今天的用）
MAX_AGE_SEC = 20 * 3600

_CACHE = {'key': None, 'data': None, 'gen': ''}


# ---------------------------------------------------------------- 读写
def path():
    return FILE


def save(result, source='tdx'):
    """把 tdx_fetch.do_snapshot() 的结果压成快照文件（原子写）

    压缩后每只股票只 5 个固定短键 + 3 个可选键:
      o/h/l/c/v = 开/高/低/收/量（固定）
      f = 除权因子、z = 总市值、lz = 流通市值（有值才写）
    """
    st = result.get('stocks') or {}
    out = {}
    for code, v in st.items():
        try:
            row = {
                'o': round(float(v.get('open') or 0), 3),
                'h': round(float(v.get('high') or 0), 3),
                'l': round(float(v.get('low') or 0), 3),
                'c': round(float(v.get('close') or 0), 3),
                'v': round(float(v.get('volume') or 0), 0),
            }
        except (TypeError, ValueError):
            continue
        f = v.get('factor')
        if f is None:
            f = v.get('f')       # 已压过的快照再存一次时是短键
        if f:
            try:
                row['f'] = round(float(f), 6)
            except (TypeError, ValueError):
                pass
        # 市值 (亿元) —— 写 CSV 第 7/8 列要用, 顺带存进快照省一次联网
        try:
            z = float(v.get('mcap') or 0)
            if z > 0:
                row['z'] = round(z, 2)
            lz = float(v.get('fmcap') or 0)
            if lz > 0:
                row['lz'] = round(lz, 2)
        except (TypeError, ValueError):
            pass
        out[code] = row
    doc = {
        'version': 1,
        'trade_date': result.get('trade_date') or '',
        'hq_date': result.get('hq_date') or '',
        'captured_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'captured_ts': time.time(),
        'source': source,
        'count': len(out),
        'suspects': result.get('suspects', 0),
        'exrights': result.get('exrights') or {},
        'elapsed': result.get('elapsed'),
        'stocks': out,
    }
    tmp = FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(doc, fh, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, FILE)
    _CACHE['key'] = None          # 强制下次重新读
    # v2.159: 除权事件单独存一份小日志（一年几十条，可复盘可追查）
    try:
        log_exrights(doc.get('trade_date') or '', doc.get('exrights') or {})
    except Exception:
        pass
    return doc


def log_exrights(trade_date, exrights):
    """把今天的除权事件追加进 exrights_log.json

    为什么不把「昨收」写成 CSV 的第 9 列:
      · 历史行根本填不上（三家数据源都给不出「每根K线各自的昨收」）
      · 一年除权只有几十条，日志比字段小三个数量级
      · 不动 CSV 就零风险（加列要重写 773 万行）
    """
    if not trade_date or not exrights:
        return
    doc = {}
    try:
        with open(LOG_FILE, encoding='utf-8') as fh:
            doc = json.load(fh) or {}
    except Exception:
        doc = {}
    doc[trade_date] = {k: v for k, v in exrights.items()}
    tmp = LOG_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=1, sort_keys=True)
    os.replace(tmp, LOG_FILE)
    return doc


def _stat_key():
    try:
        st = os.stat(FILE)
        return (st.st_mtime_ns, st.st_size)
    except OSError:
        return None


def _snap():
    """内存里缓存一份（按 mtime+size 失效）—— 板块计算会逐只调用，不能每次都读盘"""
    key = _stat_key()
    if key is None:
        _CACHE.update({'key': None, 'data': None, 'gen': ''})
        return None
    if _CACHE['key'] == key:
        return _CACHE['data']
    data = None
    try:
        with open(FILE, encoding='utf-8') as fh:
            data = json.load(fh)
    except Exception:
        data = None
    if data:
        # 过期就不认（防昨天的快照被当成今天的）
        try:
            age = time.time() - float(data.get('captured_ts') or 0)
            if age > MAX_AGE_SEC:
                data = None
        except (TypeError, ValueError):
            pass
    gen = ''
    if data:
        gen = f"{data.get('trade_date', '')}|{data.get('captured_ts', 0)}"
    _CACHE.update({'key': key, 'data': data, 'gen': gen})
    return data


def gen():
    """快照版本标记 —— 塞进板块缓存 key，快照变了缓存自然失效（空串=没有快照）"""
    _snap()
    return _CACHE['gen']


def valid():
    d = _snap()
    return bool(d and d.get('stocks'))


# ---------------------------------------------------------------- 对外
def prices():
    """{6位代码: 收盘价} —— 与 engine 现成的 realtime_prices 同构（只含今日有数据的）"""
    d = _snap()
    if not d:
        return None
    out = {}
    for code, r in (d.get('stocks') or {}).items():
        try:
            c = float(r.get('c') or 0)
        except (TypeError, ValueError):
            continue
        if c > 0:
            out[code] = c
    return out or None


def bars():
    """{6位代码: {date,open,close,high,low,volume,mcap,fmcap}} —— 需要完整 OHLCV 的调用方用"""
    d = _snap()
    if not d:
        return None
    td = d.get('trade_date') or ''
    out = {}
    for code, r in (d.get('stocks') or {}).items():
        try:
            series = {'date': td,
                      'open': float(r.get('o') or 0),
                      'close': float(r.get('c') or 0),
                      'high': float(r.get('h') or 0),
                      'low': float(r.get('l') or 0),
                      'volume': float(r.get('v') or 0),
                      'mcap': float(r.get('z') or 0),
                      'fmcap': float(r.get('lz') or 0),
                      # v2.159: 写 CSV 时要把历史行价格乘上它
                      'factor': float(r.get('f') or 0)}
        except (TypeError, ValueError):
            continue
        if series['close'] > 0:
            out[code] = series
    return out or None


def caps():
    """{6位代码: (总市值, 流通市值)} 亿元 —— 写 CSV 第7/8列用；缺失的票不在字典里"""
    d = _snap()
    if not d:
        return None
    out = {}
    for code, r in (d.get('stocks') or {}).items():
        z = r.get('z') or 0
        lz = r.get('lz') or 0
        if z or lz:
            try:
                out[code] = (float(z), float(lz))
            except (TypeError, ValueError):
                continue
    return out or None


def apply(tail, code):
    """把快照叠到某只股票的日K尾部 → 新的 tail（不改原列表）

    返回的列表与入参同构 [{date,open,close,high,low,volume}, ...]
    """
    d = _snap()
    if not d:
        return tail
    st = (d.get('stocks') or {}).get(code)
    if not st:
        return tail
    td = (d.get('trade_date') or '').strip()
    if len(td) != 10:
        return tail
    try:
        c = float(st.get('c') or 0)
    except (TypeError, ValueError):
        return tail
    if c <= 0:
        return tail               # 停牌
    last_date = tail[-1]['date'] if tail else ''
    if not last_date:
        return tail
    if td < last_date:
        return tail               # 快照过期 ⇒ 绝不回退
    # 今日有除权: 历史先换到今日尺度（否则序列里留下假跳空）
    f = 0.0
    try:
        f = float(st.get('f') or 0)
    except (TypeError, ValueError):
        f = 0.0
    base = tail
    if f > 0 and abs(f - 1.0) > 1e-9:
        base = []
        for r in tail:
            # v2.159 (2026-10-20): **成交量不换算** —— 与 server.py 写 CSV 的口径保持一致
            #   （那边只乘价格 4 列）。以前这里写了 volume / f，导致同一个交易日的
            #   11:35 面板（走这里）与 15:35 面板（走 CSV）量比不一致。实测通达信官方
            #   前复权导出也只调整价格、成交量原样保留。
            base.append({'date': r['date'],
                         'open': r['open'] * f,
                         'close': r['close'] * f,
                         'high': r['high'] * f,
                         'low': r['low'] * f,
                         'volume': r['volume']})
    bar = {'date': td,
           'open': float(st.get('o') or c),
           'close': c,
           'high': float(st.get('h') or c),
           'low': float(st.get('l') or c),
           'volume': float(st.get('v') or 0)}
    if td == last_date:
        out = base[:-1]
        out.append(bar)
        return out
    out = list(base)
    out.append(bar)
    return out


def status():
    """给前端/接口看的摘要（不含 5571 行的明细）"""
    d = _snap()
    if not d:
        return {'ok': False, 'exists': os.path.exists(FILE), 'msg': '暂无临时快照'}
    return {
        'ok': True,
        'exists': True,
        'trade_date': d.get('trade_date') or '',
        'hq_date': d.get('hq_date') or '',
        'captured_at': d.get('captured_at') or '',
        'count': d.get('count') or 0,
        'source': d.get('source') or '',
        'elapsed': d.get('elapsed'),
        'exrights': d.get('exrights') or {},
        'age_sec': round(time.time() - float(d.get('captured_ts') or 0), 0),
    }


def clear():
    try:
        os.remove(FILE)
    except OSError:
        pass
    _CACHE.update({'key': None, 'data': None, 'gen': ''})
