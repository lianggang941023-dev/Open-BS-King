# -*- coding: utf-8 -*-
"""同花顺 fuyao 数据源 (2026-10-21 接入, 用户 m00787 定案)

为什么接它（实测数据，不是文档抄的）:
    全市场快照 **一次请求 5580 只 / 0.96 秒**；通达信同一件事要 2 次计费调用 / 12.7 秒。
    官方口径: 不限制累计调用次数，只有 429 或信封 code=4001 才算触发限流。

口径差异（这四条是接它时最容易出错的地方）:
  ① volume 单位是**股** —— 本地 CSV / 腾讯一律用**手** ⇒ 落盘前必须 /100。
     新浪也是股、腾讯是手，混用等于成交量差 100 倍（engine.py 那条老陷阱）。
  ② 快照**不返回中文名，也不返回市值/总股本** ⇒ 简称仍以腾讯为准（datasource.name_map），
     市值仍由通达信/腾讯补 —— 所以它替不掉 15:35 的市值那一次调用。
  ③ prev_price = 前收盘价。A 股行情惯例里，除权日的「前收盘价」就是**除权参考价**
     （与通达信 LastClose 同义）⇒ 可以直接当除权系数基准 = prev_price / 本地末行收盘。
     ⚠ 这一条没有官方明文，2026-10-22 有 26 只除权、10-23 有 42 只（含 688809 十送四），
       那两天 11:35 的审计账里如果 exrights 归零，就是它给的是**原始**昨收，必须回退通达信。
  ④ K 线接口 adjust 默认 forward(前复权)；要原始价必须显式 adjust=none。

配置 (data_sources.json → online.fuyao):
    {'enabled': True, 'base': 'https://fuyao.aicubes.cn', 'key': '', 'timeout': 20, 'batch': 800}
key 与通达信同一铁律: 空 / '******' = 未修改（见 datasource._preserve_keys）。
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(HERE)                 # D:\AI Working\_运行环境\终端8799
CFG_FILE = os.path.join(HERE, 'data_sources.json')
LOG_FILE = os.path.join(HERE, 'fuyao_fetch.log')
LOG_MAX = 2 * 1024 * 1024                        # 2MB 后截半，不许无限长

# v2.234 (2026-10-27 用户 m10187 定案): 境内源优先直连。本机挂全局 SSR 代理,
#   走代理比直连慢 4~11 倍, 且代理链路更易被掐断 (日志里六成失败是 SSL/断连)。
try:
    from . import net as _net
except Exception:                       # 作为脚本直接跑时 (python fuyao.py) 没有包上下文
    import net as _net

BASE_URL = 'https://fuyao.aicubes.cn'
DEFAULT = {'enabled': True, 'base': BASE_URL, 'key': '', 'timeout': 20, 'batch': 800}

# 一次全市场拉全用的分页大小（实测 6000 一次就返回 5580 条，服务端没截断）
ALL_LIMIT = 6000


class FuyaoError(Exception):
    """接口错误 / 网络错误 / 未配置 key"""


class FuyaoRateLimited(FuyaoError):
    """429 或信封 code=4001 —— 被限流（要写日志，别静默重试）"""


# ---------------------------------------------------------------- 配置 / 日志
def load_cfg():
    """读 data_sources.json 的 online.fuyao 节（与数据源设置窗口共用同一份配置）"""
    cfg = dict(DEFAULT)
    try:
        with open(CFG_FILE, encoding='utf-8') as fh:
            d = json.load(fh)
        f = ((d.get('online') or {}).get('fuyao') or {})
        if 'enabled' in f:
            cfg['enabled'] = bool(f.get('enabled'))
        for k in ('base', 'key'):
            v = str(f.get(k) or '').strip()
            if v:
                cfg[k] = v
        for k in ('timeout', 'batch'):
            try:
                v = int(f.get(k) or 0)
                if v > 0:
                    cfg[k] = v
            except (TypeError, ValueError):
                pass
    except Exception:
        pass
    cfg['base'] = str(cfg['base']).rstrip('/') or BASE_URL
    return cfg


def configured():
    """有 key 且启用 —— 调用方拿它决定「这次走同花顺还是退回通达信」"""
    c = load_cfg()
    return bool(c.get('enabled')) and bool(str(c.get('key') or '').strip())


def log(level, api, detail):
    """限流/失败日志（用户 m00787 要求: 拉取失败被限流要能查）。

    单文件追加，超过 2MB 只留后半 —— 一天最多几万行，但没必要留一个月。
    """
    try:
        line = '%s | %-5s | %s | %s\n' % (
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, api, detail)
        try:
            if os.path.getsize(LOG_FILE) > LOG_MAX:
                with open(LOG_FILE, 'r', encoding='utf-8', errors='replace') as fh:
                    tail = fh.readlines()[-2000:]
                with open(LOG_FILE, 'w', encoding='utf-8') as fh:
                    fh.write(''.join(tail))
        except OSError:
            pass
        with open(LOG_FILE, 'a', encoding='utf-8') as fh:
            fh.write(line)
    except Exception:
        pass


# ---------------------------------------------------------------- HTTP
def _req(path, params=None, timeout=None, api_name=''):
    cfg = load_cfg()
    key = str(cfg.get('key') or '').strip()
    if not key:
        raise FuyaoError('未配置同花顺 API Key (数据源设置 → 同花顺 fuyao)')
    url = cfg['base'] + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'X-api-key': key,
        'User-Agent': 'Mozilla/5.0 (qt-terminal)',
        'Accept': 'application/json',
    })
    t0 = time.time()
    try:
        with _net.open_url(req, timeout=timeout or cfg['timeout']) as r:
            raw = r.read().decode('utf-8', 'ignore')
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        if e.code == 429:
            log('限流', api_name or path, 'HTTP 429 %dms' % ms)
            raise FuyaoRateLimited('同花顺限流 HTTP 429 (%dms)' % ms)
        log('失败', api_name or path, 'HTTP %s %dms' % (e.code, ms))
        raise FuyaoError('同花顺 HTTP %s (%dms)' % (e.code, ms))
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        log('失败', api_name or path, '%s %dms' % (str(e)[:80], ms))
        raise FuyaoError('同花顺请求失败: %s (%dms)' % (str(e)[:80], ms))
    try:
        j = json.loads(raw)
    except Exception:
        log('失败', api_name or path, '响应不是 JSON: %s' % raw[:60].replace('\n', ' '))
        raise FuyaoError('同花顺响应不是 JSON')
    code = j.get('code')
    if code == 4001:
        log('限流', api_name or path, '信封 code=4001 %s' % str(j.get('message'))[:60])
        raise FuyaoRateLimited('同花顺限流 code=4001: %s' % str(j.get('message'))[:60])
    if code != 0:
        log('失败', api_name or path, 'code=%s %s' % (code, str(j.get('message'))[:60]))
        raise FuyaoError('同花顺错误 code=%s: %s' % (code, str(j.get('message'))[:60]))
    return j.get('data') or {}


def _clean_item(it):
    """单条快照 → 本地口径 dict；脏数据返回 None"""
    t = str(it.get('ticker') or '').strip()
    if not t:
        return None
    try:
        return {
            'close': float(it.get('last_price') or 0),
            'open': float(it.get('open_price') or 0),
            'high': float(it.get('high_price') or 0),
            'low': float(it.get('low_price') or 0),
            'prev_close': float(it.get('prev_price') or 0),
            # 🔴 股 → 手（本地口径；少这一步全市场成交量会大 100 倍）
            'volume': int(round(float(it.get('volume') or 0) / 100.0)),
            'turnover': float(it.get('turnover') or 0),
            'pct': float(it.get('price_change_ratio_pct') or 0),
            'thscode': str(it.get('thscode') or ''),
        }
    except (TypeError, ValueError):
        return None


def snapshot(thscodes=None, limit=ALL_LIMIT, offset=0):
    """A 股行情快照。

    thscodes: 显式代码表（'600519.SH' / '600519' 都行）→ 按入参批量取、不分页
              省略 → 按 limit/offset 分页遍历全市场
    返回 {6位裸码: {close,open,high,low,prev_close,volume(手),turnover,pct,thscode}}
    """
    p = {}
    if thscodes:
        p['thscodes'] = thscodes if isinstance(thscodes, str) else ','.join(thscodes)
    else:
        p['limit'] = int(limit)
        p['offset'] = int(offset or 0)
    d = _req('/api/a-share/prices/snapshot', p, api_name='prices/snapshot')
    out = {}
    for it in (d.get('item') or []):
        row = _clean_item(it)
        if not row:
            continue
        code = os.path.basename(row['thscode']).split('.')[0] or row.get('thscode', '')[:6]
        out[code] = row
    return out


def snapshot_all(max_pages=12):
    """全市场快照 → ({裸码: 行}, timestamp_ms)

    一次 limit=6000 实测就返回 5580 条；万一服务端截断（返回条数 < total），
    按 offset 续拉，最多 max_pages 页（防死循环）。
    """
    rows = {}
    ts = 0
    total = 0
    off = 0
    for _ in range(max_pages):
        p = {'limit': ALL_LIMIT, 'offset': off}
        d = _req('/api/a-share/prices/snapshot', p, api_name='prices/snapshot(all)')
        items = d.get('item') or []
        try:
            total = int(d.get('total') or 0)
        except (TypeError, ValueError):
            total = 0
        try:
            ts = int(d.get('timestamp') or 0) or ts
        except (TypeError, ValueError):
            pass
        for it in items:
            row = _clean_item(it)
            if not row:
                continue
            code = row['thscode'].split('.')[0] or ''
            if len(code) == 6:
                rows[code] = row
        if not items or (total and len(rows) >= total) or len(items) < ALL_LIMIT:
            break
        off += len(items)
    return rows, ts


def trade_date_of(ts_ms):
    """快照 timestamp(毫秒) → 'YYYY-MM-DD'（数据就绪时间，不是本机今天）"""
    try:
        if ts_ms and int(ts_ms) > 946684800000:      # 2000-01-01 之后才算有效
            return datetime.fromtimestamp(int(ts_ms) / 1000.0).strftime('%Y-%m-%d')
    except Exception:
        pass
    return datetime.now().strftime('%Y-%m-%d')


# ---------------------------------------------------------------- 兼容旧调用
def quote_batch(codes, chunk=200):
    """批量取实时价（给自动刷新用）→ {裸码: {price, pct, prev, open, high, low, vol}}

    codes 可以带前缀（'SH600519'/'600519'），内部统一成 '600519.SH' 形式发出去。
    """
    syms = []
    for c in codes:
        s = to_thscode(c)
        if s:
            syms.append(s)
    out = {}
    for i in range(0, len(syms), chunk):
        part = syms[i:i + chunk]
        got = snapshot(thscodes=part)
        for k, v in got.items():
            out[k] = {
                'price': v['close'], 'pct': v['pct'], 'prev': v['prev_close'],
                'open': v['open'], 'high': v['high'], 'low': v['low'],
                'vol': v['volume'],
            }
    return out


def to_thscode(code):
    """本地基名/裸码 → 同花顺 thscode。'SH600519'→'600519.SH'；'600519'→ 按首位猜。"""
    c = str(code or '').strip().upper()
    if not c:
        return ''
    if '.' in c:                       # 已经是 600519.SH
        bare, sfx = c.split('.', 1)
        return '%s.%s' % (bare, 'SH' if sfx in ('SH', 'SS') else sfx)
    if c[:2] in ('SH', 'SZ', 'BJ', 'BZ'):
        bare, pre = c[2:], c[:2]
        return '%s.%s' % (bare, {'SH': 'SH', 'SZ': 'SZ', 'BJ': 'BJ', 'BZ': 'BJ'}[pre])
    if len(c) == 6 and c.isdigit():
        # 北交所优先判: 920xxx(新号段) / 43xxxx / 8xxxxx —— 不能落进下面的 9→SH
        if c[:3] == '920' or c[0] in ('4', '8'):
            return c + '.BJ'
        if c[0] in ('6', '9'):
            return c + '.SH'
        if c[0] in ('0', '3', '2'):
            return c + '.SZ'
    return ''


def test():
    """数据源设置窗口的「测试」按钮 —— 拉 2 只票，回延迟和价格，绝不回显 key"""
    t0 = time.time()
    try:
        got = snapshot(thscodes=['600519.SH', '000001.SZ'])
        ms = int((time.time() - t0) * 1000)
        if not got:
            return {'ok': False, 'latency_ms': ms, 'detail': '快照返回空'}
        g = got.get('600519') or {}
        return {'ok': True, 'latency_ms': ms,
                'detail': '茅台 %s · 2 只 %dms · 免费' % (g.get('close') or '?', ms)}
    except FuyaoRateLimited as e:
        return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000),
                'detail': '被限流: %s' % str(e)[:80]}
    except Exception as e:
        return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000),
                'detail': str(e)[:100]}


# ---------------------------------------------------------------- CLI (自测)
def _cli():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--selftest', action='store_true')
    ap.add_argument('--codes', default='')
    a = ap.parse_args()
    if a.selftest or a.codes:
        if a.codes:
            r = snapshot(thscodes=[x for x in a.codes.split(',') if x])
            print(json.dumps(r, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(test(), ensure_ascii=False))
        return
    t0 = time.time()
    rows, ts = snapshot_all()
    print(json.dumps({'count': len(rows), 'trade_date': trade_date_of(ts),
                      'elapsed': round(time.time() - t0, 2)}, ensure_ascii=False))


if __name__ == '__main__':
    _cli()
