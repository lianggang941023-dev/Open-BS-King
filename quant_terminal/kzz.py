# -*- coding: utf-8 -*-
"""可转债标的（转债正股）—— 给龙虎榜等列表打「债」标

为什么要单独模块:
  本地 concept_map.csv 里**没有**「转债标的」这个概念（实测 5585 行 0 命中），
  同花顺涨停池 / 龙虎榜接口也不带这个标记 ⇒ 只能另取一份正股清单。

数据源（2026-11-14 实测）:
  东财数据中心 RPT_BOND_CB_LIST —— 一次拿到**全部**可转债（含已退市），
  带 `CONVERT_STOCK_CODE`（正股 6 位码）与 `CEASE_DATE`（债券存续到期日）。
  分 3 页 500/页拿完 1067 只约 1.2 秒；存续（CEASE_DATE >= 今天）319 只 / 316 只正股。
  与本地 5589 只股票库求交集 = 314 只。

  注意：`push2.eastmoney.com` 那套行情接口在本机常年被限流（RemoteDisconnected），
  但 `datacenter-web.eastmoney.com` 这个数据中心接口是通的 —— 别再拿 push2 去试。

对外:
  bonds(force=False)      -> {正股6位: [{'code','name','end'}]}
  stock_set(force=False)  -> frozenset(正股6位)
  refresh(force=True)     -> 强制重拉

缓存: kline_data/kzz_cache.json，TTL 12 小时（转股/退市都按天级变化，没必要更勤）。
失败一律降级到旧缓存；连旧缓存都没有就返回空集（**不阻断列表**）。
"""
import json
import os
import time
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CACHE_FILE = os.path.join(ROOT, 'kline_data', 'kzz_cache.json')
LOG_FILE = os.path.join(HERE, 'kzz.log')
TTL = 12 * 3600

_API = ('https://datacenter-web.eastmoney.com/api/data/v1/get?'
        'reportName=RPT_BOND_CB_LIST'
        '&columns=SECURITY_CODE,SECURITY_NAME_ABBR,CONVERT_STOCK_CODE,DELIST_DATE,CEASE_DATE'
        '&pageSize=500&sortColumns=PUBLIC_START_DATE&sortTypes=-1&pageNumber=%d')

_C = {'ts': 0.0, 'map': None}


def log(level, msg):
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 2 * 1024 * 1024:
            with open(LOG_FILE, 'rb') as f:
                tail = f.read()[-1024 * 1024:]
            with open(LOG_FILE, 'wb') as f:
                f.write(tail)
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write('[%s] %s %s\n' % (time.strftime('%Y-%m-%d %H:%M:%S'), level, msg))
    except Exception:
        pass


def _fetch():
    """拉全量可转债 → {正股6位: [债券...]}（只留存续中的）"""
    # v2.234 那条铁律：本机挂全局 SSR 代理，境内源必须显式直连
    op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    rows = []
    for pn in (1, 2, 3, 4):
        d = json.loads(op.open(_API % pn, timeout=25).read().decode('utf-8'))
        data = ((d.get('result') or {}).get('data')) or []
        rows += data
        if len(data) < 500:
            break
    if not rows:
        raise RuntimeError('可转债列表为空')
    today = datetime.now().strftime('%Y-%m-%d')
    m = {}
    for x in rows:
        sc = str(x.get('CONVERT_STOCK_CODE') or '').strip()
        end = str(x.get('CEASE_DATE') or '')[:10]
        # 存续 = 到期日还没到；到期日缺失的算存续（宁可多标，也不漏）
        if not sc.isdigit() or len(sc) != 6:
            continue
        if end and end < today:
            continue
        m.setdefault(sc, []).append({
            'code': str(x.get('SECURITY_CODE') or ''),
            'name': x.get('SECURITY_NAME_ABBR') or '',
            'end': end,
        })
    return m


def _load_cache(max_age=None):
    try:
        with open(CACHE_FILE, encoding='utf-8') as f:
            d = json.load(f)
        ts = float(d.get('ts') or 0)
        if max_age is not None and (time.time() - ts) > max_age:
            return None
        return {'ts': ts, 'map': d.get('map') or {}}
    except Exception:
        return None


def _save_cache(m):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        tmp = CACHE_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({'ts': time.time(), 'map': m}, f, ensure_ascii=False)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        log('WARN', '写缓存失败: %s' % e)


def bonds(force=False):
    """{正股6位: [{'code','name','end'}]}"""
    if not force:
        if _C['map'] is not None and (time.time() - _C['ts']) <= TTL:
            return _C['map']
        c = _load_cache(max_age=TTL)
        if c:
            _C.update({'ts': c['ts'], 'map': c['map']})
            return c['map']
    try:
        m = _fetch()
        _C.update({'ts': time.time(), 'map': m})
        _save_cache(m)
        log('INFO', '刷新成功: %d 只正股' % len(m))
        return m
    except Exception as e:
        log('WARN', '刷新失败, 用旧缓存: %s' % e)
        c = _load_cache(max_age=None)          # 过期也认，宁可旧也别没有
        if c:
            _C.update({'ts': c['ts'], 'map': c['map']})
            return c['map']
        _C.update({'ts': time.time(), 'map': {}})
        return {}


def stock_set(force=False):
    """frozenset(正股6位) —— 给列表打「债」标用"""
    return frozenset(bonds(force).keys())


def refresh(force=True):
    m = bonds(force)
    return {'ok': bool(m), 'n': len(m)}


if __name__ == '__main__':
    import sys
    t0 = time.time()
    r = refresh('--force' in sys.argv)
    print('正股数', r['n'], '耗时 %.2fs' % (time.time() - t0))
    m = bonds()
    for k in sorted(m)[:5]:
        print(' ', k, m[k])
