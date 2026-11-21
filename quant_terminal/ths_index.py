# -*- coding: utf-8 -*-
"""同花顺自编指数（全A 883957 等）日K / 分时取数 —— v2.360 (2026-11-21 用户 m38439)

## 为什么需要这个模块
顶栏七大指数走的都是「腾讯 fqkline（日K）+ 腾讯 minute（分时）」。
但 **同花顺全A（883957）腾讯、新浪都没有**（实测 sh883957 / sz883957 / bj883957 全空，
新浪 `var hq_str_s_sh883957=""` 也是空串），全网只有同花顺网页接口有：
    · 日K  https://d.10jqka.com.cn/v6/line/48_883957/01/all.js
    · 分时 https://d.10jqka.com.cn/v6/time/48_883957/last.js
两个都是 **gbk 编码 + JSONP 外壳**（`quotebridge_v6_line_..._all({...})`），所以这里自己解。

## 日K 接口的真实结构（实测，别按记忆写）
顶层是**平铺的 dict**（不是 `{'48_883957': {...}}` 那种嵌套）：
    {total:2067, start:'20180517', name:'同花顺全A(沪深京)', marketType:..., priceFactor:1000,
     price:'995539,3350,5831,2710,...', volumn:'26430757000,...',
     afterVolumn:'...', dates:'0517,0518,...', sortYear:[[2018,155],[2019,244],...]}

### price 字段：每天 4 个数，全部相对「收盘」的偏移量（priceFactor 缩放）
    p[4k]   = 收盘 × pf
    p[4k+1] = (最高 − 收盘) × pf
    p[4k+2] = (最低 − 收盘) × pf   ← 注意是**减**，不是加
    p[4k+3] = (开盘 − 收盘) × pf
⇒ 收盘 = p[0]/pf；最高 = (p[0]+p[1])/pf；最低 = (p[0]−p[2])/pf；开盘 = (p[0]+p[3])/pf
   实证（2026-11-20）：p=[1488051,44269,54245,0]/1000 ⇒ 收 1488.051 高 1532.320 低 1433.806 开 1488.051，
   与同日同花顺分时首点 1532.320 / 最低 1433.806 完全吻合。

### dates 只有 4 位 MMDD，年份必须靠 sortYear 累加还原
    sortYear = [[2018,155],[2019,244],...] 表示「2018 年有 155 个交易日」，
    按顺序吃 dates 就能还原出完整日期。长度恒等于 total。

## 分时接口
顶层是**嵌套的** `{'48_883957': {...}}`（和日K不同，别搞混）：
    {name, open:0（**顶层 open 恒为 0，不可用**）, pre:昨收, date:'20261120', dotsCount, data, ...}
    data = '0930,1532.320,13531068000,12.290,1100999110;0931,...'
           时刻, 价格, 成交额, 均价, 成交量 —— **分号分段、逗号分列**
    开盘价取**第一点价格**（顶层 open 不可用，这是本接口最坑的地方）。

## 字段口径（喂给终端其它模块时要对齐）
日K 返回 [{date,open,close,high,low,volume}]，与 engine._norm_tencent_bars 同构。
`volume` 用日K 的 `volumn`（股），但终端指数日K 全站用「手」——
这里 **÷100 转手**，与腾讯 fqkline 的成交量同口径（否则量比/成交额全错一位）。
"""

import json
import time
import urllib.request

_LINE_URL = 'https://d.10jqka.com.cn/v6/line/%s/01/all.js'
_TIME_URL = 'https://d.10jqka.com.cn/v6/time/%s/last.js'
_HDR = {
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/120.0 Safari/537.36'),
    'Referer': 'https://www.10jqka.com.cn/',
}
_TIMEOUT = 12

# 同花顺自编指数的前缀：48_ = 同花顺编制（行业/概念/全A 883957 都在这档）
# hs_ 是沪深个股或部分指数（实测 hs_000852 返回的是「石化机械」这只个股，绝不能用）
_HS_CODE_PREFIX = '48_'


def _fetch_jsonp(url, timeout=_TIMEOUT):
    """拉 JSONP 并剥壳。返回 (dict|None, err)。

    走直连 opener（用户的系统代理常开，requests/urllib 默认读注册表会 502）。
    """
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        req = urllib.request.Request(url, headers=_HDR)
        with opener.open(req, timeout=timeout) as r:
            raw = r.read()
        txt = raw.decode('gbk', 'replace')
        i, j = txt.find('('), txt.rfind(')')
        if i < 0 or j <= i:
            return None, '返回不是 JSONP: %s' % txt[:80]
        return json.loads(txt[i + 1:j]), ''
    except Exception as e:
        return None, '%s: %s' % (type(e).__name__, str(e)[:120])


def _restore_dates(mmdd_list, sort_year):
    """MMDD 4 位 + sortYear 累计 → ['YYYY-MM-DD', ...]"""
    out = []
    p = 0
    n = len(mmdd_list)
    for ym in (sort_year or []):
        try:
            y, cnt = int(ym[0]), int(ym[1])
        except (TypeError, ValueError, IndexError):
            continue
        for _ in range(cnt):
            if p >= n:
                break
            s = str(mmdd_list[p]).strip()
            p += 1
            if len(s) == 4 and s.isdigit():
                mm, dd = s[:2], s[2:]
            elif '/' in s:
                mm, dd = s.split('/', 1)
            else:
                continue
            out.append('%d-%s-%s' % (y, mm.zfill(2), dd.zfill(2)))
    return out


def full_bars(code='883957'):
    """同花顺自编指数全量日K → [{date,open,close,high,low,volume}]（volume 已 ÷100 成「手」）

    失败返回 []（调用方按「无数据」处理，绝不抛异常 —— 它跑在指标同步线程里）。
    """
    d, err = _fetch_jsonp(_LINE_URL % (_HS_CODE_PREFIX + str(code)))
    if not d:
        return []
    try:
        pf = float(d.get('priceFactor') or 1000) or 1000.0
        price = [float(x) for x in str(d.get('price') or '').split(',') if x.strip()]
        vols = str(d.get('volumn') or '').split(',')
        dates_raw = str(d.get('dates') or '').split(',')
        dates = _restore_dates(dates_raw, d.get('sortYear') or [])
        n = min(len(dates), len(price) // 4)
        out = []
        for k in range(n):
            c = price[4 * k] / pf
            h = (price[4 * k] + price[4 * k + 1]) / pf
            lo = (price[4 * k] - price[4 * k + 2]) / pf
            o = (price[4 * k] + price[4 * k + 3]) / pf
            try:
                v = float(vols[k]) if k < len(vols) else 0.0
            except (TypeError, ValueError):
                v = 0.0
            if c <= 0:
                continue
            out.append({'date': dates[k], 'open': o, 'close': c,
                        'high': h, 'low': lo, 'volume': v / 100.0})
        out.sort(key=lambda x: x['date'])
        return out
    except Exception:
        return []


def tail_bars(code='883957', n=10):
    """最近 n 根（增量比对用）。同花顺接口一次就给全量，这里切片即可。"""
    rows = full_bars(code)
    return rows[-max(1, int(n)):] if rows else []


def minute(code='883957'):
    """同花顺自编指数当日分时 → {ok, code, name, date, pre, times, prices, open, high, low, last, pct, source}

    times 是 'HHMM' 字符串列表，prices 与之一一对应。
    开盘价取第一点（顶层 open 恒为 0，不能用）。
    """
    d, err = _fetch_jsonp(_TIME_URL % (_HS_CODE_PREFIX + str(code)))
    if not d:
        return {'ok': False, 'code': code, 'error': err or '取不到分时'}
    inner = d.get(_HS_CODE_PREFIX + str(code))
    if not isinstance(inner, dict):
        inner = next((v for v in d.values() if isinstance(v, dict)), {})
    times, prices = [], []
    for seg in str(inner.get('data') or '').split(';'):
        seg = seg.strip()
        if not seg:
            continue
        parts = seg.split(',')
        if len(parts) < 2:
            continue
        try:
            px = float(parts[1])
        except (TypeError, ValueError):
            continue
        if px <= 0:
            continue
        times.append(parts[0].strip())
        prices.append(px)
    if not prices:
        return {'ok': False, 'code': code, 'error': '分时数据为空'}
    try:
        pre = float(inner.get('pre') or 0)
    except (TypeError, ValueError):
        pre = 0.0
    last = prices[-1]
    return {
        'ok': True, 'code': code,
        'name': inner.get('name') or '',
        'date': inner.get('date') or time.strftime('%Y%m%d'),
        'pre': pre,
        'open': prices[0],          # 顶层 open 是 0，只能用第一点
        'high': max(prices),
        'low': min(prices),
        'last': last,
        'pct': round((last - pre) / pre * 100, 2) if pre > 0 else 0.0,
        'times': times, 'prices': prices,
        'source': '同花顺分时',
    }


def live_bar(code='883957'):
    """盘中当日 live bar（形状同 engine._live_index_bar 的返回）—— 给 get_index_bars 用。"""
    m = minute(code)
    if not m.get('ok'):
        return None
    dt = m.get('date') or ''
    if len(dt) == 8 and dt.isdigit():
        date = '%s-%s-%s' % (dt[:4], dt[4:6], dt[6:])
    else:
        date = time.strftime('%Y-%m-%d')
    seen = m.get('prices') or []
    vol = 0.0
    try:
        d, _e = _fetch_jsonp(_TIME_URL % (_HS_CODE_PREFIX + str(code)))
        inner = (d or {}).get(_HS_CODE_PREFIX + str(code)) or {}
        vsum = 0.0
        for seg in str(inner.get('data') or '').split(';'):
            p2 = seg.split(',')
            if len(p2) >= 5:
                try:
                    vsum = float(p2[4])       # 累计成交量（股）
                except (TypeError, ValueError):
                    pass
        vol = vsum / 100.0
    except Exception:
        vol = 0.0
    return {'date': date, 'open': m['open'], 'close': seen[-1] if seen else m['last'],
            'high': m['high'], 'low': m['low'], 'volume': vol}


def selftest():
    rows = full_bars()
    print('日K  %d 根  %s ~ %s' % (len(rows), rows[0]['date'] if rows else '-',
                                   rows[-1]['date'] if rows else '-'))
    if rows:
        r = rows[-1]
        print('  末根 %s  开%.3f 高%.3f 低%.3f 收%.3f  量%.0f手'
              % (r['date'], r['open'], r['high'], r['low'], r['close'], r['volume']))
    m = minute()
    print('分时 ok=%s  %d 点  开%.3f 高%.3f 低%.3f 现%.3f 昨收%.3f (%+.2f%%)'
          % (m.get('ok'), len(m.get('times') or []), m.get('open', 0), m.get('high', 0),
             m.get('low', 0), m.get('last', 0), m.get('pre', 0), m.get('pct', 0)))
    return rows, m


if __name__ == '__main__':
    selftest()
