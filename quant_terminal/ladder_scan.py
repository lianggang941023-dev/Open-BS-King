# -*- coding: utf-8 -*-
"""连板梯队（TSP limit-ladder 移植给 8799，v2.299）

数据来源（全部本地/已有缓存，不新增外网接口）
------------------------------------------------
* 今日涨停池   : ``fuyao_limit.limit_up_pool(date)``   —— 含 ``lianban``（连板数）、封单、换手、行业、原因
* 今日炸板池   : ``fuyao_limit.limit_break_pool(date)`` —— 摸过板没封住（TSP 的 ``broken``）
* 今日跌停池   : ``fuyao_limit.limit_down_pool(date)``
* 昨日涨停池   : 同 ``limit_up_pool(上一交易日)`` —— 用来推 ①炸板票该归第几板 ②断板票（TSP ``failed``）
* 行业/概念     : ``engine.load_industry_concept_maps()``
* 板块徽章     : ``mcap.prefixed()``

> 为什么不复用 ``/api/limit_pools?kind=ladder``：那份数据只有 ``{thscode,ticker,name,board_num}``，
> 没有涨幅/封单/换手/炸板断板状态，做不出 TSP 那种卡片，实测 2026-11-13 只有 4 个非空档。
> 这里自己按涨停池分组重算，档位从 1 板起、含炸板与断板三种状态。
"""
import os
import sys
import time
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from quant_terminal import fuyao_limit as _fl          # noqa: E402
from quant_terminal import engine as _eng             # noqa: E402
from quant_terminal import mcap as _mcap              # noqa: E402
from quant_terminal import board_rank as _brd         # noqa: E402
from quant_terminal import lastgood as _lg            # noqa: E402

_TTL = 300.0
_CACHE = {}
_LOCK = threading.Lock()

# 板块徽章（跟终端其它地方同一套）
_TAG = {'创': '创业板', '科': '科创板', '北': '北交所'}


def _tag_of(code):
    c = str(code or '')
    if c[:3] in ('300', '301', '302'):
        return '创'
    if c[:3] in ('688', '689'):
        return '科'
    if c[:3] == '920' or c[:1] in ('4', '8'):
        return '北'
    return ''


def _limit_pct(code, name=''):
    """涨跌停幅度（只用来算连跌天数，口径从宽）"""
    c = str(code or '')
    nm = str(name or '')
    if 'ST' in nm.upper():
        return 5.0
    if c[:3] in ('300', '301', '302', '688', '689'):
        return 20.0
    if c[:3] == '920' or c[:1] in ('4', '8'):
        return 30.0
    return 10.0


def _pull(fn, date):
    """fuyao_limit 的池子两种返回形态：list 或 (list, meta) —— 统一剥一层"""
    try:
        r = fn(date)
    except Exception as e:
        return [], '%s: %s' % (type(e).__name__, e)
    if isinstance(r, tuple):
        r = r[0] if r else []
    if not isinstance(r, list):
        return [], 'bad-type:%s' % type(r).__name__
    return r, ''


def _prev_trade_date(date):
    """上一交易日：从本地 K 线（600000）日期序列里取 date 之前的那根"""
    try:
        rows = _eng.load_csv_kline('600000') or []
        ds = [r.get('date') for r in rows if r.get('date')]
        if not ds:
            return ''
        if date in ds:
            i = ds.index(date)
            return ds[i - 1] if i > 0 else ''
        prev = [d for d in ds if d < date]
        return prev[-1] if prev else ''
    except Exception:
        return ''


def _mk_stock(r, status, boards, ind, con):
    code = str(r.get('code') or '')
    con_raw = con.get(code) or ''
    # 换手：同花顺涨停池的 turnover 恒为 0（实测），只能用本地 CSV 现算；算不出来就留 None
    _tv = float(r.get('turnover') or 0.0)
    if _tv <= 0:
        _tv = _turn_now(code) or None
    return {
        'code': code,
        'name': r.get('name') or '',
        'pct': round(float(r.get('pct') or 0.0), 2),
        'price': round(float(r.get('price') or 0.0), 2),
        'turn': (round(_tv, 2) if _tv else None),
        'seal_money': float(r.get('seal_money') or 0.0),      # 封单额（元）
        'seal_max': float(r.get('max_seal') or 0.0),
        'first_time': r.get('first_time') or '',
        'last_time': r.get('last_time') or '',
        'boards': int(boards or 1),
        'status': status,                                     # limit_up / broken / failed / limit_down
        'tag': _tag_of(code),
        'board': r.get('board') or '',
        'industry': ind.get(code) or (r.get('industry') or ''),
        'concepts': [x for x in con_raw.split('|') if x][:4],
        'reason': r.get('reason') or '',
        'is_new': bool(r.get('is_new')),
        'is_st': bool(r.get('is_st')),
    }


def _down_boards(code, name):
    """跌停票的连跌天数：本地 K 线倒着数「跌幅 >= 涨跌停幅度-0.35」的连续根数"""
    lp = _limit_pct(code, name)
    try:
        rows = _eng.load_csv_kline(code) or []
    except Exception:
        return 1
    n = 0
    for i in range(len(rows) - 1, 0, -1):
        c0 = rows[i].get('close')
        c1 = rows[i - 1].get('close')
        if not c0 or not c1:
            break
        pct = (c0 - c1) / c1 * 100.0
        if pct <= -(lp - 0.35):
            n += 1
        else:
            break
    return max(1, n)


def _last2(code):
    """本地 K 线末两根 (date, close) —— 断板票要用「今天」的真实涨幅，不能拿昨天的凑"""
    try:
        rows = _eng.load_csv_kline(code) or []
    except Exception:
        return ('', 0.0, 0.0)
    if len(rows) < 2:
        return ('', 0.0, 0.0)
    a, b = rows[-2], rows[-1]
    return (b.get('date') or '', float(a.get('close') or 0), float(b.get('close') or 0))


def _turn_now(code):
    """今日实际换手率（%）= 成交量(手) ÷ 自由流通股本(万股)。

    ⚠ 两个坑（都实测过）：
      1. 同花顺涨停池里的 ``turnover`` 字段**恒为 0**（2026-11-13 抽样 37 只全 0），
         直接用会在卡片上显示「换 0.0%」看着像真数据 —— 必须自己算。
      2. **不能用 ``engine.load_csv_kline()``** —— 它只解析到第 6 列 volume，
         第 8 列流通市值、第 9 列自由流通股本根本没读出来 ⇒ free 恒 0 ⇒ 换手恒算不出来。
         要用 ``board_rank._tail_rows()``（它解析到第 9 列）。
      CSV 列序: date,open,close,high,low,volume,总市值,流通市值[,自由流通股本]
      volume 单位=手, 自由流通股本单位=万股 ⇒ vol/free 直接就是百分数。
    """
    try:
        rows = _brd._tail_rows(os.path.join(_brd.kline_dir(), _mcap.prefixed(code) + '.csv'), 3)
    except Exception:
        return 0.0
    if not rows:
        return 0.0
    r = rows[-1]
    vol = float(r.get('volume') or 0)
    free = float(r.get('free') or 0)
    if vol <= 0 or free <= 0:
        return 0.0
    return round(vol / free, 2)


def _tags(stocks):
    """概念/行业分布（只在涨停票上统计，跟 TSP 一致：炸板断板不计入分布）"""
    cc, ic = {}, {}
    for s in stocks:
        if s['status'] != 'limit_up':
            continue
        for c in s['concepts']:
            cc[c] = cc.get(c, 0) + 1
        ind = s['industry']
        if ind:
            ic[ind] = ic.get(ind, 0) + 1
    f = lambda d: [{'name': k, 'count': v} for k, v in sorted(d.items(), key=lambda x: -x[1])][:40]
    return {'concept': f(cc), 'industry': f(ic)}


def ladder(date='', direction='up', force=False):
    """连板梯队主入口。返回 {ok,date,direction,tiers[],overview,tags,counts,errors,elapsed}"""
    direction = 'down' if str(direction).lower() == 'down' else 'up'
    date = (date or '').strip()
    key = '%s|%s' % (direction, date)
    now = time.time()
    with _LOCK:
        c = _CACHE.get(key)
        # v2.399: 用存档顶上的结果只缓存 60 秒 —— 上游一恢复就该立刻换回真数据
        if c and not force and (now - c['ts']) < (60.0 if c.get('stale') else _TTL):
            return c['data']

    t0 = time.time()
    errors = []
    if not date:
        try:
            date = _fl.default_pool_date() or ''
        except Exception:
            date = ''

    ind, con = {}, {}
    try:
        ind, con = _eng.load_industry_concept_maps()
    except Exception as e:
        errors.append('maps: %s' % e)

    tiers = {}
    def _put(boards, stock):
        tiers.setdefault(int(boards), []).append(stock)

    counts = {'up': 0, 'down': 0, 'broken': 0, 'failed': 0}

    _srcs = []          # v2.399: 各路数据的真实采集时刻
    if direction == 'up':
        up, e1 = _pull(_fl.limit_up_pool, date)
        if e1:
            errors.append('up: %s' % e1)
        br, e2 = _pull(_fl.limit_break_pool, date)
        if e2:
            errors.append('break: %s' % e2)
        prev = _prev_trade_date(date)
        yup, e3 = _pull(_fl.limit_up_pool, prev) if prev else ([], '')
        if e3:
            errors.append('yup: %s' % e3)
        for _api, _d in ((_fl.POOL_UP, date), (_fl.POOL_BREAK, date), (_fl.POOL_UP, prev)):
            if _d:
                _s = _fl.last_src(_api, _d)
                if _s:
                    _srcs.append((_d, _s))

        yboards = {str(r.get('code')): int(r.get('lianban') or 1) for r in yup}
        today_codes = set()

        for r in up:
            code = str(r.get('code') or '')
            today_codes.add(code)
            b = int(r.get('lianban') or 1)
            _put(b, _mk_stock(r, 'limit_up', b, ind, con))
            counts['up'] += 1

        # 炸板：今天摸过板没封住 —— 归到「昨天板数+1」，昨天没上榜就算首板
        for r in br:
            code = str(r.get('code') or '')
            today_codes.add(code)
            b = yboards.get(code, 0) + 1
            _put(b, _mk_stock(r, 'broken', b, ind, con))
            counts['broken'] += 1

        # 断板：昨天在涨停池、今天既没封也没摸 —— 就断在昨天那个板位上
        # ⚠ 必须用「今天」的实际收盘价算涨幅：yup 那行是昨天的数据（price/pct 都是昨天的），
        #   直接拿来会显示成「昨天还涨停」，看着像没断板。
        for r in yup:
            code = str(r.get('code') or '')
            if code in today_codes:
                continue
            b = int(r.get('lianban') or 1)
            st = _mk_stock(r, 'failed', b, ind, con)
            d2, pc, cc = _last2(code)
            if pc > 0 and cc > 0:
                st['price'] = round(cc, 2)
                st['pct'] = round((cc - pc) / pc * 100.0, 2)
            _put(b, st)
            counts['failed'] += 1
    else:
        dn, e1 = _pull(_fl.limit_down_pool, date)
        if e1:
            errors.append('down: %s' % e1)
        _s = _fl.last_src(_fl.POOL_DOWN, date)
        if _s:
            _srcs.append((date, _s))
        for r in dn:
            code = str(r.get('code') or '')
            b = _down_boards(code, r.get('name') or '')
            _put(b, _mk_stock(r, 'limit_down', b, ind, con))
            counts['down'] += 1

    allst = []
    out_tiers = []
    # v2.301 (2026-11-13 用户 m15900): 「连板梯队应该是连板高的排前面，你排反了」
    #   原先 sorted(tiers.keys()) 是升序 ⇒ 首板在最上、五板在最下, 方向错了。
    #   现在按板数**降序**: 五板 → 四板 → … → 首板。跌停方向同理(连跌天数高的在前)。
    #   注: 卷里 tiers 的顺序也一并反过来(前端按序渲染), overview.tiers 同样。
    for b in sorted(tiers.keys(), reverse=True):
        lst = tiers[b]
        # 排序：涨停优先 → 封单额降序 → 涨幅降序
        order = {'limit_up': 0, 'broken': 1, 'failed': 2, 'limit_down': 0}
        lst.sort(key=lambda s: (order.get(s['status'], 9), -s['seal_money'], -s['pct']))
        allst.extend(lst)
        label = ('%d连跌' % b if b > 1 else '首跌') if direction == 'down' else ('%d板' % b if b > 1 else '首板')
        out_tiers.append({
            'boards': b,
            'label': label,
            'count': len(lst),
            'lu': sum(1 for s in lst if s['status'] in ('limit_up', 'limit_down')),
            'stocks': lst,
        })

    seal_n = counts['up']
    touch = counts['up'] + counts['broken']
    seal_rate = round(seal_n / touch * 100.0, 1) if touch else 0.0

    # v2.399: 「数据时间」以**当日**那几路池子为准(昨日池只用于断板判定, 不算新鲜度)
    _all = [x for _d, x in _srcs]
    _cur = [x for _d, x in _srcs if _d == date]
    data = {
        'ok': True,
        'date': date,
        'prev_date': _prev_trade_date(date) if direction == 'up' else '',
        'direction': direction,
        'tiers': out_tiers,
        'overview': {
            'tiers': [{'boards': t['boards'], 'label': t['label'], 'lu': t['lu']} for t in out_tiers],
            'broken': counts['broken'],
            'failed': counts['failed'],
            'seal_rate': seal_rate,
        },
        'tags': _tags(allst),
        'counts': counts,
        'total': len(allst),
        'errors': errors,
        'elapsed': round(time.time() - t0, 2),
        'ts': time.strftime('%H:%M:%S'),
        # v2.399: 界面要看到「这份数据是什么时候的」—— 上游挂掉回落本地缓存时,
        #   这里必须是缓存文件的真实采集时刻, 不能写成"现在"(否则看着像刚拉的实时数据)。
        'stale': False,
        'cached': bool(_cur) and all(x.get('cached') for x in _cur),
        'data_time': time.strftime(
            '%Y-%m-%d %H:%M:%S',
            time.localtime(max([x.get('ts') or 0 for x in (_cur or _all)] or [0]) or time.time())),
        'data_date': date,
    }
    # v2.399: 拿到数据就覆盖进单文件存档；一条都没拿到就反过来用存档顶上
    lg_name = 'ladder_' + ('down' if direction == 'down' else 'up')
    if len(allst) > 0:
        try:
            _lg.save(lg_name, data, date)
        except Exception:
            pass
    else:
        try:
            _o = _lg.load(lg_name)
        except Exception:
            _o = {'ok': False}
        if _o.get('ok') and _o.get('payload'):
            _sp = dict(_o['payload'])
            _sp['stale'] = True
            # 数据本身的时刻优先用存档 payload 里记的那个(更准), 没有再退回存档写入时刻
            _sp['data_time'] = (_o['payload'].get('data_time')
                                or _o.get('saved_at') or '')
            _sp['data_date'] = _o.get('data_date') or ''
            _sp['arch_time'] = _o.get('saved_at') or ''
            # v2.399: 「多久之前」按数据本身时刻算
            _a = _lg.age_of(_sp['data_time'])
            _sp['stale_sec'] = _a if _a is not None else (_o.get('age_sec') or 0)
            _sp['errors'] = list(errors) + [
                '本次拉取失败, 显示的是 %s 的存档' % (_o.get('saved_at') or '?')]
            _sp['elapsed'] = round(time.time() - t0, 2)
            _sp['ts'] = time.strftime('%H:%M:%S')
            _sp['ok'] = True
            with _LOCK:
                _CACHE[key] = {'ts': now, 'data': _sp, 'stale': True}
            return _sp
    with _LOCK:
        _CACHE[key] = {'ts': now, 'data': data}
    return data


def invalidate():
    with _LOCK:
        _CACHE.clear()


if __name__ == '__main__':
    d = ladder(sys.argv[1] if len(sys.argv) > 1 else '')
    print('ok=%s date=%s dir=%s total=%s elapsed=%ss errors=%s'
          % (d['ok'], d['date'], d['direction'], d['total'], d['elapsed'], d['errors']))
    for t in d['tiers']:
        print('  %-6s 共%2d 涨停%2d  %s' % (t['label'], t['count'], t['lu'],
              ' / '.join('%s(%s)%s' % (s['name'], s['code'], s['status'][0]) for s in t['stocks'][:6])))
    print('  counts', d['counts'], 'seal_rate', d['overview']['seal_rate'])
    print('  tags concept', [x['name'] for x in d['tags']['concept'][:5]])
    print('  tags industry', [x['name'] for x in d['tags']['industry'][:5]])
