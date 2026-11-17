# -*- coding: utf-8 -*-
"""龙虎榜 / 同花顺热榜 —— 同花顺 fuyao「特色数据」封装

为什么单独一个模块:
  和 fuyao_limit（涨停池）同源同风格，但字段完全不同；放一起会互相污染。
  缓存复用 fuyao_limit 的 _load_json/_save_json，落到同一个 kline_data/fuyao_cache 里。

对外:
  lhb_list(date=None, board='all')      龙虎榜个股（按票去重、按净买额排序）
  lhb_inst(code, date=None)             个股机构/营业部席位明细
  lhb_dept(dept_id, page=1, size=20)    营业部画像 + 近期上榜
  hot_list(period='day', limit=100)     同花顺热榜

日期口径:
  龙虎榜是**收盘后**才有的数据，所以默认取「上一个交易日」；
  date 传 YYYY-MM-DD 或 YYYYMMDD 都接受。
"""
import json
import time

from . import fuyao as _fq
from . import fuyao_limit as _fl

LHB_LIST = '/api/a-share/special-data/dragon-tiger-list'
LHB_INST = '/api/a-share/special-data/dragon-tiger-stock-institution'
DEPT_CLASSIC = '/api/a-share/special-data/dragon-tiger-department-classic-record'
DEPT_HISTORY = '/api/a-share/special-data/dragon-tiger-department-history'
HOT_LIST = '/api/a-share/special-data/hot-stock-list'

CACHE_TTL = {'lhb': 1800, 'hot': 60}


def log(level, api, detail):
    _fl.log(level, api, detail)


def _num(v, default=0.0):
    try:
        return float(v or 0)
    except Exception:
        return default


def _yi(v):
    """元 → 亿元（保留 2 位），前端直接显示"""
    return round(_num(v) / 1e8, 2)


def _norm_date(v):
    return _fl._norm_date(v)


def _prev_trading_day(today):
    """严格取「今天之前」的最近交易日"""
    try:
        days = [d for d in (_fl.trading_days() or []) if str(d) < str(today)]
        if days:
            return max(days)
    except Exception:
        pass
    return today


def _def_lhb_date():
    """龙虎榜默认日期

    龙虎榜是**收盘后 18:00 左右**才挂出来的，所以：
      · 今天是交易日且已过 18:00 → 今天
      · 否则 → 上一个交易日
    （用交易日历判断，不猜周末/节假日）
    """
    today = time.strftime('%Y-%m-%d')
    try:
        is_td = bool(_fl.is_trading_day(today))
    except Exception:
        is_td = time.localtime().tm_wday < 5
    if is_td and time.localtime().tm_hour >= 18:
        return today
    return _prev_trading_day(today)


def _lhb_raw(date, board='all'):
    # v2.212 (2026-10-24 DEBUG #1): 空日期不能走 _norm_date ——
    #   _norm_date(None)/_norm_date('') 会返回「今天」, 于是 `or _def_lhb_date()`
    #   的兜底永远执行不到; 周末/凌晨/18:00 前传今天 ⇒ 同花顺直接拒
    #   (code=1002: date must be an A-share trading day)。
    #   与 lhb_inst 保持同一写法。
    d = _norm_date(date) if date else _def_lhb_date()
    name = 'lhb_%s_%s' % (board, d.replace('-', ''))
    c = _fl._load_json(name, max_age=CACHE_TTL['lhb'])
    if c:
        return c, d
    r = _fq._req(LHB_LIST, {'date': d, 'board_type': board}, timeout=30, api_name='dragon-tiger-list')
    _fl._save_json(name, r)
    return r, d


def lhb_list(date=None, board='all'):
    """龙虎榜个股列表

    board: all / org（机构）/ hot_money（游资）
    返回 {ok, date, board, count, rows:[...], hot_money:[...]}
    """
    try:
        raw, d = _lhb_raw(date, board)
    except Exception as e:
        log('ERR', 'lhb_list', str(e))
        return {'ok': False, 'error': str(e)[:200]}
    # 空榜兜底：数据还没挂出来时（比如今天刚开盘），自动退到上一个交易日
    if not (raw.get('stock_items') or raw.get('hot_money_items')):
        if date is None or str(date) == '':
            _d2 = _prev_trading_day(d)
            if _d2 and _d2 != d:
                raw, d = _lhb_raw(_d2, board)
    items = raw.get('stock_items') or []
    # 同一只票可能因「上榜区间」不同出现多次（1日/3日/5日…）——
    # 取区间最长的那个（累计最全），并把出现过的区间列出来，避免重复计算净额。
    best = {}
    for it in items:
        tk = str(it.get('ticker') or '')
        if not tk:
            continue
        rd = int(_num(it.get('range_days'), 1) or 1)
        cur = best.get(tk)
        if cur is None or rd > cur['_rd']:
            best[tk] = {
                'ticker': tk,
                'name': it.get('name') or '',
                'change': round(_num(it.get('change')) * 100, 2),
                'net_value': _yi(it.get('net_value')),
                'net_rate': round(_num(it.get('net_rate')) * 100, 2),
                'buy_value': _yi(it.get('buy_value')),
                'sell_value': _yi(it.get('sell_value')),
                'org_net_value': _yi(it.get('org_net_value')),
                'hot_money_net_value': _yi(it.get('hot_money_net_value')),
                'hot_rank': int(_num(it.get('hot_rank'))),
                'limit_reason': it.get('limit_reason') or '',
                'concepts': [c.get('name') for c in (it.get('concept_list') or []) if c.get('name')],
                'range_days': rd,
                '_rd': rd,
            }
    rows = list(best.values())
    for r in rows:
        r.pop('_rd', None)
    rows.sort(key=lambda x: -abs(x['net_value']))

    hot = []
    for g in (raw.get('hot_money_items') or []):
        rr = []
        for x in (g.get('rows') or []):
            rr.append({
                'ticker': str(x.get('ticker') or ''),
                'name': x.get('name') or '',
                'change': round(_num(x.get('change')) * 100, 2),
                'amount': _yi(x.get('amount')),
                'net_value': _yi(x.get('net_value')),
            })
        rr.sort(key=lambda y: -abs(y['net_value']))
        hot.append({'name': g.get('name') or '', 'buying': _yi(g.get('buying')),
                    'count': len(rr), 'rows': rr})
    hot.sort(key=lambda y: -y['buying'])

    return {'ok': True, 'date': d, 'board': board, 'count': len(rows),
            'stock_count': int(_num(raw.get('stock_count'))), 'rows': rows, 'hot_money': hot}


def lhb_inst(code, date=None):
    """个股席位明细：机构合计 + 买入/卖出营业部名单"""
    c = str(code or '').strip()
    if not c:
        return {'ok': False, 'error': '缺代码'}
    try:
        tk = _fq.to_thscode(c)
    except Exception:
        tk = c
    # 注意: _norm_date('') 会返回「今天」—— 龙虎榜按今天查必然空榜，
    #   所以空白日期必须走 _def_lhb_date()（它才考虑 18:00 这个闸）。
    d = _norm_date(date) if date else _def_lhb_date()
    name = 'lhbinst_%s_%s' % (tk.replace('.', ''), d.replace('-', ''))
    cache = _fl._load_json(name, max_age=CACHE_TTL['lhb'])
    if cache:
        return cache
    try:
        r = _fq._req(LHB_INST, {'thscode': tk, 'date': d}, timeout=30, api_name='dragon-tiger-stock-institution')
    except Exception as e:
        log('ERR', 'lhb_inst', str(e))
        return {'ok': False, 'error': str(e)[:200]}
    out = {'ok': True, 'code': c[-6:], 'thscode': tk, 'date': r.get('trade_date') or d, 'groups': []}
    for it in (r.get('item') or []):
        buys = [{'name': x.get('name') or '', 'id': x.get('department_id') or ''}
                for x in (it.get('buy_departments') or [])]
        sells = [{'name': x.get('name') or '', 'id': x.get('department_id') or ''}
                 for x in (it.get('sell_departments') or [])]
        out['groups'].append({
            'range_days': int(_num(it.get('range_days'), 1) or 1),
            'start': it.get('start_date') or '',
            'end': it.get('end_date') or '',
            'org_buy': _yi(it.get('org_buy_value')),
            'org_sell': _yi(it.get('org_sell_value')),
            'org_net': _yi(it.get('org_net_value')),
            'org_buy_num': int(_num(it.get('org_buy_num'))),
            'org_sell_num': int(_num(it.get('org_sell_num'))),
            'buys': buys, 'sells': sells,
        })
    _fl._save_json(name, out)
    return out


def lhb_dept(dept_id, page=1, size=20):
    """营业部画像：经典战绩 + 近期上榜记录"""
    did = str(dept_id or '').strip()
    if not did:
        return {'ok': False, 'error': '缺 department_id'}
    out = {'ok': True, 'id': did}
    try:
        c = _fq._req(DEPT_CLASSIC, {'department_id': did}, timeout=20, api_name='dragon-tiger-department-classic-record')
        out['name'] = c.get('department_name') or ''
        cr = c.get('classic_record') or {}
        out['classic'] = {'thscode': cr.get('thscode') or '', 'name': cr.get('name') or '',
                          'pattern': cr.get('board_pattern') or ''}
    except Exception as e:
        log('ERR', 'lhb_dept_classic', str(e))
        out['classic'] = None
    try:
        h = _fq._req(DEPT_HISTORY, {'department_id': did, 'page': int(page), 'size': int(size)},
                     timeout=20, api_name='dragon-tiger-department-history')
        rows = []
        for x in (h.get('item') or []):
            rows.append({'ticker': str(x.get('thscode') or '')[:6], 'name': x.get('name') or '',
                         'date': x.get('trade_date') or '',
                         'change': round(_num(x.get('change_ratio')) * 100, 2),
                         'net': _yi(x.get('net_buy_amount')),
                         'rank_type': int(_num(x.get('rank_type')))})
        pg = h.get('pagination') or {}
        out['history'] = {'rows': rows, 'page': int(_num(pg.get('page'), page)),
                          'size': int(_num(pg.get('size'), size)), 'total': int(_num(pg.get('total')))}
    except Exception as e:
        log('ERR', 'lhb_dept_history', str(e))
        out['history'] = {'rows': [], 'total': 0}
    return out


def hot_list(period='day', limit=100):
    """同花顺热榜（period: day / hour），最多 100 名"""
    per = 'hour' if str(period).lower() == 'hour' else 'day'
    lim = max(1, min(100, int(limit or 100)))
    name = 'hot_%s_%s' % (per, lim)
    cache = _fl._load_json(name, max_age=CACHE_TTL['hot'])
    if cache:
        return cache
    try:
        r = _fq._req(HOT_LIST, {'period': per, 'limit': lim}, timeout=20, api_name='hot-stock-list')
    except Exception as e:
        log('ERR', 'hot_list', str(e))
        return {'ok': False, 'error': str(e)[:200]}
    rows = []
    for it in (r.get('item') or []):
        rows.append({
            'rank': int(_num(it.get('rank'))),
            'ticker': str(it.get('ticker') or ''),
            'name': it.get('name') or '',
            'heat': _num(it.get('heat')),
            'rank_change': int(_num(it.get('rank_change'))),
            'trend': it.get('rank_trend') or 'flat',
        })
    out = {'ok': True, 'period': per, 'count': len(rows), 'ts': r.get('timestamp') or 0, 'rows': rows}
    _fl._save_json(name, out)
    return out


if __name__ == '__main__':
    print(json.dumps({k: (v if not isinstance(v, dict) else {kk: vv for kk, vv in list(v.items())[:6]})
                      for k, v in [('lhb', lhb_list()), ('hot', hot_list())]}, ensure_ascii=False)[:1200])
