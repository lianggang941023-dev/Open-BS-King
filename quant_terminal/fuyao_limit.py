# -*- coding: utf-8 -*-
"""同花顺 fuyao · 涨停/跌停/炸板/连板天梯/涨停统计 + A股交易日历

v2.187 (2026-10-23) 新增。此前这些数据全走东财 push2ex（涨停池）+ 快照反推（交易日历）。

同花顺这一组接口的价值：
  · 涨停池带 **涨停原因**（limit_up_reason）、**峰值封单额**（max_seal_money）、
    **连板文本**（continue_day_text，能表达「5天4板」这种非连续连板）— 东财池都没有。
  · 连板天梯能一次拿到近 30 个交易日 × 6 个板位的梯队矩阵。
  · 涨停统计给封板率 / 打开家数 / 各板晋级率。
  · 交易日历官方口径，不用再拿快照 trade_date 反推。

缺的字段（同花顺池子里本来就没有）：
  · 换手率 turnover  → 置 0（妖龙评分里那 10 分固定拿不到，属均匀损失，不影响排序）
  · 流通市值 flow_mv → 用本地当日快照的流通市值补（intraday_snapshots.json）
  · 所属行业 industry → 用本地 industry_map 补（T1 已换成同花顺口径）
  · 炸板次数 zhaban  → 用炸板池反查（同一只票在炸板池里出现过即 ≥1）

所有接口都写了本地磁盘缓存：交易日历 6 小时、池子按「日期 + 1 分钟」。
"""
import io
import json
import os
import time
import urllib.parse
from datetime import datetime, timedelta

try:
    from . import fuyao as _fq
except Exception:                                    # 直接跑本文件时
    import fuyao as _fq

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, 'kline_data', 'fuyao_cache')
LOG_FILE = os.path.join(CACHE_DIR, 'fuyao_limit.log')

POOL_UP = '/api/a-share/special-data/limit-up-pool'
POOL_DOWN = '/api/a-share/special-data/limit-down-pool'
POOL_BREAK = '/api/a-share/special-data/limit-break-pool'
LADDER = '/api/a-share/special-data/limit-up-ladder'
STAT = '/api/a-share/special-data/limit-up-statistics'
CALENDAR = '/api/a-share/calendar/trading-days'

PAGE_SIZE = 200                                      # 接口上限
MAX_PAGES = 8                                        # 最多 1600 条，够 A 股一天用
_POOL_TTL = 60                                       # 盘中池子缓存秒数

_MEM = {}                                            # 内存缓存 {key: (ts, value)}
_PRUNE_LAST = [0.0]                                  # 上次清理按日缓存的时间(v2.399)
# v2.399: 每次返回的数据实际是什么时候的 —— {'ts': float, 'cached': bool}
#   上游挂掉回落磁盘缓存时 ts = 缓存文件的 mtime(真实采集时刻), cached=True
#   ⇒ 界面能显示「数据 15:41 (本地缓存)」而不是假装是刚刚拉的
_SRC = {}
# v2.405 (用户 m10129): 区分「拉不到数据」和「拉到了、确实 0 条」。
#   _FETCH     : 最近一次真实拉取的结果 {api:date -> {ok, confirmed_empty, total, err, ts, cached}}
#   _POOL_TOTAL: 上游 pagination.total 的原始值 {api:date -> int|None}
_FETCH = {}
_POOL_TOTAL = {}


# ---------------------------------------------------------------- 基础设施
def log(level, api, detail):
    """写 fuyao_limit.log（>2MB 截半），方便事后查限流/失败。"""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 2 * 1024 * 1024:
            with io.open(LOG_FILE, encoding='utf-8', errors='replace') as f:
                keep = f.read()[-1024 * 1024:]
            with io.open(LOG_FILE, 'w', encoding='utf-8', newline='\n') as f:
                f.write(keep)
        line = '[%s] %-5s %-22s %s\n' % (
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, api, detail)
        with io.open(LOG_FILE, 'a', encoding='utf-8', newline='\n') as f:
            f.write(line)
    except Exception:
        pass


def configured():
    """同花顺可用（enabled + 有 key）。调用方据此决定要不要走同花顺。"""
    try:
        return bool(_fq.configured())
    except Exception:
        return False


def _cache_file(name):
    return os.path.join(CACHE_DIR, name + '.json')


def _load_json(name, max_age=None):
    try:
        p = _cache_file(name)
        if max_age is not None and time.time() - os.path.getmtime(p) > max_age:
            return None
        with io.open(p, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _save_json(name, obj):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        p = _cache_file(name)
        tmp = p + '.tmp'
        with io.open(tmp, 'w', encoding='utf-8', newline='\n') as f:
            f.write(json.dumps(obj, ensure_ascii=False))
        os.replace(tmp, p)
    except Exception as e:
        log('WARN', 'cache', '写 %s 失败: %s' % (name, e))


def prune_cache(keep=40):
    '''清理「按日期分文件」的旧缓存（v2.399, 用户 m08249 关心磁盘不会被撑大）。

    只认文件名末尾是 8 位数字日期的：``pool_limit-up-pool_20261127`` / ``lhb_all_20261126``。
    每个前缀只留最近 keep 个交易日，多的删掉。``lhbinst_<代码>_<日期>`` 前缀里带代码,
    各自独立成组, 不会被误删。一小时最多跑一次，失败一律吞掉。
    '''
    now = time.time()
    if now - _PRUNE_LAST[0] < 3600:
        return 0
    _PRUNE_LAST[0] = now
    n = 0
    try:
        groups = {}
        for fn in os.listdir(CACHE_DIR):
            if not fn.endswith('.json'):
                continue
            base = fn[:-5]
            i = base.rfind('_')
            if i <= 0:
                continue
            d = base[i + 1:]
            if len(d) != 8 or not d.isdigit():
                continue
            groups.setdefault(base[:i], []).append((d, fn))
        for _pre, lst in groups.items():
            lst.sort(reverse=True)
            for _d, fn in lst[keep:]:
                try:
                    os.unlink(os.path.join(CACHE_DIR, fn))
                    n += 1
                except Exception:
                    pass
        if n:
            log('INFO', 'cache', '清理旧缓存 %d 个 (每类保留最近 %d 日)' % (n, keep))
    except Exception:
        pass
    return n


def _day_ms(yyyymmdd=None):
    """'YYYY-MM-DD' / 'YYYYMMDD' / None(今天) → Asia/Shanghai 当天 00:00 的毫秒戳。"""
    if not yyyymmdd:
        d = datetime.now()
    else:
        s = str(yyyymmdd).replace('-', '')[:8]
        d = datetime(int(s[:4]), int(s[4:6]), int(s[6:8]))
    return int(d.timestamp() * 1000)


def _norm_date(v):
    if not v:
        return datetime.now().strftime('%Y-%m-%d')
    s = str(v).replace('-', '')[:8]
    return '%s-%s-%s' % (s[:4], s[4:6], s[6:8])


# ---------------------------------------------------------------- 交易日历
def trading_days(refresh_hours=6):
    """A股近一年交易日 ['YYYY-MM-DD', ...]（升序）。

    官方是固定窗口 [今日-1年, 今日]，无入参。落盘缓存 6 小时，
    进程内也缓存 —— 它会在 is_holiday() 里被高频调用。
    """
    key = 'tdays'
    hit = _MEM.get(key)
    if hit and time.time() - hit[0] < refresh_hours * 3600:
        return hit[1]
    disk = _load_json('trading_days', max_age=refresh_hours * 3600)
    if disk and disk.get('days'):
        _MEM[key] = (time.time(), disk['days'])
        return disk['days']
    try:
        r = _fq._req(CALENDAR, api_name='calendar/trading-days')
        items = (r or {}).get('item') or []
        days = []
        for it in items:
            s = str(it.get('date') or '')[:8]
            if len(s) == 8 and s.isdigit():
                days.append('%s-%s-%s' % (s[:4], s[4:6], s[6:8]))
        days = sorted(set(days))
        if days:
            _save_json('trading_days', {'days': days,
                                        'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
            _MEM[key] = (time.time(), days)
            log('INFO', 'calendar', '拉到 %d 个交易日 (%s ~ %s)' % (len(days), days[0], days[-1]))
        return days
    except Exception as e:
        log('ERROR', 'calendar', '拉取失败: %s' % e)
        old = _load_json('trading_days')
        return (old or {}).get('days') or []


def is_trading_day(day=None):
    """True/False=官方口径明确；None=日历不可用（调用方必须退回旧判定，别硬判）。"""
    days = trading_days()
    if not days:
        return None
    d = _norm_date(day)
    if d < days[0] or d > days[-1]:
        return None                                  # 超出一年窗口 → 不敢判
    return d in set(days)


def next_trading_day(day=None, back=1):
    """'YYYY-MM-DD' 的下/上 back 个交易日（日历不全时返回 ''）。"""
    days = trading_days()
    d = _norm_date(day)
    if d not in days:
        return ''
    i = days.index(d) + back
    return days[i] if 0 <= i < len(days) else ''


# ---------------------------------------------------------------- 池子
def default_pool_date():
    """池子默认查哪一天：盘中/收盘后=今天；开盘前或非交易日=上一个交易日。

    坑（2026-10-23 实测）：涨停池接口在没有数据时会自己回退到最近交易日，
    但跌停池/炸板池不会 —— 凌晨查「今天」会得到「涨停 33 只、跌停 0 只」这种
    半真半假的组合。所以由我们自己统一算一个有效交易日。
    """
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    days = trading_days() or []
    if days:
        _ds = set(days)
        if today in _ds:
            # 今天是交易日: 开盘前(<09:30)当天的池子还没生成 ⇒ 退到上一个交易日
            if now.hour * 100 + now.minute < 930:
                _prev = [d for d in days if d < today]
                return max(_prev) if _prev else today
            return today
        # v2.212 (2026-10-24 DEBUG #2): 今天不在交易日历里(=周末/节假日) ⇒ 直接取
        #   日历中「今天之前最后一个交易日」。
        #   原写法只认 is_trading_day() 的 True/False, 但它对「超出日历末日」
        #   会返回 None(不敢判), 于是落到最后一句 return today = 周六,
        #   涨停池/跌停池/炸板池全部查空 ⇒ 界面 ok:true 但 rows:[] 半真半假。
        _prev = [d for d in days if d < today]
        if _prev:
            return max(_prev)
    # 日历彻底不可用(空) ⇒ 退回旧口径, 宁可能查错也不要全空
    ok = is_trading_day(today)
    if ok is False:
        return next_trading_day(today, -1) or today
    if ok and now.hour * 100 + now.minute < 930:
        return next_trading_day(today, -1) or today
    return today


def _file_ts(name):
    '''缓存文件的 mtime = 这份数据最后一次真正抓到的时刻; 读不到就退回现在'''
    try:
        return os.path.getmtime(os.path.join(CACHE_DIR, name + '.json'))
    except Exception:
        return time.time()


def last_src(api, date):
    '''这份数据实际是什么时候拿到的 —— {'ts': float, 'cached': bool}（v2.399）'''
    try:
        return dict(_SRC.get('%s:%s' % (api, _norm_date(date))) or {})
    except Exception:
        return {}


def last_fetch(api, date):
    '''最近一次「实际去拉」的结果（v2.405, 用户 m10129）。

    ok=True 且 confirmed_empty=True ⇒ 上游正常响应、明确说这个交易日 0 条（真没有, 不是故障）
    ok=False                        ⇒ 拉取失败（异常/上游报错）, 现在返回的可能是旧缓存
    total                           ⇒ 上游 pagination.total（判断到底是 0 还是没给数）
    '''
    try:
        return dict(_FETCH.get('%s:%s' % (api, _norm_date(date))) or {})
    except Exception:
        return {}


def _pool(api, date=None, extra=None):
    """通用分页拉取 → (items, meta)。"""
    out, page, meta = [], 1, {}
    _k = '%s:%s' % (api, _norm_date(date))
    _POOL_TOTAL[_k] = None            # v2.405: 先清掉上一次的值, 拉取失败时不会误用旧 total
    dm = _day_ms(date)
    while page <= MAX_PAGES:
        params = {'date_ms': dm, 'page': page, 'size': PAGE_SIZE}
        if extra:
            params.update(extra)
        r = _fq._req(api, params=params, api_name=api.rsplit('/', 1)[-1])
        items = (r or {}).get('item') or []
        pg = (r or {}).get('pagination') or {}
        if not meta:
            meta = {'total': pg.get('total'), 'pages': pg.get('pages'),
                    'timestamp': (r or {}).get('timestamp')}
        out.extend(items)
        _POOL_TOTAL[_k] = meta.get('total')     # v2.405
        if not pg.get('pages') or page >= int(pg.get('pages') or 1):
            break
        if len(items) < PAGE_SIZE:
            break
        page += 1
    return out, meta


def _cached_pool(api, date, builder):
    """池子缓存：同一交易日 + 60 秒内复用（盘中避免重复拉）。"""
    d = _norm_date(date)
    key = '%s:%s' % (api, d)
    hit = _MEM.get(key)
    if hit and time.time() - hit[0] < _POOL_TTL:
        return hit[1]
    name = 'pool_' + api.rsplit('/', 1)[-1] + '_' + d.replace('-', '')
    disk = _load_json(name, max_age=_POOL_TTL)
    if disk:
        _MEM[key] = (time.time(), disk)
        _SRC[key] = {'ts': _file_ts(name), 'cached': True}
        return disk
    # v2.399 (用户 m08249): 上游整块挂掉时 builder() 会抛异常(同花顺 code=5003 数据源不可用)。
    #   以前这里直接向上抛 ⇒ 连板梯队整块空、界面显示「今日无连板数据」, 看着像"今天真没有"。
    #   现在先按「返回空」那套逻辑回捞本交易日的磁盘旧缓存; 真的一条都没有才继续往上抛,
    #   让调用方(ladder_scan)知道失败并去读 lastgood 单文件存档。
    try:
        val = builder()
    except Exception as _e:
        _api_name = api.rsplit('/', 1)[-1]
        _FETCH[key] = {'ok': False, 'confirmed_empty': False, 'total': None,
                       'err': str(_e)[:200], 'ts': time.time(), 'cached': True}
        _stale = _MEM.get('stale:' + key)
        if _stale and _stale[1]:
            log('WARN', _api_name, '拉取失败(%s)，沿用上次非空 %d 条(内存)' % (str(_e)[:60], len(_stale[1])))
            _SRC.setdefault(key, {'ts': time.time(), 'cached': True})
            return _stale[1]
        _old = _load_json(name, max_age=None)         # 不限年龄, 文件名已带日期不会串日
        if _old:
            _MEM[key] = (time.time(), _old)
            _MEM['stale:' + key] = (time.time(), _old)
            _SRC[key] = {'ts': _file_ts(name), 'cached': True}
            log('WARN', _api_name, '拉取失败(%s)，沿用磁盘旧缓存 %d 条' % (str(_e)[:60], len(_old)))
            return _old
        log('WARN', _api_name, '拉取失败且无任何旧缓存: %s' % str(_e)[:120])
        raise
    # 空结果不覆盖缓存：池子偶尔会返回空（接口抖动/限流），不能让一次抖动把好数据抹成空表。
    #   同一交易日里只要曾经拿到过非空，就一直沿用它，直到拿到新的非空。
    #
    # v2.405 (用户 m10129): 「拉不到数据」和「拉到了、确实 0 条」是两件事, 不能混为一谈。
    #   上游正常返回 total=0（例如全市场几乎没有跌停的普涨日）时必须如实显示 0,
    #   而不是当成拉取失败去顶上旧存档 —— 那样界面会显示昨天的 7 只跌停 +
    #   橙字「上游暂时不可用」, 看着像接口坏了, 其实今天真没有。
    _items = val[0] if (isinstance(val, tuple) and val) else (val or [])
    _total = _POOL_TOTAL.get(key)
    _empty = (len(_items) == 0)
    _confirmed = False
    if _empty:
        try:
            _confirmed = (_total is not None) and (int(_total) == 0)
        except Exception:
            _confirmed = False
    _FETCH[key] = {'ok': True, 'confirmed_empty': bool(_confirmed), 'total': _total,
                   'err': '', 'ts': time.time(), 'cached': False}
    if _confirmed:
        log('INFO', api.rsplit('/', 1)[-1], '上游确认该交易日 0 条 (total=0)')
        _MEM[key] = (time.time(), val)
        _SRC[key] = {'ts': time.time(), 'cached': False}
        return val
    if _empty:
        # v2.285 (2026-11-12): 兜底原来只查 _MEM['stale:'+key], 服务重启后内存是空的,
        #   一次接口抖动就会让整个交易日显示 0 条, 而且是静默的
        #   (板面表现为「炸板 0 / 封板率 100%」, 看着像数据对, 其实整块错)。
        #   ⇒ 内存没有再往磁盘捞一次**不限年龄**的同日缓存: 文件名里已经带日期,
        #     不会串到别的交易日; 捞到就补回 _MEM 并沿用。
        api_name = api.rsplit('/', 1)[-1]
        stale = _MEM.get('stale:' + key)
        if stale and stale[1]:
            log('WARN', api_name, '本次返回空，沿用上次非空 %d 条(内存)' % len(stale[1]))
            _SRC.setdefault(key, {'ts': time.time(), 'cached': True})
            return stale[1]
        old = _load_json(name, max_age=None)          # 不限年龄, 只要是这个交易日就行
        if old:
            _MEM[key] = (time.time(), old)
            _MEM['stale:' + key] = (time.time(), old)
            _SRC[key] = {'ts': _file_ts(name), 'cached': True}
            log('WARN', api_name, '本次返回空，沿用磁盘旧缓存 %d 条' % len(old))
            return old
        log('WARN', api_name, '本次返回空且无任何旧缓存可用(该交易日首次就失败)')
        _MEM[key] = (time.time(), val)
        return val
    _FETCH[key] = {'ok': True, 'confirmed_empty': False, 'total': _total,
                   'err': '', 'ts': time.time(), 'cached': False}
    _MEM[key] = (time.time(), val)
    _MEM['stale:' + key] = (time.time(), val)
    _SRC[key] = {'ts': time.time(), 'cached': False}
    _save_json(name, val)
    return val


def _industry_map():
    """本地 industry_map.csv → {6位码: 行业名}（T1 已是同花顺口径）。"""
    key = 'ind'
    hit = _MEM.get(key)
    if hit and time.time() - hit[0] < 1800:
        return hit[1]
    out = {}
    try:
        p = os.path.join(BASE_DIR, 'kline_data', 'industry_map.csv')
        with io.open(p, encoding='utf-8-sig') as f:
            for i, line in enumerate(f):
                if i == 0:
                    continue
                parts = line.rstrip('\n').rstrip('\r').split(',')
                if len(parts) >= 2 and parts[0]:
                    out[str(parts[0]).strip()[-6:]] = parts[1].strip()
    except Exception as e:
        log('WARN', 'industry_map', str(e))
    _MEM[key] = (time.time(), out)
    return out


def _intraday_caps():
    """当日快照里的流通市值 {6位码: 亿}，给 fuyao 池子补 flow_mv。"""
    key = 'caps'
    hit = _MEM.get(key)
    if hit and time.time() - hit[0] < 120:
        return hit[1]
    out = {}
    try:
        try:
            from . import intraday as _id
        except Exception:
            import intraday as _id
        caps = _id.caps() or {}
        for k, v in caps.items():
            try:
                fm = float(v[1]) if isinstance(v, (list, tuple)) and len(v) > 1 else 0.0
            except (TypeError, ValueError):
                fm = 0.0
            if fm > 0:
                out[str(k)[-6:]] = fm
    except Exception as e:
        log('WARN', 'intraday.caps', str(e))
    _MEM[key] = (time.time(), out)
    return out


def _to_board(code):
    try:
        try:
            from .dragon import board_of
        except Exception:
            from dragon import board_of
        return board_of(code)
    except Exception:
        return ''


def _raw_limit_up(date=None):
    items, meta = _pool(POOL_UP, date)
    rows = []
    for it in items:
        code = str(it.get('ticker') or '')[:6]
        if len(code) != 6 or not code.isdigit():
            continue
        t = str(it.get('limit_up_time') or '')
        rows.append({
            'code': code,
            'name': it.get('name') or code,
            'pct': round(_num(it.get('price_change_ratio_pct')), 2),
            'price': _num(it.get('last_price')),
            'turnover': 0.0,                              # 同花顺池子不给
            'seal_money': _num(it.get('seal_money')),
            'flow_mv': 0.0,                               # 下面补
            'first_time': t,
            'last_time': t,
            'zhaban': 0,                                  # 下面用炸板池反查
            'lianban': int(_num(it.get('continue_day_cnt'))),
            'industry': '',                               # 下面补
            'board': _to_board(code),
            # v2.187 新增字段（前端可选用，不影响老消费方）
            'reason': (it.get('limit_up_reason') or '').strip(),
            'lianban_text': (it.get('continue_day_text') or '').strip(),
            'max_seal': _num(it.get('max_seal_money')),
            'is_new': bool(it.get('is_new')),
            'is_st': bool(it.get('is_st')),
        })
    ind = _industry_map()
    caps = _intraday_caps()
    # v2.190: intraday.caps() 给的是**亿元**，而东财口径的 flow_mv 是**元** ⇒ 这里统一乘 1e8，
    #   否则 engine 的 seal_ratio = seal_money/flow_mv*100 会大 1e8 倍（前端那个看不懂的百分比就是它）。
    for r in rows:
        r['industry'] = ind.get(r['code'], '')
        r['flow_mv'] = caps.get(r['code'], 0.0) * 1e8
    # 同花顺涨停池不给炸板次数，只能反查炸板池：在里面就算炸过 1 次
    try:
        _bc = set(x['code'] for x in (limit_break_pool() or []))
        for r in rows:
            if r['code'] in _bc:
                r['zhaban'] = 1
    except Exception:
        pass
    rows.sort(key=lambda x: (-x['lianban'], -x['seal_money']))
    try:
        prune_cache()
    except Exception:
        pass
    return rows, meta


def _num(v):
    try:
        f = float(v)
        return f if f == f else 0.0
    except (TypeError, ValueError):
        return 0.0


def limit_up_pool(date=None):
    """涨停池（已补齐行业/流通市值/炸板次数）→ (rows, meta)。"""
    if date is None:
        date = default_pool_date()
    rows, meta = _cached_pool(POOL_UP, date, lambda: _raw_limit_up(date))
    rows = [dict(r) for r in rows]                       # 别让调用方改到缓存
    # 炸板池反查 → zhaban 次数（同花顺不直接给次数，用「在炸板池出现过」记为 1）
    if rows:
        try:
            brk = limit_break_pool(date)
            m = {}
            for b in brk:
                m[b['code']] = m.get(b['code'], 0) + 1
            for r in rows:
                if r['code'] in m:
                    r['zhaban'] = max(r['zhaban'], m[r['code']])
        except Exception as e:
            log('WARN', 'break-pool', '炸板池反查失败: %s' % e)
    return rows, meta


def limit_down_pool(date=None):
    """跌停池。"""
    # v2.405: 先把日期定死 —— build 里 _pool 用的日期必须和 _cached_pool 的 key 是同一个,
    #   否则 date=None 时 _pool 查「今天」而缓存 key 是「上一交易日」, total 对不上。
    date = date or default_pool_date()
    def build():
        items, _meta = _pool(POOL_DOWN, date)
        out = []
        for it in items:
            code = str(it.get('ticker') or '')[:6]
            if len(code) != 6 or not code.isdigit():
                continue
            out.append({'code': code, 'name': it.get('name') or code,
                        'pct': round(_num(it.get('price_change_ratio_pct')), 2),
                        'price': _num(it.get('last_price')),
                        'first_time': str(it.get('first_limit_down_time') or it.get('limit_down_time') or ''),
                        'last_time': str(it.get('last_limit_down_time') or ''),
                        'board': _to_board(code)})
        return out
    return _cached_pool(POOL_DOWN, date, build)


def limit_break_pool(date=None):
    """炸板池（曾涨停后开板）。"""
    def build():
        items, _meta = _pool(POOL_BREAK, date)
        out = []
        for it in items:
            code = str(it.get('ticker') or '')[:6]
            if len(code) != 6 or not code.isdigit():
                continue
            out.append({'code': code, 'name': it.get('name') or code,
                        'pct': round(_num(it.get('price_change_ratio_pct')), 2),
                        'price': _num(it.get('last_price')),
                        'break_time': str(it.get('break_time') or ''),
                        'board': _to_board(code)})
        return out
    return _cached_pool(POOL_BREAK, date or default_pool_date(), build)


def limit_ladder():
    """近 30 交易日 × 6 板位的连板天梯（无入参）。"""
    def build():
        r = _fq._req(LADDER, api_name='limit-up-ladder')
        return {'window': (r or {}).get('window') or {},
                'item': (r or {}).get('item') or [],
                'timestamp': (r or {}).get('timestamp')}
    key = 'ladder'
    hit = _MEM.get(key)
    if hit and time.time() - hit[0] < _POOL_TTL:
        return hit[1]
    val = build()
    _MEM[key] = (time.time(), val)
    return val


def limit_statistics(date=None):
    """涨停汇总 + 各板晋级率。"""
    try:
        params = {'date': _norm_date(date or default_pool_date())}
        r = _fq._req(STAT, params=params, api_name='limit-up-statistics')
        return r or {}
    except Exception as e:
        log('ERROR', 'limit-up-statistics', str(e))
        return {}


def selftest():
    """手动自检：python -m quant_terminal.fuyao_limit"""
    print('configured =', configured())
    td = trading_days()
    print('交易日历 %d 天: %s ... %s' % (len(td), td[0] if td else '-', td[-1] if td else '-'))
    print('今天是不是交易日 =', is_trading_day())
    rows, meta = limit_up_pool()
    print('涨停池 %d 只 (total=%s)' % (len(rows), (meta or {}).get('total')))
    for r in rows[:8]:
        print('   %s %-6s %5.2f%% %-4s 封%s万 原因=%s 行业=%s' % (
            r['code'], r['name'], r['pct'], r['lianban_text'] or ('%d板' % r['lianban']),
            round(r['seal_money'] / 1e4, 0), r['reason'] or '-', r['industry'] or '-'))
    dn = limit_down_pool()
    bk = limit_break_pool()
    print('跌停池 %d 只, 炸板池 %d 只' % (len(dn), len(bk)))
    st = limit_statistics()
    if st:
        print('统计: 涨停 %s 家 / 触板 %s / 封板率 %s' % (
            st.get('limit_up_stock_count'), st.get('touched_limit_up_stock_count'),
            round(float(st.get('seal_rate') or 0) * 100, 1)))
    lad = limit_ladder()
    print('天梯窗口 %s 天' % ((lad.get('window') or {}).get('length')))


if __name__ == '__main__':
    selftest()
