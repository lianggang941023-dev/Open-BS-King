# -*- coding: utf-8 -*-
"""宽基指数日线本地存档 + 深度历史回补（v2.359）

用途：给「市场总览 → 特色指标 / 指数一览」两个页供数据。

为什么要单开一个模块，而不是直接用 engine 里那套？
    1. `engine.INDEX_DAILY_CODES` 写死 7 个代码、`_fetch_index_full()` 只拉
       腾讯 **最近 2000 根**（2018-06 起）—— 中证红利要 2005 年、创业板指要
       2010 年，2000 根根本不够。
    2. 这里要能对**任意**指数代码按需建库（后面用户自己挑指数加进「指数一览」）。

关键发现（2026-11-21 实测）：
    腾讯 `_fqkline_req(code, count='2000', end='YYYY-MM-DD')` 会拼成
        param={code},day,1990-01-01,{end},{count},qfq
    ⇒ **用 end 往前翻页**，一次 2000 根，可以一路翻到指数上市日。
        sh000922 end=2010-12-31 -> 1457 根 2005-01-04 ~ 2010-12-31
        sz399006 end=2014-12-31 -> 1112 根 2010-06-01 ~ 2014-12-31
    所以**不依赖东财**（东财 https 被封、http 虽通但属规避手段）。

存档格式：与 engine 的 index_daily 完全一致（共享目录、共享文件）
    <项目根>/kline_data/index_daily/SH000922.csv
    date,open,close,high,low,volume     纯 LF、无 BOM、价格 3 位小数
这样 engine 的 guard/sync 线程和这里看到的是同一份文件，不会打架。
"""

import io
import os
import threading
import time
from datetime import datetime, timedelta

from quant_terminal import engine as E
from quant_terminal import idx_registry as _idxr   # v2.409: 海外指数代码表

# ── 指数登记表 ────────────────────────────────────────────────────────────
# code → (显示名, 短名)。顺序 = 「指数一览」里的默认顺序。
IDX_INFO = {
    'sh000001': ('上证指数', '上证'),
    'sz399001': ('深证成指', '深证成指'),
    'sz399006': ('创业板指', '创业板指'),
    'sh000300': ('沪深300', '沪深300'),
    'sz399905': ('中证500', '中证500'),
    'sh000688': ('科创50', '科创50'),
    'sh000680': ('科创综指', '科创综指'),
    # 特色指标的分母（同时也是常见宽基，后面用户挑的时候可直接进「指数一览」）
    'sh000922': ('中证红利', '中证红利'),
}

# v2.363 (用户 m39345): 顶栏可替换的那几只 + 同花顺全A，也进「指数一览」。
#   它们不在 idx_registry.ALL 之外，这里只是补齐显示名（short 与 name 同）。
#   ⚠ 同花顺全A(883957) 走 10jqka 通路，日K 只有 2018-05-17 起 2067 根 —— 正常。
for _c, _nm in (('883957', '同花顺全A'), ('sh000852', '中证1000'),
                ('sh000985', '中证全指'), ('sh000016', '上证50')):
    IDX_INFO.setdefault(_c, (_nm, _nm))

# v2.367 (用户 m40047): 5 只沪深300ETF 进「指数一览」。
#   **短名用用户指定的**（原话「ETF 的名称比较长，需要简化一下前端显示就叫
#   300华泰，300易方，300华夏，300嘉实，300天弘」），全名只给 tooltip 用。
#   src='em' ⇒ 取数/建库走 engine 的东财分支（见下面的 ensure/sync 分流）。
ETF_CODES = ('sh510300', 'sh510310', 'sh510330', 'sz159919', 'sh515330')
for _c, _nm, _sh in (('sh510300', '沪深300ETF华泰柏瑞', '300华泰'),
                     ('sh510310', '沪深300ETF易方达', '300易方'),
                     ('sh510330', '沪深300ETF华夏', '300华夏'),
                     ('sz159919', '沪深300ETF嘉实', '300嘉实'),
                     ('sh515330', '沪深300ETF天弘', '300天弘')):
    IDX_INFO[_c] = (_nm, _sh)

# 「指数一览」的 12 只（v2.363 用户 m39345：七大 + 可替换候选 + 中证红利）
#   顺序 = 左列从上到下；与 idx_registry.LIST_CODES 保持一致。
LIST_CODES = ('sh000001', 'sz399001', 'sz399006', 'sh000680', 'sh000688',
              'sh000300', 'sz399905',
              '883957', 'sh000852', 'sh000985', 'sh000016', 'sh000922',
              # v2.367 (用户 m40047): 5 只沪深300ETF 接在最后
              ) + ETF_CODES + _idxr.OVERSEAS_CODES


# v2.409 (用户 m11025): 14 个海外指数。显示名/短名与 idx_registry 保持一份口径,
#   但短名这里用更短的 (左列 244px 放得下) —— 全名给 tooltip。
for _c, _nm, _sh in (
        ('us.dji', '道琼斯', '道指'),
        ('us.ixic', '纳斯达克', '纳指'),
        ('us.ndx', '纳斯达克100', '纳100'),
        ('us.inx', '标普500', '标普'),
        ('hk.hsi', '恒生指数', '恒生'),
        ('jp.n225', '日经225', '日经'),
        ('kr.ks11', '韩国KOSPI', '韩国'),
        ('tw.twii', '台湾加权', '台湾'),
        ('au.as51', '澳大利亚标普200', '澳洲'),
        ('uk.ftse', '英国富时100', '富时100'),
        ('de.gdaxi', '德国DAX30', '德国'),
        ('fr.fchi', '法国CAC40', '法国'),
        ('in.sensex', '印度孟买SENSEX', '印度'),
        ('sg.cn00y', '富时中国A50期货', 'A50'),
):
    IDX_INFO[_c] = (_nm, _sh)

# 特色指标的三组比值 —— (分子, 分母, 显示名)
PAIRS = (
    ('sz399006', 'sh000922', '创业板指/中证红利'),
    ('sh000680', 'sh000922', '科创综指/中证红利'),
    ('sh000688', 'sh000922', '科创50/中证红利'),
)

# 特色指标页每张图要的两个代码的并集 = 存档预热范围
FEATURE_CODES = tuple(sorted({c for a, b, _ in PAIRS for c in (a, b)}))

_LK = threading.RLock()
_MEM = {}          # code -> {'ts': epoch, 'rows': [...]}   进程内一层缓存
_MEM_TTL = 600.0   # 10 分钟；文件变了会靠 mtime 失效，见 _disk_mtime
_MTIME = {}        # code -> 落盘 mtime，用来判断内存缓存是否过期

_FLOOR = '1990-01-01'
_PAGE = '2000'


def _is_em(code):
    """是不是走东财通路的标的（v2.367: 目前就是 5 只沪深300ETF）。

    判据用 idx_registry.src_of()，不在这里再抄一份代码表 —— 两份表迟早会不一致。
    """
    try:
        return E._idxr.src_of((code or '').lower()) == 'em'
    except Exception:
        return False


def _is_overseas(code):
    """v2.409: 是不是海外指数（走 overseas_index.py 那条独立取数通路）。

    与 _is_em 同款判据 —— 只看 idx_registry.src_of()，不在这里再抄一份代码表。
    """
    try:
        return _idxr.src_of((code or '').lower()) == 'overseas'
    except Exception:
        return False


def name_of(code):
    """指数显示名。登记表没有的代码返回大写代码本身，不抛异常。"""
    it = IDX_INFO.get((code or '').lower())
    return it[0] if it else (code or '').upper()


def short_of(code):
    it = IDX_INFO.get((code or '').lower())
    return it[1] if it else (code or '').upper()


def _fpath(code):
    return E._idx_fpath(code)


def _disk_mtime(code):
    try:
        return os.path.getmtime(_fpath(code))
    except OSError:
        return 0.0


# ── 本地读写（直接复用 engine 的实现，保证格式与锁一致）────────────────────
def load(code):
    """读本地日线（带一层内存缓存）。没有文件返回 []。"""
    code = (code or '').lower()
    if not code:
        return []
    mt = _disk_mtime(code)
    with _LK:
        c = _MEM.get(code)
        if c and c.get('ts') and (time.time() - c['ts'] < _MEM_TTL) and c.get('mt') == mt:
            return c['rows']
    rows = E.read_index_daily(code) or []
    with _LK:
        _MEM[code] = {'ts': time.time(), 'rows': rows, 'mt': mt}
    return rows


def save(code, rows):
    """原子落盘（engine.write_index_daily：tmp + os.replace + 文件锁）。"""
    code = (code or '').lower()
    ok = E.write_index_daily(code, rows)
    if ok:
        with _LK:
            _MEM[code] = {'ts': time.time(), 'rows': rows, 'mt': _disk_mtime(code)}
    return ok


def meta(code):
    """{'code','name','short','n','start','end','mtime','src'} —— 不联网。"""
    code = (code or '').lower()
    rows = load(code)
    return {
        'code': code,
        'name': name_of(code),
        'short': short_of(code),
        'n': len(rows),
        'start': rows[0]['date'] if rows else '',
        'end': rows[-1]['date'] if rows else '',
        'mtime': _disk_mtime(code),
        'src': 'local',
    }


# ── 深度回补 ──────────────────────────────────────────────────────────────
def _page(code, end):
    """腾讯取一页。end='' 表示最新一页（最近 2000 根）。"""
    try:
        j = E._fqkline_req(code, count=_PAGE, end=end)
        return E._norm_tencent_bars(j, code) or []
    except Exception:
        return []


def fetch_deep(code, max_pages=12, pause=0.05):
    """一路往前翻页，拼出该指数**尽可能早**的完整日线（去重按日期）。

    每页 2000 根；翻到「本页不够 2000 根」（到头了）或「最早日期不再前移」为止。
    实测：sh000922 → 2005-01-04 起；sz399006 → 2010-06-01 起。

    返回 [{date,open,close,high,low,volume}]，失败返回 []。
    """
    code = (code or '').lower()
    if not code:
        return []
    seen = {}
    end = ''
    prev_earliest = None
    for _ in range(max(1, int(max_pages))):
        bars = _page(code, end)
        if not bars:
            break
        for b in bars:
            seen[b['date']] = b
        earliest = bars[0]['date']
        if len(bars) < int(_PAGE):
            break                      # 这页没取满 = 已经到上市日
        if prev_earliest is not None and earliest >= prev_earliest:
            break                      # 不再前移，防死循环
        prev_earliest = earliest
        try:
            d = datetime.strptime(earliest, '%Y-%m-%d') - timedelta(days=1)
        except ValueError:
            break
        end = d.strftime('%Y-%m-%d')
        if end <= _FLOOR:
            break
        time.sleep(pause)
    rows = [seen[k] for k in sorted(seen)]
    return rows


def _ensure_overseas(code, local=None):
    """v2.409: 海外指数全量落盘。只在「拿到的比本地更全」时才覆盖（与 ensure 同一护栏）。"""
    from quant_terminal import overseas_index as _ov
    try:
        rows = _ov.full_bars(code)
    except Exception:
        rows = []
    if not rows:
        return len(local or load(code))
    # v2.410 (用户 m11576): 海外市场的收盘时间与 A 股不同 —— 必须用该市场自己的
    #   收盘时刻判定「这一天的 bar 是否已定稿」。用 A 股的北京 15:06 会把美股
    #   22:31 刚开盘的半截日K写进 CSV, 而且增量只追加更大日期 ⇒ 永久错。
    rows = _ov.drop_unsettled(rows, code)
    if not rows:
        return len(local or load(code))
    cur = local if local is not None else load(code)
    if cur and len(rows) <= len(cur) and rows[-1]['date'] <= cur[-1]['date']:
        return len(cur)
    save(code, rows)
    return len(rows)


def _sync_overseas(code, local=None):
    """v2.409: 海外指数增量补齐。返回 'appended:N' / 'noop' / 'fail'。"""
    from quant_terminal import overseas_index as _ov
    loc = local if local is not None else load(code)
    try:
        tail = _ov.tail_bars(code, 10)
    except Exception:
        tail = []
    if not tail:
        return 'fail'
    # v2.410: 同上 —— 按该市场自己的收盘时刻过滤未定稿的 bar
    tail = _ov.drop_unsettled(tail, code)
    if not tail:
        return 'noop'
    if not loc:
        return 'missing-rebuilt' if _ensure_overseas(code, loc) else 'fail'
    last = loc[-1]['date']
    add = [b for b in tail if b['date'] > last]
    if not add:
        return 'noop'
    seen = {}
    for b in loc + add:
        seen[b['date']] = b
    save(code, [seen[k] for k in sorted(seen)])
    return 'appended:%d' % len(add)


def ensure(code, min_rows=0, deep=True):
    """本地缺失/行数不足 → 全量（深）回补落盘。返回最终行数。

    min_rows>0 时，只有「本地行数 < min_rows」才回补；=0 表示「没有文件才回补」。
    deep=False 就只取最近 2000 根（应急，不给特色指标用）。
    """
    code = (code or '').lower()
    if not code:
        return 0
    with _LK:
        local = load(code)
        if local and (min_rows <= 0 or len(local) >= min_rows):
            return len(local)
        # v2.367: ETF 不能走腾讯翻页（腾讯对 ETF 单次只给 640 根且字段要另解），
        #   交给 engine.ensure_index_daily —— 它内部按 idx_registry.src_of() 分派，
        #   em 分支走 etf_src.full_bars（东财，自带 beg=20200102）。
        if _is_em(code):
            try:
                E.ensure_index_daily(code)
            except Exception:
                pass
            return len(load(code))
        # v2.409: 海外指数 —— 源本身没有翻页能力，一次拉全量。
        if _is_overseas(code):
            return _ensure_overseas(code, local)
        rows = fetch_deep(code) if deep else _page(code, '')
        if not rows:
            return len(local)
        rows = E._drop_forming_today(rows)     # 盘中不落当日 forming bar
        if not rows:
            return len(local)
        # 深度回补只在「拿到的比本地更全」时才覆盖，避免网络抽风把老数据截短
        if local and len(rows) <= len(local) and rows[-1]['date'] <= local[-1]['date']:
            return len(local)
        save(code, rows)
        return len(rows)


def sync(code):
    """增量补齐：只拉最近 10 根，比本地末行新的追加进去。

    返回 'appended:N' / 'noop' / 'fail' / 'missing-rebuilt'。
    """
    code = (code or '').lower()
    if not code:
        return 'fail'
    with _LK:
        local = load(code)
        if not local:
            return 'missing-rebuilt' if ensure(code) else 'fail'
        # v2.367: ETF 增量走 engine.sync_index_daily（同样是按 src 分派到东财）
        if _is_em(code):
            try:
                return E.sync_index_daily(code, force=True)
            except Exception:
                return 'fail'
        # v2.409: 海外指数增量 —— 拉最近 10 根比对末行。
        if _is_overseas(code):
            return _sync_overseas(code, local)
        tail = _page(code, '')[-10:]
        if not tail:
            return 'fail'
        tail = E._drop_forming_today(tail)
        last = local[-1]['date']
        add = [b for b in tail if b['date'] > last]
        if not add:
            return 'noop'
        merged = local + add
        seen = {}
        for b in merged:
            seen[b['date']] = b
        save(code, [seen[k] for k in sorted(seen)])
        return 'appended:%d' % len(add)


# ── 组合视图（特色指标用）─────────────────────────────────────────────────
def series(code, start='', end=''):
    """取 [{date, close}]，可选日期区间（闭区间）。"""
    out = []
    for r in load(code):
        d = r['date']
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append({'date': d, 'close': r['close']})
    return out


def pair_view(a, b, limit=0):
    """两组收盘价 + 比值（**从短板开始**，用户 m38119 明确要求）。

    a = 分子（绿线），b = 分母（红线）；ratio = a.close / b.close（金线）。
    因为两边交易日集合理论上相同，但停牌/缺行时有差异 ⇒ 按**两个日期集合的交集**对齐，
    这样比值线的每个点都对应真实存在的两个收盘价。
    limit>0 时只保留最后 limit 个点。
    """
    a = (a or '').lower()
    b = (b or '').lower()
    ra = {r['date']: r['close'] for r in load(a)}
    rb = {r['date']: r['close'] for r in load(b)}
    dates = sorted(set(ra) & set(rb))       # 交集 ⇒ 自动「取短板」
    if limit and len(dates) > limit:
        dates = dates[-limit:]
    sa, sb, rt = [], [], []
    for d in dates:
        ca, cb = ra[d], rb[d]
        sa.append(ca)
        sb.append(cb)
        rt.append(round(ca / cb, 6) if cb else None)
    return {
        'ok': True,
        'a': {'code': a, 'name': name_of(a), 'dates': dates, 'close': sa},
        'b': {'code': b, 'name': name_of(b), 'dates': dates, 'close': sb},
        'ratio': {'name': (short_of(a) + '/' + short_of(b)), 'values': rt},
        'n': len(dates),
        'start': dates[0] if dates else '',
        'end': dates[-1] if dates else '',
    }


def pairs_meta():
    """给前端左列选项用：三组比值 + 各自「短板起点」与点数（不联网）。"""
    out = []
    for a, b, label in PAIRS:
        la = load(a)
        lb = load(b)
        sa = la[0]['date'] if la else ''
        sb = lb[0]['date'] if lb else ''
        start = max(sa, sb) if (sa and sb) else (sa or sb)
        out.append({
            'a': a, 'b': b, 'label': label,
            'a_name': name_of(a), 'b_name': name_of(b),
            'a_start': sa, 'b_start': sb, 'start': start,
            'a_n': len(la), 'b_n': len(lb),
            'ready': bool(la and lb),
        })
    return out


def list_view(limit=260):
    """「指数一览」：登记表里 LIST_CODES 的最近 limit 根收盘 + 元信息。"""
    out = []
    for c in LIST_CODES:
        rows = load(c)
        tail = rows[-limit:] if (limit and len(rows) > limit) else rows
        dates = [r['date'] for r in tail]
        closes = [r['close'] for r in tail]
        pct = None
        if len(closes) >= 2 and closes[-2]:
            pct = round((closes[-1] - closes[-2]) / closes[-2] * 100.0, 2)
        # v2.364 (用户 m39650): 左列要显示趋势判定词 —— 直接复用 engine._regime_from_rows(),
        #   与顶栏 classify_index_regime() / 悬停浮窗 index_hist_recent() 是**同一份实现**
        #   (v2.70 定的规矩: 判定式只许有一处)。行数 <30 时它返回 None ⇒ 该行没有趋势词,
        #   前端必须容忍为空, 不能画成 undefined。
        _rg = {}
        try:
            _rg = E._regime_from_rows(rows) or {}
        except Exception:
            _rg = {}
        _ss = {}
        if _is_overseas(c):
            # v2.410 (用户 m11576): 海外市场的交易时段与 A 股完全不同, 左列要能
            #   显示「这只现在开盘没有」。纯时间计算, 一次网络请求都不发。
            try:
                from quant_terminal import overseas_index as _ovs
                _ss = _ovs.session(c)
            except Exception:
                _ss = {}
        out.append({
            'code': c, 'name': name_of(c), 'short': short_of(c), 'sess': _ss,
            'n': len(rows),
            'start': rows[0]['date'] if rows else '',
            'end': rows[-1]['date'] if rows else '',
            'last': closes[-1] if closes else None,
            'pct': pct,
            'regime': _rg.get('regime') or '',
            'icon': _rg.get('icon') or '',
            'regime_adx': _rg.get('adx'),
            'dates': dates, 'closes': closes,
        })
    return out


def deepen(code, min_start='2015-01-01', pause=0.05):
    """把本地历史往前补到「上市日」——只要本地起点比 min_start 还晚就翻页。

    为什么单列一个函数：`ensure()` 的判据是「行数够不够」，而 2049 行（2018-06 起）
    已经满足 min_rows，但它对特色指标不够用（创业板指要 2010-06 起）。
    这里按**起点日期**判断，才真正把历史补深。

    返回最终行数。
    """
    code = (code or '').lower()
    if not code:
        return 0
    with _LK:
        local = load(code)
        if local and local[0]['date'] <= (min_start or ''):
            return len(local)          # 已经够早，不联网
        # v2.367: ETF 历史按用户要求**截断在 2020-01-02**，不再往前翻页。
        if _is_em(code):
            if not local:
                try:
                    E.ensure_index_daily(code)
                except Exception:
                    pass
            return len(load(code))
        # v2.409: 海外指数没有翻页能力，一次全量即可（源多深就多深）。
        if _is_overseas(code):
            return _ensure_overseas(code, local)
        rows = fetch_deep(code, pause=pause)
        if not rows:
            return len(local)
        rows = E._drop_forming_today(rows)
        if not rows:
            return len(local)
        if local and len(rows) <= len(local):
            return len(local)          # 没拿到更多就别覆盖
        save(code, rows)
        return len(rows)


def prewarm(log=None):
    """开机预热：把特色指标要的代码补深 + 一览的 7 个建好库。

    顺序有讲究：先 `deepen` 特色指标那 4 个（含中证红利，它要翻 3 页），
    再 `ensure` 其余一览代码（多半已存在，纯秒回）。
    """
    done = []
    for c in FEATURE_CODES:
        try:
            k = deepen(c)
            done.append('%s:%d' % (c, k))
        except Exception as e:
            if log:
                log('idx_store 补深 %s 失败: %s' % (c, e))
    for c in LIST_CODES:
        if c in FEATURE_CODES:
            continue                   # 上面已经处理
        try:
            k = ensure(c, min_rows=250)
            done.append('%s:%d' % (c, k))
        except Exception as e:
            if log:
                log('idx_store 预热 %s 失败: %s' % (c, e))
    return done


def selftest():
    print('=== idx_store selftest ===')
    for c in FEATURE_CODES + LIST_CODES:
        m = meta(c)
        print('  %-10s %-8s %5d 行  %s ~ %s' %
              (m['code'], m['name'], m['n'], m['start'] or '-', m['end'] or '-'))
    print('--- pairs ---')
    for p in pairs_meta():
        print('  %-22s %s 起  %d/%d' % (p['label'], p['start'] or '-', p['a_n'], p['b_n']))


if __name__ == '__main__':
    selftest()
