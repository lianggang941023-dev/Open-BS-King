# -*- coding: utf-8 -*-
"""大盘云图（云图 / heatmap）—— 全市场板块 + 个股的方块树图数据层。

v2.353 (2026-11-20 用户 m36569):
  「在市场总览和连板梯队中间加一个"大盘云图"，具体怎么做你参考 a-share-heatmap，
    窗口大小参考最大化的"多股同列"窗口一样的大小，因为大盘云图信息量比较密集
    所以需要窗口界面尽量大」

设计（与 board_rank / board_rotation 都不同的一层，别混）:
  board_rank      = 板块**排行榜**（一行一个板块，文本列表）
  board_rotation  = 板块的 N 日涨幅**排名矩阵**
  yuntu（本模块） = 把板块/个股摊成**面积成比例的方块**，一眼看全市场热度分布

两种口径（结构不同，不是同一棵树的两种画法）:
  行业 industry = 一级 90 -> 二级 257 -> 个股 5585（三层，个股不重复计算）
  概念 concept  = 概念 351 个板块（**只有一层**）

  ★ 概念为什么不做个股层: 实测 5588 只票平均属于 **9.5** 个概念（归属条目 53,279 条），
    把个股摊进概念方块里等于同一只票画 9 遍、全市场市值被重复计 9 倍，
    面积完全没有意义（实测 JSON 4.9 MB、ECharts 要画 5.3 万个矩形，必卡死）。
    概念要看成分股就走现成的 /api/board_members，点方块下钻。

  行业为什么不重复: `im` 是 {代码: 二级行业名} 一对一，归属条目正好 5,586 = 个股数。

层级压平（重要）: **27 个一级行业底下只有 1 个二级、且名字与一级完全相同**
（实测 `单二级的一级行业: 同名 27, 异名 0`）⇒ 必须压平，否则界面上会出现
「银行」里套一个同名「银行」的怪框、还白占一层。

面积权重三种（前端切换，后端把原始数一次给全，不来回请求）:
  mv  = 流通市值（节点带 `cap`，单位**亿元**）
  amt = 成交额（节点带 `amt`，单位**元**）
  eq  = 等权（前端每只给 1）
  用市值线性算时 21% 的票面积 < 40px²（放不下字）⇒ 前端另给「平方根压缩」开关。

数据来源全部复用 board_rank 的现成缓存, 本模块**不自己联网、不自己读 CSV**:
  board_rank._facts()          全市场事实表（1800 秒缓存, 冷启约 6 秒）
  board_rank._maps()           (im, cm, pm) 行业/概念/上级行业映射
"""
import json
import threading
import time

from quant_terminal import board_rank as BR

_TTL = 60.0                 # 跟着 board_rank._realtime_quotes() 的 60 秒走
_LOCK = threading.Lock()
# ★ 必须按 kind 分开存 —— 行业 283KB + 概念 44KB 各一份；
#   单槽缓存会让「切一次口径」就整份重算（实测行业那趟要 6 秒）。
_CACHE = {}                 # kind -> {'ts': float, 'key': str, 'd': dict}

KINDS = ('industry', 'concept')
KIND_TX = {'industry': '行业云图', 'concept': '概念云图'}


def _f(v, n=2):
    """安全取数 + 四舍五入; None/异常/非有限一律给 0.0"""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return 0.0
    if x != x or x in (float('inf'), float('-inf')):
        return 0.0
    return round(x, n)


def _sack(name, codes, fx, with_stock=True):
    """把一组代码收成一个板块节点; 成员全空就返回 None。

    节点字段:
      n    名称
      pct  等权平均涨幅（成分股涨幅的算术平均, 与 board_rank 的 avg_pct 同口径）
      cap  流通市值合计（亿元）
      amt  成交额合计（元）
      k    成分股只数
      up/dn 涨/跌家数
      st   成分股 [[code,name,pct,cap亿,amt], ...]（with_stock=False 时不给）
      lead 涨幅最高的那只 [名字, pct]（概念只有一层时给前端做 tooltip）
    """
    rows, ps, cap, amt = [], [], 0.0, 0.0
    for c in codes:
        f = fx.get(c)
        if not f:
            continue
        cap += (f.get('cap_f') or 0.0)
        amt += (f.get('amt') or 0.0)
        if f.get('pct') is not None:
            ps.append(f['pct'])
        if with_stock:
            rows.append([c, f.get('name') or '', _f(f.get('pct')),
                         _f((f.get('cap_f') or 0.0) / 1e8, 1), int(f.get('amt') or 0.0)])
        else:
            rows.append((f.get('pct') or 0.0, f.get('name') or ''))
    if not rows and not ps:
        return None
    node = {
        'n': name,
        'pct': _f(sum(ps) / len(ps)) if ps else 0.0,
        'cap': _f(cap / 1e8, 1),
        'amt': int(amt),
        'k': len(rows),
        'up': sum(1 for v in ps if v > 0),
        'dn': sum(1 for v in ps if v < 0),
    }
    if with_stock:
        node['st'] = rows
    else:
        best = max(rows, key=lambda r: r[0]) if rows else None
        if best:
            node['lead'] = [best[1], _f(best[0])]
    return node


def _bucket():
    """缓存时间片（60 秒一格）—— _facts() 有自己的 1800 秒 + 目录 mtime key，
    _realtime_quotes() 有自己的 60 秒 TTL；这里只做「一屏内不重复构建」。"""
    return '%d' % (time.time() // _TTL)


def build(kind='industry', force=False):
    """构建云图数据。

    返回 {'ok','kind','lv','ts','date','qdate','src','n_stock','n_attr','n_lv1','n_lv2','secs','g'}

    行业: lv=3；g = [一级节点]，一级节点多 'flat'（=只有一个同名二级）与 'sb'（二级节点列表）
    概念: lv=1；g = [概念节点]，每个都 flat=True，没有 sb
    """
    if kind not in KINDS:
        kind = 'industry'
    key = '%s|%s' % (kind, _bucket())
    with _LOCK:
        c = _CACHE.get(kind)
        if (not force) and c and c['key'] == key and time.time() - c['ts'] < _TTL:
            return c['d']

    t0 = time.time()
    fx = BR._facts()
    im, cm, pm = BR._maps()

    if kind == 'concept':
        groups = {}
        for c, v in cm.items():
            for n in (v or '').split('|'):
                n = n.strip()
                if n:
                    groups.setdefault(n, []).append(c)
        out = []
        for name, codes in groups.items():
            node = _sack(name, codes, fx, with_stock=False)
            if node:
                node['flat'] = True          # 概念只有一层 ⇒ 前端直接把它当叶子画
                out.append(node)
        lv = 1
    else:
        lv2 = {}                             # 二级名 -> [codes]
        for c, v in im.items():
            n = (v or '').strip()
            if n:
                lv2.setdefault(n, []).append(c)
        lv1 = {}                             # 一级名 -> [二级名]
        for n2 in lv2:
            lv1.setdefault(pm.get(n2) or n2, []).append(n2)

        out = []
        for n1, subs in lv1.items():
            flat = (len(subs) == 1 and subs[0] == n1)
            sbs = []
            for n2 in sorted(subs):
                node = _sack(n2, lv2[n2], fx, with_stock=True)
                if node:
                    sbs.append(node)
            if not sbs:
                continue
            k = sum(s['k'] for s in sbs)
            # 一级涨幅按**成分股加权**（k 加权）比按二级等权准：大行业不被小行业稀释
            wsum = sum(s['pct'] * s['k'] for s in sbs)
            out.append({
                'n': n1, 'pct': _f(wsum / max(1, k)),
                'cap': _f(sum(s['cap'] for s in sbs), 1),
                'amt': sum(s['amt'] for s in sbs),
                'k': k,
                'up': sum(s['up'] for s in sbs),
                'dn': sum(s['dn'] for s in sbs),
                'flat': flat, 'sb': sbs,
            })
        lv = 3

    out.sort(key=lambda g: -g['cap'])
    d = {
        'ok': True,
        'kind': kind,
        'lv': lv,
        'ts': time.strftime('%m-%d %H:%M:%S'),
        'date': '',
        'qdate': '',
        'src': 'csv',
        # 行业：每个二级只归一个一级 ⇒ 求和就是真实只数
        # 概念：一只票平均属于 9.5 个概念 ⇒ 求和是「归属条目数」，真实只数另给
        'n_stock': (len(cm) if kind == 'concept' else sum(g['k'] for g in out)),
        'n_attr': sum(g['k'] for g in out),
        'n_lv1': len(out),
        'n_lv2': sum(len(g.get('sb') or []) for g in out),
        'secs': round(time.time() - t0, 2),
        'g': out,
    }
    try:
        one = next(iter(fx.values()))
        d['date'] = one.get('date') or ''
    except StopIteration:
        pass
    try:
        rq = BR._realtime_quotes()
        if rq:
            d['src'] = 'realtime'
            d['qdate'] = next(iter(rq.values())).get('qdate') or ''
    except Exception:
        d['src'] = 'csv'

    with _LOCK:
        _CACHE[kind] = {'ts': time.time(), 'key': key, 'd': d}
    return d


def invalidate(kind=None):
    """清缓存 —— kind 给 None 就两个口径一起清。"""
    with _LOCK:
        if kind is None:
            _CACHE.clear()
        else:
            _CACHE.pop(kind, None)


def summary(d=None, kind='industry'):
    """轻量摘要 —— 预热/自检/接口调试用，不把 280KB 的数据打出来。"""
    d = d or build(kind)
    body = json.dumps(d, ensure_ascii=False, separators=(',', ':'))
    return {
        'ok': True, 'kind': d['kind'], 'lv': d['lv'],
        'n_lv1': d['n_lv1'], 'n_lv2': d['n_lv2'],
        'n_stock': d['n_stock'], 'n_attr': d['n_attr'],
        'date': d['date'], 'qdate': d['qdate'], 'src': d['src'],
        'top': [{'n': g['n'], 'pct': g['pct'], 'cap': g['cap'], 'k': g['k']} for g in d['g'][:3]],
        'bytes': len(body.encode('utf-8')),
    }


def selftest():
    print('=' * 72)
    print('大盘云图 数据层自测')
    print('=' * 72)
    for kind in KINDS:
        t0 = time.time()
        d = build(kind, force=True)
        dt = time.time() - t0
        s = summary(d, kind)
        print()
        print('[%s] %.2fs  层=%d 一级=%d 二级=%d 个股=%d 归属=%d  日期=%s 快照=%s 源=%s'
              % (kind, dt, s['lv'], s['n_lv1'], s['n_lv2'], s['n_stock'], s['n_attr'],
                 d['date'], d['qdate'], d['src']))
        print('     JSON %.1f KB' % (s['bytes'] / 1024))
        for t in s['top']:
            print('     %-16s %+6.2f%%  %8.1f 亿  %4d 只' % (t['n'], t['pct'], t['cap'], t['k']))
        g = d['g'][0]
        if g.get('sb'):
            print('     最大块 %s: %d 只 / %+.2f%% / flat=%s / 二级 %d'
                  % (g['n'], g['k'], g['pct'], g['flat'], len(g['sb'])))
            s0 = g['sb'][0]
            print('        %s: %d 只 / %+.2f%% / 首只 %s'
                  % (s0['n'], s0['k'], s0['pct'], s0['st'][0]))
        else:
            print('     最大块 %s: %d 只 / %+.2f%% / lead=%s'
                  % (g['n'], g['k'], g['pct'], g.get('lead')))
    print()
    t = time.time()
    build('industry')
    print('行业缓存命中 %.3fs' % (time.time() - t))
    t = time.time()
    build('concept')
    print('概念缓存命中 %.3fs' % (time.time() - t))


if __name__ == '__main__':
    selftest()
