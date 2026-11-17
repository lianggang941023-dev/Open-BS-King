# -*- coding: utf-8 -*-
"""
board_monitor.py —— 「监控中心」数据层 (v2.285, 2026-11-12 用户 m11544)

用户原话 (截图红字, 等于功能规格):
  「监控中心可用于监控股票价格，做两个按钮，一个是"监控结果"，另一个是"正在监控"，
    正在监控里面可以添加监控，可以输入股票代码或简称找到具体某个股票然后设定一个价格，
    后续只要触及过那个价格就把结果展示在"监控结果"里面，盘中刷新价格的时候触发了就
    点亮"市场总览"按钮进入红光呼吸闪烁并且设计一个警报音效，被监控的股票在盘中每次
    刷新行情的时候都必定同步拉取这些股票的实时行情」

设计 (为什么这么做):
  ① **触发后是否结案, 由每条自己的 repeat 决定** (v2.286, 用户 m12101「把反复提醒的开关加上」):
     repeat=False (默认) ⇒ 一次性: 进「监控结果」并从「正在监控」移除;
     repeat=True        ⇒ 反复提醒: 留在「正在监控」, 每次触及都进结果并响铃,
                          但同一根 K 线内只报一次 (用 last_hit_bar 去重, 换 bar 才再报),
                          否则 30 秒一轮会不停在响。
  ② **方向由建仓价推断**: 目标价 > 建仓价 ⇒ 等它涨上来 (向上触及);
     目标价 < 建仓价 ⇒ 等它跌下来 (向下触及); 相等 ⇒ 任一侧碰到即算。
     这样用户不用选方向, 直觉上就是"它到那个价就行"。
  ③ **状态落在 monitor_state.json** (与 simulator.py 同款原子写: 先写 .tmp 再 os.replace,
     并留 .bak 备份上一版), 重启 8799 不丢。
  ④ **故意不写死 15:00 / 9:30 判断**: 只要拿得到实时价就检查。非交易时段快照价格
     就是收盘价, 不会产生假触发 (除非目标价正好等于收盘价, 那种情况本来就该报)。
  ⑤ 触发的"未读"用 `unseen` 计数, 前端据此点亮「市场总览」按钮红光呼吸 + 响警报;
     用户打开窗口看过后调 `mark_seen()` 清掉。

数据:
  watches : [{id, code, name, target, base, dir, note, created_at}]
  results : [{id, code, name, target, base, dir, hit_price, hit_pct, hit_at, note}]
"""
import os
import sys
import json
import time
import threading
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
for _p in (ROOT, HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

STATE_FILE = os.path.join(HERE, 'monitor_state.json')

_LOCK = threading.RLock()
_ST = {'watches': [], 'results': [], 'unseen': 0, 'alarm_at': 0.0}
_LOADED = False
MAX_RESULTS = 300        # 结果最多留这么多条, 超出裁旧的
MAX_WATCHES = 200


# ------------------------------------------------------------------ 持久化
def _load():
    global _LOADED
    if _LOADED:
        return
    with _LOCK:
        if _LOADED:
            return
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    d = json.load(f) or {}
                _ST['watches'] = list(d.get('watches') or [])
                _ST['results'] = list(d.get('results') or [])
                _ST['unseen'] = int(d.get('unseen') or 0)
                _ST['alarm_at'] = float(d.get('alarm_at') or 0.0)
        except Exception:
            pass
        _LOADED = True


def _save():
    with _LOCK:
        d = {
            'watches': _ST['watches'],
            'results': _ST['results'],
            'unseen': _ST['unseen'],
            'alarm_at': _ST['alarm_at'],
            'saved_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        try:
            tmp = STATE_FILE + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(d, f, ensure_ascii=False, indent=1)
            try:
                if os.path.exists(STATE_FILE):
                    import shutil
                    shutil.copy2(STATE_FILE, STATE_FILE + '.bak')
            except Exception:
                pass
            os.replace(tmp, STATE_FILE)
        except Exception:
            pass


# ------------------------------------------------------------------ 股票搜索
_NAME_MAP = {'ts': 0.0, 'd': {}}


def _name_map():
    """{6位码: 简称} —— sector_kline.tickers() 文件缓存, 0.00 秒"""
    now = time.time()
    if _NAME_MAP['d'] and now - _NAME_MAP['ts'] < 600:
        return _NAME_MAP['d']
    d = {}
    try:
        from quant_terminal import sector_kline as _sk
        d = dict(_sk.tickers() or {})
    except Exception:
        d = {}
    _NAME_MAP['d'] = d
    _NAME_MAP['ts'] = now
    return d


def _bare(code):
    c = str(code or '').strip().upper()
    if len(c) > 2 and c[:2] in ('SH', 'SZ', 'BJ', 'BZ'):
        c = c[2:]
    return c


def search(kw='', limit=10):
    """按代码前缀 / 简称包含 找股票 —— 监控中心「输入代码或简称找到具体某个股票」"""
    k = (kw or '').strip()
    if not k:
        return []
    nm = _name_map()
    up = k.upper()
    hits = []
    # 1) 纯 6 位码 / 带前缀码, 直接命中
    b = _bare(up)
    if b.isdigit() and len(b) == 6:
        if b in nm:
            hits.append({'code': b, 'name': nm[b]})
        else:
            hits.append({'code': b, 'name': b})
    # 2) 代码前缀
    if up.isdigit():
        for c, n in nm.items():
            if c.startswith(up) and c != b:
                hits.append({'code': c, 'name': n})
                if len(hits) >= limit:
                    break
    # 3) 简称包含
    if len(hits) < limit:
        for c, n in nm.items():
            if n and k in n:
                if not any(h['code'] == c for h in hits):
                    hits.append({'code': c, 'name': n})
                    if len(hits) >= limit:
                        break
    return hits[:limit]


def quote_one(code):
    """单只实时价 —— 用于添加监控时取"建仓价"基"""
    c = _bare(code)
    if not c:
        return None
    q = _quotes([c])
    v = q.get(c) or {}
    p = v.get('price')
    return float(p) if p else None


def _quotes(codes):
    """批量取实时价 {6位码: {price, pct, name}} —— 复用 board_rank 的前缀规则 + 缓存"""
    codes = [c for c in dict.fromkeys(_bare(x) for x in (codes or [])) if c]
    if not codes:
        return {}
    # 全市场快照已有缓存 (board_rank, 60 秒) -> 直接被监控的票一定在其中
    try:
        from quant_terminal import board_rank as _brd
        allq = _brd._realtime_quotes() or {}
    except Exception:
        allq = {}
    out = {}
    miss = []
    for c in codes:
        v = allq.get(c)
        if v and v.get('price'):
            out[c] = v
        else:
            miss.append(c)
    # 快照里没有的 (新股/退市整理) 单独补一批
    if miss:
        try:
            from quant_terminal import datasource as ds

            def _sc(c):
                if c[:3] == '920' or c[:1] in ('4', '8'):
                    return 'bj' + c
                return ('sh' + c) if c[:1] in '69' else ('sz' + c)

            r = ds.fetch_realtime([_sc(c) for c in miss]) or {}
            for k, v in r.items():
                if not isinstance(v, dict):
                    continue
                kk = str(k)
                if len(kk) > 2:
                    kk = kk[2:]
                if v.get('price'):
                    out[kk] = {'price': v.get('price'), 'pct': v.get('change_pct'),
                               'name': v.get('name') or ''}
        except Exception:
            pass
    return out


# ------------------------------------------------------------------ 增删查
def _find(wid):
    for w in _ST['watches']:
        if w.get('id') == wid:
            return w
    return None


def add(code, target, note='', repeat=False):
    """加一条监控。返回 {ok, watch|error}"""
    _load()
    c = _bare(code)
    if not c or not c.isdigit() or len(c) != 6:
        return {'ok': False, 'error': '\u4ee3\u7801\u4e0d\u5408\u6cd5\uff08\u9700 6 \u4f4d\uff09'}
    try:
        tg = round(float(target), 3)
    except Exception:
        return {'ok': False, 'error': '\u76ee\u6807\u4ef7\u4e0d\u5408\u6cd5'}
    if tg <= 0:
        return {'ok': False, 'error': '\u76ee\u6807\u4ef7\u5fc5\u987b\u5927\u4e8e 0'}
    with _LOCK:
        if len(_ST['watches']) >= MAX_WATCHES:
            return {'ok': False, 'error': '\u76d1\u63a7\u5217\u8868\u5df2\u6ee1\uff08\u4e0a\u9650 %d\uff09' % MAX_WATCHES}
        if any(w.get('code') == c and abs(float(w.get('target') or 0) - tg) < 1e-6
               for w in _ST['watches']):
            return {'ok': False, 'error': '\u5df2\u6709\u4e00\u6761\u76f8\u540c\u4ee3\u7801+\u76ee\u6807\u4ef7\u7684\u76d1\u63a7'}
        nm = _name_map().get(c) or c
        q = _quotes([c]).get(c) or {}
        base = float(q.get('price') or 0.0)
        if tg > base:
            d = 'up'
        elif tg < base:
            d = 'down'
        else:
            d = 'touch'
        w = {
            'id': uuid.uuid4().hex[:12],
            'code': c,
            'name': nm,
            'target': tg,
            'base': round(base, 3),
            'dir': d,
            'note': (note or '').strip()[:60],
            'repeat': bool(repeat),
            'last_hit_bar': '',
            'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        _ST['watches'].append(w)
        _save()
        return {'ok': True, 'watch': w}


def remove(wid):
    _load()
    with _LOCK:
        n = len(_ST['watches'])
        _ST['watches'] = [w for w in _ST['watches'] if w.get('id') != wid]
        changed = len(_ST['watches']) != n
        if changed:
            _save()
        return {'ok': changed}


def clear_results():
    _load()
    with _LOCK:
        _ST['results'] = []
        _ST['unseen'] = 0
        _ST['alarm_at'] = 0.0
        _save()
    return {'ok': True}


def mark_seen():
    _load()
    with _LOCK:
        if _ST['unseen'] or _ST['alarm_at']:
            _ST['unseen'] = 0
            _ST['alarm_at'] = 0.0
            _save()
    return {'ok': True}


# ------------------------------------------------------------------ 触发检查
def check():
    """拉一次被监控票的实时价并判触发。前端每轮刷新调一次。

    返回 {ok, watches, results, unseen, alarm_at, n_watch, n_result, new:[触发的]}
    """
    _load()
    with _LOCK:
        wl = list(_ST['watches'])
    new = []
    if wl:
        q = _quotes([w['code'] for w in wl])
        with _LOCK:
            for w in wl:
                if _find(w['id']) is None:
                    continue
                v = q.get(w['code']) or {}
                px = float(v.get('price') or 0.0)
                if px <= 0:
                    continue
                tg = float(w.get('target') or 0.0)
                d = w.get('dir') or 'touch'
                hit = False
                if d == 'up':
                    hit = px >= tg
                elif d == 'down':
                    hit = px <= tg
                else:
                    hit = abs(px - tg) < max(0.005, tg * 0.0005)
                if not hit:
                    continue
                # v2.286: 反复提醒的票, 同一根 bar 只报一次 (否则 30 秒一轮, 每次刷新都响)
                if w.get('repeat'):
                    _b0 = str(v.get('qdate') or time.strftime('%Y-%m-%d %H:%M'))
                    if w.get('last_hit_bar') and w.get('last_hit_bar') == _b0:
                        continue
                pct = None
                if w.get('base'):
                    try:
                        pct = round((px - float(w['base'])) / float(w['base']) * 100.0, 2)
                    except Exception:
                        pct = None
                rec = {
                    'id': w['id'],
                    'code': w['code'],
                    'name': w.get('name') or w['code'],
                    'target': tg,
                    'base': w.get('base'),
                    'dir': d,
                    'hit_price': round(px, 3),
                    'hit_pct': pct if pct is not None else v.get('pct'),
                    'note': w.get('note') or '',
                    'hit_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                }
                _ST['results'].insert(0, rec)
                _ST['unseen'] = int(_ST.get('unseen') or 0) + 1
                _ST['alarm_at'] = time.time()
                new.append(rec)
                if w.get('repeat'):
                    # v2.286 反复提醒: 不结案, 但同一 bar(日期+时段) 内只报一次
                    _bar = str(v.get('qdate') or time.strftime('%Y-%m-%d %H:%M'))
                    for _x in _ST['watches']:
                        if _x.get('id') == w.get('id'):
                            _x['last_hit_bar'] = _bar
                            _x['last_hit_price'] = round(px, 3)
                            _x['last_hit_at'] = rec['hit_at']
                            break
                else:
                    _ST['watches'] = [x for x in _ST['watches'] if x.get('id') != w['id']]
            if len(_ST['results']) > MAX_RESULTS:
                _ST['results'] = _ST['results'][:MAX_RESULTS]
            if new:
                _save()
    with _LOCK:
        return {
            'ok': True,
            'watches': _ST['watches'],
            'results': _ST['results'],
            'unseen': _ST['unseen'],
            'alarm_at': _ST['alarm_at'],
            'n_watch': len(_ST['watches']),
            'n_result': len(_ST['results']),
            'new': new,
        }


def state():
    _load()
    with _LOCK:
        return {
            'ok': True,
            'watches': _ST['watches'],
            'results': _ST['results'],
            'unseen': _ST['unseen'],
            'alarm_at': _ST['alarm_at'],
            'n_watch': len(_ST['watches']),
            'n_result': len(_ST['results']),
        }


if __name__ == '__main__':
    import pprint
    print('\u6e05\u7406\u6d4b\u8bd5\u72b6\u6001: %s' % clear_results())
    time.sleep(0.05)
    print('\u641c\u7d22 "6002":', search('6002')[:4])
    print('\u641c\u7d22 "\u5b87\u7279":', search('\u5b87\u7279')[:4])
    r = add('600206', 99.0, 'test-up')
    print('\u52a0\u76d1\u63a7(\u5411\u4e0a):', r.get('ok'), r.get('watch') or r.get('error'))
    r2 = add('002973', 1.0, 'test-down')
    print('\u52a0\u76d1\u63a7(\u5411\u4e0b):', r2.get('ok'), (r2.get('watch') or {}).get('dir'))
    s = check()
    print('\u68c0\u67e5: n_watch=%s n_result=%s new=%s' % (s['n_watch'], s['n_result'], len(s['new'])))
    pprint.pprint(state()['watches'])
    for w in state()['watches']:
        remove(w['id'])
    print('\u6e05\u7406\u540e:', state()['n_watch'], state()['n_result'])
    clear_results()
