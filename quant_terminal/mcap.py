# -*- coding: utf-8 -*-
"""v2.131 (2026-10-01): 市值两列读写层 —— 总市值/流通市值 (单位: 亿元)

背景 (用户定案 2026-10-01):
  同花顺快照新增「总市值/流通市值」两列, 写进本地K线 CSV 第7/8列;
  之后所有展示 (悬停浮窗) 与排序 (点「简称」= 按流通市值) 都从本地文件取, 不再额外联网。

写入口只有三条 (用户定案):
  ① 快照导入 (📥 → import_ths_snapshot.py 跑完) → sync_from_snapshots() 全市场强制刷最新值
  ② 个股详情页拉到腾讯 qt 市值 → write_daily_once() 每票每天只写一次 (日期没变就不写)
  ③ 「⚡ 快照更新」→ server 追加行时直接写两列; 漏写的历史行用 sync_from_qt() 回填
     (用户 2026-10-08 定案: 更新器要写 + 已写入的那批行要补)

⚠ 口径 (与 临时脚本/_fill_mcap.py 完全一致, 不要改):
  - 单位统一「亿元」数值 (1.57万亿 → 15700) —— 排序必须按数值比大小, 不能用 '1.57万亿' 字符串
  - 值**只写在最新日期那一行**, 老行两列一律留空 (v2.131a 用户定案 2026-10-01):
    市值没有历史, 不要伪装成历史; 文件也几乎不涨 (整列同值会 +85MB)。
  - 行 = 9 列矩形 (老行后三列空着; v2.273 前是 8 列) —— 结构对 pandas/csv 读取最安全, 不做长短不一的行
  - 快照缺数据 (新股 '--' / 停牌股不在快照里) → 保持原值不动, **绝不清空**
  - 老格式快照 (无这两列) → 跳过该文件, 已写值不动
  - 行尾: daily/*.csv 全部 LF 无BOM → 只按 '\n' 拆, 绝不引入 CRLF
"""
import json
import os
import re
import threading
import time
from datetime import datetime

try:                                # v2.234: 腾讯 qt 是境内源, 优先直连 (见 net.py)
    from . import net as _net
except Exception:
    import net as _net

# ---- 路径 ----
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # D:\AI Working\BS King
_STATE_FILE = os.path.join(BASE_DIR, 'kline_data', '.mcap_written.json')  # {code: 'YYYY-MM-DD'}

# ---- 进程内缓存 (供 /api/mcap 一次性下发全市场) ----
_CACHE = {'at': 0.0, 'map': {}}
# v2.403: 自由流通股本表 (万股) —— 与 _CACHE['map'] 同一次扫描产出, 供窗口版K线算实际换手率
_CACHE_FREE = {'at': 0.0, 'map': {}}
_LOCK = threading.Lock()
_WRITE_STATE = None

CAP_T = '总市值'
CAP_F = '流通市值'
# v2.273 (2026-11-11 用户 m08653): 第 9 列「自由流通股本」(单位: 万股, 与通达信 FreeLtgb 同单位)
#   用户原话:「新增一列自由流通股本，先写进去之后我再考虑这个数据用在什么地方」
#   ⇒ 只落盘、不参与任何计算; 老行留空保持矩形, 与市值两列同一套规矩。
FREE = '自由流通股本'


def daily_dir():
    """K线目录 (走 datasource 配置; 与 server/engine 同一口径)"""
    try:
        from quant_terminal import datasource as ds
        return str(ds.resolve(ds.get('local', 'kline_dir', os.path.join('kline_data', 'daily'))))
    except Exception:
        return os.path.join(BASE_DIR, 'kline_data', 'daily')


def bare(code):
    """'SH600206' / 'sh600206' / '600206' → '600206' (全市场唯一, 前端按裸码查)"""
    c = (code or '').strip().upper()
    return c[2:] if (len(c) > 2 and c[:2] in ('SH', 'SZ', 'BZ', 'BJ') and c[2:].isdigit()) else c


def prefixed(code):
    """裸码 → 本地K线文件名前缀 (SH/SZ/BZ)。920xxx=北交所(不能按 9 开头误判成沪B)"""
    c = bare(code)
    if c[:2] in ('SH', 'SZ', 'BZ'):
        return c
    if c.startswith('920') or c.startswith(('4', '8')):
        return 'BZ' + c
    if c.startswith(('6', '9')):
        return 'SH' + c
    return 'SZ' + c


def path_of(code):
    return os.path.join(daily_dir(), prefixed(code) + '.csv')


def _num(s):
    s = (s or '').strip()
    if not s or s == '--':
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _scan_lines(lines, i_t, i_f):
    """从后往前找「最近一行有值」的市值 —— 返回 (总, 流) 或 None"""
    for l in reversed(lines):
        if not l.strip():
            continue
        r = l.split(',')
        t = _num(r[i_t]) if i_t < len(r) else None
        f = _num(r[i_f]) if i_f < len(r) else None
        if t is not None or f is not None:
            return (t, f)
    return None


def read_three(path, tail_bytes=16384):
    """读一个K线文件的三列 → (总市值, 流通市值, 自由流通股本); 无该列/无值 → 对应位 None

    v2.273: 第三列「自由流通股本」是新增的 (老文件没有) ⇒ 缺列时回落 (t, f, None),
            调用方不用关心文件是新是旧。
    """
    try:
        with open(path, 'rb') as f:
            hdr = (f.read(320).decode('utf-8', 'replace').replace('\r', '')
                   .split('\n', 1)[0].split(','))
            if CAP_T not in hdr or CAP_F not in hdr:
                return (None, None, None)
            i_t = len(hdr) - 1 - hdr[::-1].index(CAP_T)
            i_f = len(hdr) - 1 - hdr[::-1].index(CAP_F)
            i_e = len(hdr) - 1 - hdr[::-1].index(FREE) if FREE in hdr else None
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - tail_bytes))
            tail = f.read().decode('utf-8', 'replace')
    except OSError:
        return (None, None, None)
    lines = tail.split('\n')
    if size > tail_bytes:
        lines = lines[1:]
    for l in reversed(lines):
        if not l.strip():
            continue
        r = l.split(',')
        t = _num(r[i_t]) if i_t < len(r) else None
        fc = _num(r[i_f]) if i_f < len(r) else None
        if t is not None or fc is not None:
            e = _num(r[i_e]) if (i_e is not None and i_e < len(r)) else None
            return (t, fc, e)
    if size > tail_bytes:
        try:
            with open(path, 'rb') as f:
                for l in reversed(f.read().decode('utf-8', 'replace').split('\n')):
                    if not l.strip():
                        continue
                    r = l.split(',')
                    t = _num(r[i_t]) if i_t < len(r) else None
                    fc = _num(r[i_f]) if i_f < len(r) else None
                    if t is not None or fc is not None:
                        e = _num(r[i_e]) if (i_e is not None and i_e < len(r)) else None
                        return (t, fc, e)
        except OSError:
            pass
    return (None, None, None)


def get_free(code):
    """单票自由流通股本 (万股); 可传裸码或带前缀"""
    return read_three(path_of(code))[2]


def read_two(path, tail_bytes=16384):
    """读一个K线文件的两列 → (总市值, 流通市值); 无列/无值 → (None, None)

    v2.131a: 值只在最新那一行 → 从尾部往前找最近一行有值的。
      ⚠️ 为什么必须往前找: 「⚡ 快照更新」会往后追加一行只有 OHLCV 的新行,
         市值值就留在上一行了 —— 往前找正好兜住, 不会因为追加了一天就变"暂无数据"。
      只读文件头 256B + 尾部 16KB (约 200 行) —— 全市场 5,576 文件 < 1 秒。
    """
    try:
        with open(path, 'rb') as f:
            # v2.134 (2026-10-14): 先剔掉 CR —— K线 CSV 是纯 LF 约定, 字段里不该有 \r。
            #   之前少了这一步: 遇上 CRLF 文件时末字段读成 '流通市值\r' ⇒ 下面判据失配
            #   ⇒ 整个市场市值读成 None。
            hdr = (f.read(256).decode('utf-8', 'replace').replace('\r', '')
                   .split('\n', 1)[0].split(','))
            if CAP_T not in hdr or CAP_F not in hdr:
                return (None, None)
            # v2.134: 同名列出现多次时取**最后一次** —— 被 CRLF 带偏过的老文件里, mcap 把值
            #   追加在末尾那两列 (8/9); 只认第一次出现 (6/7) 会读到空格 → 判成"无市值"。
            i_t = len(hdr) - 1 - hdr[::-1].index(CAP_T)
            i_f = len(hdr) - 1 - hdr[::-1].index(CAP_F)
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - tail_bytes))
            tail = f.read().decode('utf-8', 'replace')
    except OSError:
        return (None, None)
    lines = tail.split('\n')
    # 中间起读时首行是半截的 (可能被截断成半行) → 丢掉
    if size > tail_bytes:
        lines = lines[1:]
    hit = _scan_lines(lines, i_t, i_f)
    if hit:
        return hit
    if size > tail_bytes:            # 尾部 200 行里都没有 → 整个文件再找一次 (极少)
        try:
            with open(path, 'rb') as f:
                return _scan_lines(f.read().decode('utf-8', 'replace').split('\n'), i_t, i_f) or (None, None)
        except OSError:
            return (None, None)
    return (None, None)


def get(code):
    """单票市值 (裸码或带前缀都行)"""
    return read_two(path_of(code))


def load_map(force=False, ttl=300):
    """全市场 {裸码: (总市值, 流通市值)} —— 带 TTL 缓存 (默认5分钟)"""
    now = time.time()
    with _LOCK:
        if not force and _CACHE['map'] and (now - _CACHE['at']) < ttl:
            return _CACHE['map']
    d = daily_dir()
    out = {}
    try:
        names = os.listdir(d)
    except OSError:
        names = []
    for fn in names:
        if not fn.lower().endswith('.csv'):
            continue
        t, f = read_two(os.path.join(d, fn))
        if t is None and f is None:
            continue
        out[bare(fn[:-4])] = (t, f)
    with _LOCK:
        _CACHE['map'] = out
        _CACHE['at'] = now
    return out


def invalidate():
    """写完盘调一次 —— 下次取值重扫 (不许留旧值)"""
    with _LOCK:
        _CACHE['at'] = 0.0


# ---- 写 ----
def fmt(v):
    """市值数值 → CSV 文本 (与 _render 完全同款: 两位小数去尾零; None → 空)"""
    if v is None:
        return ''
    return ('%.2f' % v).rstrip('0').rstrip('.')


def _render(text, t, f, e=None):
    """把(两/三)列改成「只有最新一行有值, 老行留空」; 表头没有这几列就追加

    行保持矩形 (老行空着) —— 不做长短不一的行, pandas/csv 读起来最安全。
    v2.273: 多了第三列 FREE(自由流通股本). e 为 None 且表头也没有 FREE 时 → 不碰第三列
            (老调用方 write_one(code, t, f) 的行为一字不变).
    返回: (新文本, 表头原本是否已有这两列)
    """
    # v2.134 (2026-10-14): 进来先剔掉所有 CR —— 本函数只按 '\n' 切行, 行尾残留的 \r 会黏在
    #   最后一个字段上, 让 CAP_T/CAP_F 判据失配 → 把两列重复追加一遍 (10-14 事故形态)。
    #   剔掉之后: 表头缩回 9 列, 行也缩回 9 列 —— 相当于自带一层自愈。
    text = text.replace('\r', '')
    end_nl = text.endswith('\n')
    body = text[:-1] if end_nl else text
    lines = [l for l in body.split('\n') if l.strip()]
    hdr = lines[0].split(',')
    # v2.134: 收掉重复的市值列 —— CRLF 事故的遗留形态是「表头 10 列 (两列各出现两次) + 行里那个
    #   \r 夹在第 8/9 字段中间」, 值是写在**最后**那两列的。只剔 CR 不收列的话, 表头是 10 列、
    #   数据被裁成 9 列 → 又是一种新畸形。这里同名只保留最后一次出现, 行列同步收窄。
    for _cap in ((CAP_T, CAP_F, FREE) if FREE in hdr else (CAP_T, CAP_F)):
        _idx = [i for i, c in enumerate(hdr) if c == _cap]
        if len(_idx) > 1:
            _keep = [i for i in range(len(hdr)) if i not in set(_idx[:-1])]

            def _pick(row, _k=_keep, _n=len(hdr)):
                cols = row.split(',')
                if len(cols) < _n:
                    cols = cols + [''] * (_n - len(cols))
                return ','.join(cols[i] for i in _k)

            hdr = [hdr[i] for i in _keep]
            lines = [lines[0]] + [_pick(l) for l in lines[1:]]
    has = CAP_T in hdr and CAP_F in hdr
    # v2.273: 第三列 —— 表头里已经有, 或者本次带了值(e 不为 None) 才存在
    col_e = (FREE in hdr) or (e is not None)
    set_e = (e is not None)          # 🔴 只有带了值才写; 没带就整列**原样不动**(绝不清空已有数据)
    if has:
        i_t, i_f = hdr.index(CAP_T), hdr.index(CAP_F)
    else:
        i_t, i_f = len(hdr), len(hdr) + 1
        hdr = hdr + [CAP_T, CAP_F]
    if col_e:
        if FREE in hdr:
            i_e = hdr.index(FREE)
        else:
            i_e = len(hdr)
            hdr = hdr + [FREE]
    n = max([i_t, i_f] + ([i_e] if col_e else []))
    ts = fmt(t)
    fs = fmt(f)
    es = fmt(e)
    out = [','.join(hdr)]
    rows = lines[1:]
    for k, l in enumerate(rows):
        cols = l.split(',')
        cols = (cols + [''] * (n + 1 - len(cols))) if len(cols) <= n else cols[:n + 1]
        if k == len(rows) - 1:          # 最新那一行 = 唯一带值的行
            cols[i_t], cols[i_f] = ts, fs
            if set_e:
                cols[i_e] = es
        else:
            cols[i_t], cols[i_f] = '', ''
            if set_e:
                cols[i_e] = ''
        out.append(','.join(cols))
    return ('\n'.join(out) + ('\n' if end_nl else '')), has


def write_one(code, total, fcap, force=True, free=None):
    """把某票 K线 的两/三列整列刷成 (total, fcap, free)

    total/fcap 均为 None → 不写 (快照没数据时保持原值, 绝不清空)
    force=False → 已是该值就跳过 (详情页/幂等); force=True → 一律重写 (快照导入强制全量)
    v2.273: free = 自由流通股本(万股). None 且表头也没有该列 → 不碰第三列 (老行为不变)
    返回 (是否写入, 说明)
    """
    if total is None and fcap is None:
        return (False, 'no-value')
    p = path_of(code)
    if not os.path.exists(p):
        return (False, 'no-kline')
    try:
        with open(p, 'rb') as fh:
            text = fh.read().decode('utf-8')
    except (OSError, UnicodeDecodeError) as e:
        return (False, 'read-error:%s' % e)
    _cur = read_three(p)
    if not force and (free is None) and _cur[0] == total and _cur[1] == fcap:
        return (False, 'same')       # 没带自由流通股本 → 只比前两列, 第三列原样留着
    if not force and _cur == (total, fcap, free):
        return (False, 'same')
    new, _ = _render(text, total, fcap, free)
    if new == text:
        return (False, 'same')
    tmp = p + '.tmp'
    try:
        with open(tmp, 'wb') as fh:
            fh.write(new.encode('utf-8'))
        os.replace(tmp, p)
    except OSError as e:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return (False, 'write-error:%s' % e)
    return (True, 'ok')


def _load_state():
    global _WRITE_STATE
    if _WRITE_STATE is None:
        try:
            with open(_STATE_FILE, 'r', encoding='utf-8') as f:
                _WRITE_STATE = json.load(f) or {}
        except Exception:
            _WRITE_STATE = {}
    return _WRITE_STATE


def _save_state():
    tmp = _STATE_FILE + '.tmp'
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(_WRITE_STATE, f)
        os.replace(tmp, _STATE_FILE)
    except Exception:
        pass


def write_daily_once(code, total, fcap, free=None):
    """详情页入口 (用户定案): 每票**每天只写一次** —— 日期没变就不写

    市值无精度要求 (盘中再变也不多), 所以一天一次足够;
    想强制刷新走快照导入 (sync_from_snapshots force=True)。
    v2.273: free = 自由流通股本(万股), 不传就只写两列。
    """
    if total is None and fcap is None:
        return False
    c = bare(code)
    st = _load_state()
    today = datetime.now().strftime('%Y-%m-%d')
    if st.get(c) == today:
        return False
    wrote, why = write_one(c, total, fcap, force=True, free=free)
    if why != 'no-kline':      # 没有K线文件的不记 —— 以后有了K线还能补写
        st[c] = today
        _save_state()
    if wrote:
        invalidate()
    return wrote


# ---- 快照导入 (同花顺 xlsx, 实为 GBK 制表符文本) ----
def _load_text(path):
    raw = open(path, 'rb').read()
    for e in ('gbk', 'gb18030', 'utf-8-sig', 'utf-8'):
        try:
            return raw.decode(e)
        except Exception:
            pass
    return raw.decode('gbk', 'replace')


def parse_cap(v):
    """'1.57万亿' → 15700.0 · '2245亿' → 2245.0 · '--' → None"""
    v = (v or '').strip().replace(',', '')
    if not v or v == '--':
        return None
    mult = 1.0
    if v.endswith('万亿'):
        v, mult = v[:-2], 10000.0
    elif v.endswith('亿'):
        v = v[:-1]
    elif v.endswith('万'):
        v, mult = v[:-1], 0.0001
    try:
        return round(float(v) * mult, 2)
    except ValueError:
        return None


def snapshot_files():
    """配置里的快照文件绝对路径 (data_sources.json → local.snapshot_files)"""
    try:
        from quant_terminal import datasource as ds
        files = ds.get('local', 'snapshot_files', []) or []
        return [str(ds.resolve(f)) for f in files]
    except Exception:
        return []


def load_snapshot_caps():
    """读全部快照 → {带前缀码: (总市值, 流通市值)}; 老格式(无市值列)的文件整体跳过"""
    caps = {}
    used, skipped = [], []
    for p in snapshot_files():
        if not os.path.exists(p):
            skipped.append(os.path.basename(p) + '(不存在)')
            continue
        try:
            lines = [l for l in _load_text(p).replace('\r', '\n').split('\n') if l.strip()]
        except Exception as e:
            skipped.append(os.path.basename(p) + '(%s)' % str(e)[:30])
            continue
        if not lines:
            skipped.append(os.path.basename(p) + '(空)')
            continue
        hdr = [h.strip() for h in lines[0].split('\t')]
        if CAP_T not in hdr or CAP_F not in hdr:
            skipped.append(os.path.basename(p) + '(无市值列)')
            continue
        i_t, i_f = hdr.index(CAP_T), hdr.index(CAP_F)
        for l in lines[1:]:
            r = l.split('\t')
            c = r[0].strip().upper()
            if not re.match(r'^(SH|SZ|BZ|BJ)\d{6}$', c):
                continue
            caps[prefixed(c)] = (parse_cap(r[i_t] if i_t < len(r) else ''),
                                 parse_cap(r[i_f] if i_f < len(r) else ''))
        used.append(os.path.basename(p))
    return caps, used, skipped


def write_all(caps, force=True):
    """全市场写回 (快照导入后调) —— caps: {带前缀码: (总,f)}"""
    d = daily_dir()
    stat = {'updated': 0, 'same': 0, 'no_data': 0, 'error': 0}
    try:
        names = sorted(f for f in os.listdir(d) if f.lower().endswith('.csv'))
    except OSError:
        return stat
    for fn in names:
        code = fn[:-4]
        t, f = caps.get(prefixed(code), (None, None))
        if t is None and f is None:
            stat['no_data'] += 1
            continue
        wrote, why = write_one(code, t, f, force=force)
        if wrote:
            stat['updated'] += 1
        elif why in ('same', 'no-value'):
            stat['same'] += 1
        else:
            stat['error'] += 1
    if stat['updated']:
        invalidate()
    return stat


def sync_from_snapshots(force=True, log=print):
    """快照导入跑完后调: 全市场两列强制刷成快照里的最新值 (缺数据的票原值不动)"""
    t0 = time.time()
    caps, used, skipped = load_snapshot_caps()
    if not caps:
        log('[市值] ⚠ 快照里没有可解析的市值数据, 跳过 (本地原值不动) — 跳过: %s' % ('、'.join(skipped) or '无'))
        return {'ok': False, 'msg': '快照无市值列'}
    stat = write_all(caps, force=force)
    msg = ('[市值] 快照 %d 个文件 / %d 只 → 写入 %d · 已是最新 %d · 快照无数据(原值不动) %d · 失败 %d (%.1fs)'
           % (len(used), len(caps), stat['updated'], stat['same'], stat['no_data'], stat['error'], time.time() - t0))
    log(msg)
    return dict(stat, snap=len(caps), files=len(used), skipped=skipped, msg=msg)


def load_pair(force=False, ttl=300):
    """一次扫描同时拿两表 → ({裸码:(总,流)}, {裸码: 自由流通股本(万股)})

    v2.403: 原来 /api/mcap 只给市值; 窗口版 K 线要算「实际换手率」= 成交量(手)/自由流通股本(万股),
            再单独扫一遍目录要翻倍 IO, 所以合并成一次 read_three 同时填两张缓存。
    """
    now = time.time()
    with _LOCK:
        if (not force and _CACHE['map'] and _CACHE_FREE['map']
                and (now - _CACHE['at']) < ttl
                and (now - _CACHE_FREE['at']) < ttl):
            return _CACHE['map'], _CACHE_FREE['map']
    d = daily_dir()
    cap, fre = {}, {}
    try:
        names = os.listdir(d)
    except OSError:
        names = []
    for fn in names:
        if not fn.lower().endswith('.csv'):
            continue
        t, f, e = read_three(os.path.join(d, fn))
        if t is not None or f is not None:
            cap[bare(fn[:-4])] = (t, f)
        if e is not None:
            fre[bare(fn[:-4])] = e
    with _LOCK:
        _CACHE['map'] = cap
        _CACHE['at'] = now
        _CACHE_FREE['map'] = fre
        _CACHE_FREE['at'] = now
    return cap, fre


def payload():
    """/api/mcap 响应体: {裸码: [总市值, 流通市值]} (亿元) + free {裸码: 自由流通股本(万股)}

    v2.403: 多给一张 free 表 — 前端窗口版 K 线「换手(实际换手率)」用它。
    """
    m, fr = load_pair()
    return {'mcap': {k: [v[0], v[1]] for k, v in m.items()},
            'free': fr, 'count': len(m)}


# ---- 腾讯 qt 实时市值 (v2.132 2026-10-08) ----
# 背景 (用户定案 2026-10-08): 「⚡ 快照更新」只往 CSV 追加 OHLCV 六列, 市值两列没写 →
#   当天所有票的市值列空着 (悬停/排序只能读到上一交易日的旧值)。
#   修法: ① server._run_kline_update_snapshot 追加行时把两列一起写 (写入端)
#         ② 已写入的那批行用 sync_from_qt() 回填 (读端补写)
QT_URL = 'http://qt.gtimg.cn/q='
QT_UA = 'Mozilla/5.0'
# qt 字段下标 (2026-10-08 实测 sh600519: f[44]=15698.40 f[45]=15698.40 f[30]=20261008213944)
QT_I_F = 44          # 流通市值 (亿元)
QT_I_T = 45          # 总市值 (亿元)
QT_I_TIME = 30       # 行情时间戳 YYYYMMDDHHMMSS


def qt_symbol(pcode):
    """带前缀码 → 腾讯 qt symbol (小写; 北交所 BZ920002 → bj920002)"""
    c = str(pcode).upper()
    return ('bj' + c[2:]) if c[:2] in ('BZ', 'BJ') else c.lower()


def qt_date_of(ts):
    """qt 时间戳 '20261008213944' → '2026-10-08'"""
    ts = str(ts or '')
    if len(ts) >= 8 and ts[:8].isdigit():
        return '%s-%s-%s' % (ts[:4], ts[4:6], ts[6:8])
    return ''


def fetch_qt_caps(pcodes, batch=50, timeout=12, log=None):
    """批量拉腾讯 qt 快照 → {带前缀码: (总市值, 流通市值, 日期)} (亿元; 无值 → None)"""
    import urllib.request
    out = {}
    lst = list(pcodes)
    for i in range(0, len(lst), batch):
        chunk = lst[i:i + batch]
        url = QT_URL + ','.join(qt_symbol(c) for c in chunk)
        try:
            req = urllib.request.Request(url, headers={'User-Agent': QT_UA})
            txt = _net.open_url(req, timeout=timeout).read().decode('gbk', 'ignore')
        except Exception as e:
            if log:
                log('[市值] qt 批次 %d/%d 失败: %s' % (i // batch + 1, (len(lst) + batch - 1) // batch, str(e)[:60]))
            continue
        for line in txt.split(';'):
            line = line.strip()
            if not line.startswith('v_') or '="' not in line:
                continue
            key = line[2:line.index('=')].lower()
            f = line.split('="')[1].rstrip('"').split('~')
            if len(f) <= QT_I_T:
                continue
            pcode = ('BZ' + key[2:]) if key.startswith('bj') else key.upper()
            t = _num(f[QT_I_T]) or None
            fc = _num(f[QT_I_F]) or None
            if t is None and fc is None:
                continue
            out[pcode] = (t, fc, qt_date_of(f[QT_I_TIME]) if len(f) > QT_I_TIME else '')
    return out


def sync_from_qt(only_date=None, batch=50, log=print, limit=0, dry=False):
    """用腾讯 qt 实时市值回填K线**末行**两列 (只补末行日期 == 快照日期的文件)

    用户定案 2026-10-08: 「⚡ 快照更新」追加的那批行没有市值两列 → 本函数一次性补齐。
    only_date='YYYY-MM-DD' 指定要补的日期; None → 自动取 qt 快照日期 (全市场同一个)。
    limit=N 只处理前 N 个文件 (自检用); dry=True 只统计不写盘。
    返回 {'ok','date','snap','updated','same','skip_date','no_data','error','msg'}
    """
    import urllib.request  # noqa: F401  (保持与 fetch_qt_caps 同一依赖)
    d = daily_dir()
    try:
        names = sorted(f for f in os.listdir(d) if f.lower().endswith('.csv'))
    except OSError:
        return {'ok': False, 'msg': 'daily 目录不存在: %s' % d}
    if limit:
        names = names[:limit]
    t0 = time.time()
    caps = fetch_qt_caps([f[:-4] for f in names], batch=batch, log=log)
    if not caps:
        return {'ok': False, 'msg': 'qt 快照全失败 (网络?)'}
    if not only_date:
        ds_ = [v[2] for v in caps.values() if v[2]]
        only_date = max(ds_) if ds_ else ''
    stat = {'ok': True, 'date': only_date, 'snap': len(caps), 'updated': 0,
            'same': 0, 'skip_date': 0, 'no_data': 0, 'error': 0, 'dry': bool(dry)}
    for fn in names:
        code = fn[:-4]
        v = caps.get(prefixed(code)) or caps.get(code)
        if not v or (v[0] is None and v[1] is None):
            stat['no_data'] += 1
            continue
        p = os.path.join(d, fn)
        try:
            sz = os.path.getsize(p)
            with open(p, 'rb') as fh:
                fh.seek(max(0, sz - 512))
                tail = fh.read().decode('utf-8', 'replace').strip()
        except OSError:
            stat['error'] += 1
            continue
        tl = [l for l in tail.splitlines() if l.strip()]
        if not tl:
            stat['no_data'] += 1
            continue
        lc = tl[-1].split(',')
        if only_date and lc[0] != only_date:
            stat['skip_date'] += 1
            continue
        if len(lc) >= 8 and _num(lc[6]) == v[0] and _num(lc[7]) == v[1]:
            stat['same'] += 1
            continue
        if dry:
            stat['updated'] += 1
            continue
        try:
            with open(p, 'rb') as fh:
                text = fh.read().decode('utf-8')
        except (OSError, UnicodeDecodeError):
            stat['error'] += 1
            continue
        new, _ = _render(text, v[0], v[1])
        if new == text:
            stat['same'] += 1
            continue
        tmp = p + '.tmp'
        try:
            with open(tmp, 'wb') as fh:
                fh.write(new.encode('utf-8'))
            os.replace(tmp, p)
        except OSError:
            stat['error'] += 1
            try:
                os.unlink(tmp)
            except OSError:
                pass
            continue
        stat['updated'] += 1
    if stat['updated'] and not dry:
        invalidate()
    stat['msg'] = ('[市值] qt %s: 快照 %d 只 / 补写 %d · 已是最新 %d · 日期不符跳过 %d · 无行情 %d · 失败 %d (%.1fs)'
                   % (only_date or '?', stat['snap'], stat['updated'], stat['same'],
                      stat['skip_date'], stat['no_data'], stat['error'], time.time() - t0))
    if log:
        log(stat['msg'])
    return stat
