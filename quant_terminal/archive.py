# -*- coding: utf-8 -*-
"""结果归档（v2.323 2026-11-15 用户 m22464）

两件事，互相独立：

一、**竞价打板全票全字段归档**  `save_scan(mode, pool, lines, rows, info)`
    每次扫描完成，把**列表里出现的每一只票**（不止主推/备选）连同当时那份
    `_summarize_result()` 的**全部字段**写成一个 JSON 文件，另写一份 `latest` 指针。

    为什么必须写全字段、不挑指标：
      用户的原话是「累积一定数量的记录后，你到时候帮我看一下什么指标最有用，
      能实现次日赚钱走不论多少」。现在挑着记，将来想补一个字段就再也补不回来了
      —— 竞价数据只有 09:25~09:30 那 5 分钟是真的，过了就永久丢失
      （见 auction_scan.py 的说明：候选池来自当天实时快照，历史不可重放）。
      所以这里**一个字段都不裁**，宁可用不上。

    行业 / 概念 / 板块档位：行里本来就带
      sector(行业) concept(概念) sector_status / concept_status(逐概念档位)
      ind_t / con_t(任意多头 = 偏多|强多头，空串=不达标) mh_ready
      —— 正是用户要的「每只票的行业所属和包含概念是否有任意多头」。

二、**各窗口的落盘存档**  `save_window(name, payload)` / `load_window(name)`
    市场总览 / 连板梯队 / 龙虎榜 / 行业板块 / 概念板块 —— 以及
    **行业趋势 / 概念趋势 / 上级行业趋势**（v2.324 用户 m22862 加的，按均线档位各一份）。
    窗口打开时先用存档秒显
    （`/api/snapshot?name=` 直接读盘，不重算），随后前端再拉新数据覆盖。
    用户原话：「没有缓存的时候点开一片空白等几秒看着很不爽。」

落盘目录：`<项目根>/data_archive/`
    scan/   竞价打板逐次全票归档
    window/ 五个窗口的最新一份
"""
import os
import json
import time
import hashlib
import tempfile
import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARC_DIR = os.path.join(ROOT, 'data_archive')
SCAN_DIR = os.path.join(ARC_DIR, 'scan')
WIN_DIR = os.path.join(ARC_DIR, 'window')

# 单次扫描归档里最多留多少行 —— 竞价打板一天最多几十只；
# 留 2000 是给「万一以后把别的模式也归档」兜底，不是判断逻辑。
SCAN_MAX_ROWS = 2000
# 同一个决策日**只留一份**归档（用户 m24803 原话：「每天只能有一份，最好就是上午
#   9点27分之后9点30分之前的数据最佳」）。同一天多份会让每日统计把同一天重复计入、
#   甚至互相矛盾。只留一份时靠 `_scan_quality()` 保证留下的是**质量最高**的那份
#   （真竞价 live > 盘后 9:30 还原 locked > 其它 replay）。
SCAN_KEEP_PER_DAY = 1

_LAST_HASH = {}      # name -> md5，用来跳过「内容没变」的重复写盘（板块榜 200KB 一份，别每 30 秒写一次）
_SAVE_TTL = {}       # name -> 上次写盘时间
_WIN_MIN_GAP = 20.0  # 秒；同一份窗口内容 20 秒内不重复写（内容变了照写）


# ──────────────────────────────────────────────────────────────── 基础
def _ensure(d):
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return os.path.isdir(d)


def _atomic_write(path, text):
    """先写同目录临时文件再 os.replace —— 进程被杀也不会留半截 JSON"""
    d = os.path.dirname(path)
    _ensure(d)
    fd, tmp = tempfile.mkstemp(dir=d, prefix='.tmp_', suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(text)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except Exception:
            pass
        raise


def _read_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def _now():
    return datetime.datetime.now()


def _stamp():
    return _now().strftime('%Y-%m-%d %H:%M:%S')


# ──────────────────────────────────────────────── 一、竞价打板全票归档
def _trade_of(info, fallback=''):
    """从 auction_info 里取「决策日」（形如 2026-11-13），归一成 20261113。

    为什么要它：文件名里必须能一眼看出**这是哪一天的选股结果**。
    文件名的存盘时间（今天 11-15）与决策日（11-13）经常不是同一天 ——
    复盘统计要按决策日分组，按存盘日分就全乱了。
    """
    d = ''
    try:
        d = str((info or {}).get('date') or '')
    except Exception:
        d = ''
    digits = ''.join(c for c in d if c.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return fallback or time.strftime('%Y%m%d')


def _scan_path(mode, trade8, ts=None, tag=''):
    """文件名：`<mode>_<决策日>_<存盘日-时分秒>_<池+档位>.json`

    例 `auction_20261113_20261115-051308_auto4.json`
    —— 用 `_` 只切三段，存盘时间内部用 `-`，所以 split('_') 稳定可解。
    """
    t = ts or time.time()
    stamp = time.strftime('%Y%m%d-%H%M%S', time.localtime(t))
    safe = lambda s: ''.join(c for c in str(s) if c.isalnum() or c in '-')[:20]
    return os.path.join(SCAN_DIR, '%s_%s_%s_%s.json'
                        % (safe(mode), safe(trade8), stamp, safe(tag) or 'x'))


def _scan_quality(info):
    """存档质量等级 —— 决定「同一天的几份里留哪份、谁不许覆盖谁」。

    v2.332 (2026-11-17 用户 m24803)：
      3 = live   真·集合竞价（9:25~9:30 抓的快照）—— **不可再生，最该保**
      2 = locked 盘后用 day/query 的 09:30 bar 还原出来的近似值
      1 = replay 其它回放
      0 = 未知（老档没写 mode，或 info 缺失）

    为什么非要分等级：盘后重跑同一只票，`sector_strength` / `turnover_t` /
    `bias_val` / `concept_status` 这些衍生列会按当时的行情重算，
    跟竞价当下抓的对不上（实测 31/31 只全不一样）。用它覆盖真竞价数据，
    以后统计出来的胜率基准就废了。
    """
    m = ''
    try:
        m = str((info or {}).get('mode') or '')
    except Exception:
        m = ''
    return {'live': 3, 'locked': 2, 'replay': 1}.get(m, 0)


def _same_day_scans(mode, trade8):
    """该 mode 在该决策日已有哪些正式归档 —— 返回 [(文件名, 质量)]，质量高的在前。

    同质量时按文件名倒序（文件名里带存盘时刻 ⇒ 新的在前），与 `_trim_scans` 一致。
    `<mode>_latest.json` 指针不算。
    """
    out = []
    try:
        if not os.path.isdir(SCAN_DIR):
            return out
        for fn in os.listdir(SCAN_DIR):
            if not fn.endswith('.json') or fn.endswith('_latest.json'):
                continue
            parts = fn[:-5].split('_')
            if len(parts) < 4 or parts[0] != str(mode) or parts[1] != str(trade8):
                continue
            d = _read_json(os.path.join(SCAN_DIR, fn)) or {}
            out.append((fn, _scan_quality(d.get('info'))))
    except Exception:
        return out
    out.sort(key=lambda x: (x[1], x[0]), reverse=True)
    return out


def save_scan(mode, pool, lines, rows, info=None, extra=None, force=False):
    """把一次扫描的**全部行、全部字段**写档。返回写成的路径，失败返回 ''。

    rows   : 已经过 _summarize_result() 的行（与前端列表、scan_cache.json 完全同构）
    info   : 该模式的结论块（auction_info，含 mode/date/index_pct/half/buy/main/alt/note）
    extra  : 任意附加字典（以后想记扫描参数就塞这里，不改签名）
    另写一份 `<mode>_latest.json`，方便「只想看最近一次」的读者直接读。
    """
    try:
        if not _ensure(SCAN_DIR):
            return ''
        rows = list(rows or [])[:SCAN_MAX_ROWS]
        cols = []
        seen = set()
        for r in rows:
            if not isinstance(r, dict):
                continue
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
        trade8 = _trade_of(info)
        # v2.332 (2026-11-17 用户 m24803): 每个决策日只留一份，且**不许降级覆盖**。
        #   同一天已经有「真竞价(live)」档了，再来一次盘后重算(locked)就**直接不写** ——
        #   盘后那份的板块强弱/换手/BIAS 全是按当时最新行情重算的近似值，覆盖上去
        #   等于把不可再生的原始数据丢了。反之，已有的是盘后档、来的是真竞价档，
        #   就照常写、写完把旧的删掉。
        q_new = _scan_quality(info)
        old_same_day = _same_day_scans(mode, trade8)
        if old_same_day and not force and old_same_day[0][1] > q_new:
            return ''
        doc = {
            'fmt': 1,
            'kind': 'scan',
            'mode': mode,
            'pool': pool,
            'lines': lines,
            'trade_date': trade8,     # 决策日（这段行情是哪天的）
            'saved_at': _stamp(),
            'saved_ts': time.time(),
            # 竞价结论：date 是决策日、index_pct 是上证竞价、half 是半仓、
            # main/alt/alt2 是当时规则给的主推/备选/备选*（空表示没有）
            'info': info if isinstance(info, dict) else None,
            'extra': extra if isinstance(extra, dict) else None,
            'count': len(rows),
            'columns': cols,      # 表头顺序（= 首次出现顺序），将来做统计时有据可依
            'rows': rows,
        }
        txt = json.dumps(doc, ensure_ascii=False, separators=(',', ':'))
        # v2.323c ①: 与「本模式最近一份」内容完全一致就不再写第二份
        #   （改完前端/别的模式后重扫同一批票，结果一模一样，存两份没有意义）
        h = hashlib.md5(json.dumps(rows, ensure_ascii=False, sort_keys=True,
                                   separators=(',', ':')).encode('utf-8', 'replace')).hexdigest()
        if _LAST_SCAN_HASH.get(mode) == h and not force:
            return ''
        p = _scan_path(mode, trade8, tag='%s%s' % (pool, lines))
        _atomic_write(p, txt)
        _atomic_write(os.path.join(SCAN_DIR, '%s_latest.json' % str(mode)), txt)
        _LAST_SCAN_HASH[mode] = h
        # v2.332: 写成功之后再删同一天的旧档 —— **绝不允许先删后写**（写失败就把唯一一份丢了）
        for _fn, _q in old_same_day:
            try:
                _p_old = os.path.join(SCAN_DIR, _fn)
                if os.path.abspath(_p_old) != os.path.abspath(p):
                    os.unlink(_p_old)
            except Exception:
                pass
        _trim_scans(mode)          # v2.332: 兜底 —— 每个决策日只留 1 份
        return p
    except Exception:
        return ''


_LAST_SCAN_HASH = {}   # mode -> md5(rows)，跳过「内容没变」的重复归档


def _trim_scans(mode, keep_per_day=None):
    """同一决策日只留 keep_per_day 份（v2.332 起默认 1 份），多的删掉。

    排序 = **质量优先、再按存盘时刻**（live > locked > replay > 未知）。
    ⇒ 万一某天既留下真竞价又留下盘后重算，被删的永远是盘后那份。
    只删「文件名能解析出决策日」的正式归档，`<mode>_latest.json` 指针不碰。
    返回删掉的个数。
    """
    keep = int(keep_per_day or SCAN_KEEP_PER_DAY)
    try:
        by_day = {}
        for fn in os.listdir(SCAN_DIR):
            if not fn.endswith('.json') or fn.endswith('_latest.json'):
                continue
            parts = fn[:-5].split('_')
            if len(parts) < 4 or parts[0] != str(mode):
                continue
            by_day.setdefault(parts[1], []).append(fn)
        n = 0
        for day, files in by_day.items():
            if len(files) <= keep:
                continue
            # v2.332: 排序键 = (质量等级, 文件名)。质量高的留，同质量留新的。
            #   绝对不能只按文件名排 —— 盘后重算的文件名字段时间更晚，
            #   一旦只按名字排，被删掉的会是那天真正的竞价数据。
            _q = {}
            for fn in files:
                _d = _read_json(os.path.join(SCAN_DIR, fn)) or {}
                _q[fn] = _scan_quality(_d.get('info'))
            files.sort(key=lambda f: (_q.get(f, 0), f), reverse=True)
            for fn in files[keep:]:
                try:
                    os.unlink(os.path.join(SCAN_DIR, fn))
                    n += 1
                except Exception:
                    pass
        return n
    except Exception:
        return 0


def list_scans(mode='', limit=100):
    """列出归档文件（新的在前）。mode 为空 = 全部模式。"""
    out = []
    try:
        if not os.path.isdir(SCAN_DIR):
            return out
        for fn in os.listdir(SCAN_DIR):
            if not fn.endswith('.json') or fn.endswith('_latest.json'):
                continue
            if mode and not fn.startswith(str(mode) + '_'):
                continue
            p = os.path.join(SCAN_DIR, fn)
            try:
                st = os.stat(p)
            except Exception:
                continue
            out.append({'file': fn, 'size': st.st_size, 'mtime': st.st_mtime,
                        'mtime_tx': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.st_mtime))})
        out.sort(key=lambda x: x['mtime'], reverse=True)
        return out[:max(1, int(limit))]
    except Exception:
        return out


def load_scan(name):
    """按文件名读一份归档（name 只允许归档目录里的裸文件名，绝不允许路径穿越）"""
    try:
        nm = os.path.basename(str(name or ''))
        if not nm.endswith('.json'):
            return None
        return _read_json(os.path.join(SCAN_DIR, nm))
    except Exception:
        return None


def scan_days(mode='auction'):
    """有哪些交易日有归档 —— 给「历史」列表用。

    按**决策日**（文件名第 2 段 / doc['trade_date']）分组，不是按存盘日。
    文件名 `auction_20261113_20261115-051308_auto4.json` ⇒ split('_') = [mode, 决策日, 存盘, tag]
    """
    days = {}
    for it in list_scans(mode=mode, limit=100000):
        parts = it['file'][:-5].split('_')
        day = parts[1] if len(parts) > 1 and len(parts[1]) == 8 and parts[1].isdigit() else ''
        if not day:
            continue
        d = days.setdefault(day, {'date': day, 'n': 0, 'last_mtime': 0, 'files': []})
        d['n'] += 1
        d['last_mtime'] = max(d['last_mtime'], it['mtime'])
        d['files'].append(it['file'])
    return sorted(days.values(), key=lambda x: x['date'], reverse=True)


# ──────────────────────────────────────────────── 二、各窗口落盘存档
#   v2.326: 除下面这些, 还有「市场环境 / 情绪周期」两页的 regime_* / phase_* 一组。
_WIN_NAMES = ('market_overview', 'ladder_up', 'ladder_down',
              # v2.347 (用户 m27833): 多股同列的行情快照 —— 前端每 10 秒覆盖式重写
              'mtx_watch',
              'lhb_all', 'lhb_org', 'lhb_hm',
              'board_industry_1', 'board_industry_2', 'board_concept_1', 'board_concept_2',
    # v2.353: 大盘云图两个口径
    'yuntu_industry', 'yuntu_concept',
    # v2.359 (用户 m38119): 特色指标 / 指数一览 两页的存档
    'idx_pair', 'idx_list', 'idx_head',
              # v2.324 (用户 m22862): 三条趋势线也各有存档（按均线档位分，3/4/5/6）
              'trend_sector_3', 'trend_sector_4', 'trend_sector_5', 'trend_sector_6',
              'trend_concept_3', 'trend_concept_4', 'trend_concept_5', 'trend_concept_6',
              'trend_parent_3', 'trend_parent_4', 'trend_parent_5', 'trend_parent_6',
              # v2.326 (用户 m23308): 市场环境 / 情绪周期 两页也要「点开秒显」。
              #   regime_* 存 /api/regime?days=(history) —— 情绪周期页的 history 读的也是它。
              #   phase_*  存情绪周期页另外三块（阶段概览 / 阶段段 / 主线排行）。
              'regime_250', 'regime_500', 'regime_1000',
              'phase_sum_250', 'phase_sum_500', 'phase_sum_1000',
              'phase_segs_250', 'phase_segs_500', 'phase_segs_1000',
              'phase_ml_250_concept', 'phase_ml_250_industry',
              'phase_ml_500_concept', 'phase_ml_500_industry',
              'phase_ml_1000_concept', 'phase_ml_1000_industry')


def _win_path(name):
    safe = ''.join(c for c in str(name) if c.isalnum() or c in '-_')[:48]
    return os.path.join(WIN_DIR, safe + '.json')


def save_window(name, payload, force=False):
    """把窗口接口的**原始返回**原样落盘（不做任何裁字段 —— 前端怎么用就怎么存）。

    永远是**同一个文件名原地覆盖**（`window/<name>.json`），绝不追加、绝不换名 ——
    所以不管刷新多少次，这个目录永远是固定那 8 个文件、总共约 0.4 MB。

    三条去重（v2.323b，用户 m22716 追问后修的）：
      ① 哈希**只算 payload**。第一版把 saved_at/saved_ts 一起算进哈希，
         而时间戳每次都不同 ⇒ 哈希永远不同 ⇒ 「内容没变」永远判不出来 ⇒ 每次调用都重写一遍。
      ② 内容没变 ⇒ 直接跳过（与时间无关）。
      ③ 内容变了 ⇒ `_WIN_MIN_GAP` 秒内也先不写，攒够再写；`force=True` 无视这两条。
    返回写成的路径；跳过或失败返回 ''。
    """
    try:
        if not _ensure(WIN_DIR):
            return ''
        # ① 只对 payload 做哈希：payload 不变 = 存档内容不变 = 没有任何重写的意义
        h = hashlib.md5(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                   separators=(',', ':')).encode('utf-8', 'replace')).hexdigest()
        last = _LAST_HASH.get(name)
        now = time.time()
        if not force:
            if last == h:
                return ''                      # ② 内容一模一样，一次都不写
            if (now - _SAVE_TTL.get(name, 0)) < _WIN_MIN_GAP:
                return ''                      # ③ 内容变了但还在间隔内，等下一轮
        txt = json.dumps({'fmt': 1, 'kind': 'window', 'name': name,
                          'saved_at': _stamp(), 'saved_ts': now,
                          'payload': payload},
                         ensure_ascii=False, separators=(',', ':'))
        p = _win_path(name)
        _atomic_write(p, txt)
        _LAST_HASH[name] = h
        _SAVE_TTL[name] = now
        return p
    except Exception:
        return ''


def load_window(name):
    """读窗口存档。**不重算、不联网**，只读盘 —— 必须快到前端可以「点开立刻画」。

    返回 {'ok':True,'name','saved_at','age_sec','payload'} 或 {'ok':False,'error'}
    """
    try:
        p = _win_path(name)
        d = _read_json(p)
        if not isinstance(d, dict) or 'payload' not in d:
            return {'ok': False, 'name': name, 'error': '还没有存档'}
        age = 0.0
        try:
            age = max(0.0, time.time() - float(d.get('saved_ts') or 0))
        except Exception:
            age = 0.0
        return {'ok': True, 'name': name, 'saved_at': d.get('saved_at') or '',
                'age_sec': round(age, 1), 'payload': d.get('payload')}
    except Exception as e:
        return {'ok': False, 'name': name, 'error': str(e)[:120]}


def window_info():
    """五个窗口存档的现状（界面/排查用）"""
    out = []
    for nm in _WIN_NAMES:
        p = _win_path(nm)
        try:
            st = os.stat(p)
            out.append({'name': nm, 'exists': True, 'size': st.st_size,
                        'mtime_tx': time.strftime('%Y-%m-%d %H:%M:%S',
                                                  time.localtime(st.st_mtime))})
        except Exception:
            out.append({'name': nm, 'exists': False, 'size': 0, 'mtime_tx': ''})
    return out


def selftest():
    r = {'dir': ARC_DIR, 'ok': _ensure(SCAN_DIR) and _ensure(WIN_DIR),
         'windows': window_info()}
    r['scans'] = list_scans(limit=5)
    r['days_auction'] = scan_days('auction')[:5]
    return r


if __name__ == '__main__':
    print(json.dumps(selftest(), ensure_ascii=False, indent=2))
