# -*- coding: utf-8 -*-
"""板块指数日线本地库（同花顺 fuyao）— v2.173 (2026-10-22)

为什么需要本地库
----------------
同花顺指数历史接口 `prices/historical` 一次只给**一只**、窗口 **≤5 年**，848 个
指数每天全量重拉不现实。所以照股票 K 线的做法：本地 CSV 存日线，每天只补新根。

为什么**不做**「用成分股逆向倒推指数历史」
------------------------------------------
1. 官方直接给 **5 年 = 1211 根**（实measured 881101/881121/884001 全是 1211 根，
   只有 2024–2026 才有的 886xxx 新题材少一些，如 886042 存储芯片 852 根），
   MA250 完全够用 —— 不存在"前面的怎么办"。
2. 成分股变化时同花顺**自己会重算指数历史**；我们定期对齐官方数据即可，
   若自己倒推反而会因为基期/换仓处理不同而累积误差，越算越偏。

口径（2026-10-22 实测确认）
--------------------------
板块指数 = 成分股涨跌幅的**等权算术平均**（8/8 板块复算精确吻合：养殖业 36 只
等权 1.98 = 官方 1.98，电池 107 只 −3.42 = 官方 −3.42 …），与终端 /api/sector_ma
同口径。所以「板块指数 MA」和「成分股等权合成 MA」是一回事。

CSV 列与股票日线保持一致（复用同一套读取逻辑）
----------------------------------------------
date,open,close,high,low,volume,总市值,流通市值
板块指数没有市值 ⇒ 后两列留空。

文件名：`881101.TI` → `TI881101.csv`
"""
import os
import io
import sys
import csv
import time
import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(HERE)                       # …\终端8799
KDIR = os.path.join(BASE_DIR, 'kline_data', 'sector_index')
IDX_DIR = os.path.join(BASE_DIR, 'kline_data', 'ths_index')
KLOG = os.path.join(KDIR, 'sector_kline.log')

SNAP_API = '/api/a-share-index/prices/snapshot'
HIST_API = '/api/a-share-index/prices/historical'

HEADER = ['date', 'open', 'close', 'high', 'low', 'volume', '总市值', '流通市值']
WORKERS = 6
RETRY = 3
RETRY_SLEEP = 1.5
MAX_YEARS = 5                       # 官方硬限制

# 终端统一均线口径（与终端其它地方完全一致：5-10-21-55，后面 120 / 250）
MA_PERIODS = (5, 10, 21, 55, 120, 250)


def _fq():
    try:
        from . import fuyao as _m
        return _m
    except Exception:
        import fuyao as _m
        return _m


def _log(level, msg):
    try:
        os.makedirs(KDIR, exist_ok=True)
        with io.open(KLOG, 'a', encoding='utf-8') as f:
            f.write('%s [%s] %s\n' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, msg))
        if os.path.getsize(KLOG) > 2 * 1024 * 1024:
            raw = io.open(KLOG, encoding='utf-8', errors='replace').read()
            with io.open(KLOG, 'w', encoding='utf-8') as f:
                f.write(raw[-1024 * 1024:])
    except Exception:
        pass


# ---------------------------------------------------------------- 基础
def kfile(thscode):
    """`881101.TI` → .../sector_index/TI881101.csv"""
    tc = str(thscode or '').strip().upper()
    if '.' in tc:
        base, suf = tc.split('.', 1)
    else:
        base, suf = tc, 'TI'
    return os.path.join(KDIR, ('%s%s.csv' % (suf, base)))


def names():
    """{thscode: {'name':..., 'tag':...}} —— 来自 fuyao_index 的 catalog.json"""
    p = os.path.join(IDX_DIR, 'catalog.json')
    out = {}
    try:
        d = json.load(io.open(p, encoding='utf-8'))
    except Exception:
        return out
    items = d.get('items') or {}
    for tag, arr in items.items():
        for it in (arr or []):
            tc = it.get('thscode')
            if tc:
                out[tc] = {'name': it.get('name') or '', 'tag': tag}
    return out


def all_codes(tags=('industry', 'cn_concept')):
    """要建库的指数清单（默认 行业 320 + 概念 390 = 710 个）"""
    nm = names()
    out = []
    for tc, v in nm.items():
        if not tags or v.get('tag') in tags:
            out.append(tc)
    out.sort()
    return out


def _ms(dt):
    return int(dt.timestamp() * 1000)


def _d(ms):
    try:
        return datetime.datetime.fromtimestamp(int(ms) / 1000.0).strftime('%Y-%m-%d')
    except Exception:
        return ''


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------- 抓取
def history(thscode, years=MAX_YEARS, end=None, timeout=30):
    """单只指数日线 → [{date, open, close, high, low, volume, turnover}]（按日期升序）

    官方限制：interval 只支持 1d；start/end 必填毫秒；窗口 ≤5 年。
    """
    years = max(1, min(int(years or MAX_YEARS), MAX_YEARS))
    end_dt = end or datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(days=365 * years + 30)
    # 再收一次，确保不越过 5 年硬限制
    earliest = end_dt - datetime.timedelta(days=365 * MAX_YEARS)
    if start_dt < earliest:
        start_dt = earliest
    p = {'thscode': thscode, 'interval': '1d',
         'start': _ms(start_dt), 'end': _ms(end_dt)}
    d = _fq()._req(HIST_API, p, timeout=timeout, api_name='idx/hist:%s' % thscode)
    out = []
    for it in (d.get('item') or []):
        dt = _d(it.get('date_ms'))
        if not dt:
            continue
        out.append({
            'date': dt,
            'open': _f(it.get('open_price')),
            'close': _f(it.get('close_price')),
            'high': _f(it.get('high_price')),
            'low': _f(it.get('low_price')),
            'volume': _f(it.get('volume')),
            'turnover': _f(it.get('turnover')),
        })
    out.sort(key=lambda x: x['date'])
    # 盘中闸（2026-10-23 踩过）：官方历史接口在**未收盘时**会把「正在走」的当天那根
    # 一起返回（收/量都是半天的）。若原样落盘，710 个板块会全部把半天当整天写死，
    # 且 append_today 的「本地末根 >= 交易日就跳过」让真值永远盖不回来（自锁）。
    # 所以本机时间未到 15:05 时，一律丢弃「日期 == 今天」的那根；收盘后正常保留。
    _now = datetime.datetime.now()
    # 注意：build_all / sync_daily 都会显式传 end=now，所以判据不能写 end is None，
    # 必须看「请求的结束时间是否已经到/过了今天」——到今天就属于「正在走的这根」。
    _up2now = (end is None) or (getattr(end, 'date', lambda: None)() >= _now.date())
    if _up2now:
        if _now.hour * 100 + _now.minute < 1505:
            _td = _now.strftime('%Y-%m-%d')
            _n0 = len(out)
            out = [b for b in out if b['date'] != _td]
            if len(out) != _n0:
                _log('盘中闸', '%s 丢弃未收盘的当天那根' % thscode)
    return out


def snapshot_indexes(codes=None, chunk=200):
    """指数实时快照（逗号批量）→ {thscode: {close,open,high,low,prev,pct,volume,turnover,ts}}

    codes 省略 → 目录里全部 848 个。
    注意：与 A 股快照同一 PriceSnapshotItem 结构，但**没有 name**（名字在 catalog）。
    """
    if codes is None:
        codes = sorted(names().keys())
    codes = list(codes)
    out = {}
    ts = 0
    for i in range(0, len(codes), chunk):
        part = codes[i:i + chunk]
        d = _fq()._req(SNAP_API, {'thscodes': ','.join(part)},
                       api_name='idx/snapshot(%d)' % len(part))
        try:
            ts = int(d.get('timestamp') or 0) or ts
        except (TypeError, ValueError):
            pass
        for it in (d.get('item') or []):
            tc = it.get('thscode')
            if not tc:
                continue
            out[tc] = {
                'close': _f(it.get('last_price')),
                'open': _f(it.get('open_price')),
                'high': _f(it.get('high_price')),
                'low': _f(it.get('low_price')),
                'prev': _f(it.get('prev_price')),
                'pct': _f(it.get('price_change_ratio_pct')),
                'volume': _f(it.get('volume')),
                'turnover': _f(it.get('turnover')),
                'ts': ts,
            }
    return out, ts


_OV_CACHE = {'ts': 0.0, 'd': {}}


def live_index_overlay(tags=('industry', 'cn_concept'), ttl=60):
    """盘中/午休：取**当天**指数快照 → {thscode: close}（失败返回 {}）

    v2.180 (2026-10-22 用户定案): 官方板块指数日线只在收盘后落盘，午休时本地末根还停在
    昨天，导致「偏多/偏空」档位不含当天半天。这里把当天快照的点位当**临时一根**参与
    均线计算 —— **只算不落盘**，正式日线仍由 append_today 收盘后写。
    """
    now = time.time()
    if _OV_CACHE['d'] and (now - _OV_CACHE['ts']) < ttl:
        return _OV_CACHE['d']              # 60 秒缓存: 2 个分析器 + 面板共用一份
    try:
        codes = all_codes(tags)
        if not codes:
            return {}
        snap, _ts = snapshot_indexes(codes)
        d = {tc: v.get('close') for tc, v in snap.items()
             if (v.get('close') or 0) > 0}
        _OV_CACHE.update({'ts': now, 'd': d})
        return d
    except Exception as e:
        _log('快照叠加', '取指数快照失败: %s' % e)
        return {}


def _hist_with_retry(code, years, end):
    last = ''
    for i in range(RETRY):
        try:
            return code, history(code, years=years, end=end), ''
        except Exception as e:
            last = str(e)[:120]
            if i < RETRY - 1:
                time.sleep(RETRY_SLEEP * (i + 1))
    return code, None, last


# ---------------------------------------------------------------- 本地读写
def _read_rows(path):
    if not os.path.exists(path):
        return []
    rows = []
    try:
        with io.open(path, encoding='utf-8', errors='replace', newline='') as f:
            rd = csv.reader(f)
            head = next(rd, None)
            if not head:
                return []
            for r in rd:
                if not r or not r[0]:
                    continue
                rows.append({
                    'date': r[0].strip(),
                    'open': _f(r[1] if len(r) > 1 else 0),
                    'close': _f(r[2] if len(r) > 2 else 0),
                    'high': _f(r[3] if len(r) > 3 else 0),
                    'low': _f(r[4] if len(r) > 4 else 0),
                    'volume': _f(r[5] if len(r) > 5 else 0),
                })
    except Exception:
        return []
    rows.sort(key=lambda x: x['date'])
    return rows


def _write_rows(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    buf = [','.join(HEADER)]
    for b in rows:
        buf.append('%s,%.4f,%.4f,%.4f,%.4f,%.0f,,' % (
            b['date'], b['open'], b['close'], b['high'], b['low'], b['volume']))
    with io.open(tmp, 'w', encoding='utf-8', newline='') as f:
        f.write('\r\n'.join(buf) + '\r\n')
    os.replace(tmp, path)


def _same(a, b):
    for k in ('open', 'close', 'high', 'low'):
        if abs(a.get(k, 0) - b.get(k, 0)) > 0.0005:
            return False
    if a.get('volume', 0) and b.get('volume', 0):
        if abs(a['volume'] - b['volume']) > max(1.0, a['volume'] * 0.001):
            return False
    return True


def write_merge(thscode, bars):
    """把 bars 合并进本地 CSV。返回 (added, changed, total)

    * 同一天已有且完全一致 → 不动
    * 同一天有但数值变了 → 覆盖（官方修正 / 成分股变动重算）
    * 新日期 → 追加
    * added==0 且 changed==0 → **根本不落盘**（mtime 不变）
    """
    p = kfile(thscode)
    old = _read_rows(p)
    if not old and not bars:
        return 0, 0, 0
    idx = {r['date']: r for r in old}
    added = changed = 0
    for b in bars:
        d = b['date']
        cur = idx.get(d)
        if cur is None:
            idx[d] = b
            added += 1
        elif not _same(cur, b):
            idx[d] = b
            changed += 1
    total = len(idx)
    if added == 0 and changed == 0:
        return 0, 0, total
    rows = [idx[k] for k in sorted(idx)]
    _write_rows(p, rows)
    return added, changed, len(rows)


# ---------------------------------------------------------------- 建库 / 增量
def build_all(workers=WORKERS, years=MAX_YEARS, tags=('industry', 'cn_concept'),
              force=False, verbose=True):
    """首次全量建库（默认 710 个 行业+概念 × 5 年）。已存在的文件默认跳过。"""
    os.makedirs(KDIR, exist_ok=True)
    codes = all_codes(tags)
    nm = names()
    t0 = time.time()
    if verbose:
        print('  待建 %d 个指数 × %d 年，%d 并发…' % (len(codes), years, workers))
    ok = skip = fail = 0
    total_bars = 0
    errs = {}
    end = datetime.datetime.now()
    todo = []
    for tc in codes:
        if not force and os.path.exists(kfile(tc)):
            skip += 1
        else:
            todo.append(tc)
    if verbose and skip:
        print('  已有本地文件，跳过 %d 个' % skip)
    done = 0
    if todo:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_hist_with_retry, tc, years, end): tc for tc in todo}
            for fu in as_completed(futs):
                code, bars, err = fu.result()
                done += 1
                if bars:
                    a, c, n = write_merge(code, bars)
                    total_bars += n
                    ok += 1
                else:
                    fail += 1
                    errs[code] = err or '空数据'
    el = time.time() - t0
    if ok or skip:
        write_zdf(tags)
        invalidate_zdf()
    res = {'ok': ok, 'skip': skip, 'fail': fail, 'bars': total_bars,
           'elapsed': round(el, 1), 'codes': len(codes), 'errors': errs}
    if verbose:
        print('  完成 %.1fs：成功 %d / 跳过 %d / 失败 %d，累计 %d 根'
              % (el, ok, skip, fail, total_bars))
    _log('建库', 'ok=%d skip=%d fail=%d bars=%d %.1fs' % (ok, skip, fail, total_bars, el))
    # v2.177: 建库顺带补齐不足 250 根的新题材(成分股倒推, 写独立 .synth.csv)
    try:
        res['filled'] = fill_all(tags=tags, verbose=False).get('n', 0)
    except Exception as _e:
        _log('补齐', 'build_all 后补齐失败: %s' % _e)
    return res


def index_last_date():
    """本地板块指数日线的**最新日期**（供前端显示「指数至 MM-DD」，避免拿运行时刻当数据日期）"""
    for nm in ('ma_scan.json', 'zdf.json'):
        try:
            fp = os.path.join(KDIR, nm)
            if not os.path.exists(fp):
                continue
            d = json.loads(io.open(fp, encoding='utf-8').read())
            if nm == 'ma_scan.json' and d.get('date'):
                return str(d['date'])[:10]
            if nm == 'zdf.json' and d:
                ds = sorted({str(v[3])[:10] for v in d.values() if isinstance(v, list) and len(v) > 3})
                if ds:
                    return ds[-1]
        except Exception:
            continue
    return ''


def stock_trade_date(sample=80):
    """真实交易日 —— 取**个股日线**里出现最多的末行日期（v2.178）

    为什么不再用「上证指数日线末根」当权威：那个来源自己也会滞后。
    2026-10-22 15:40 实测 —— 指数历史当天还没发布，末根仍是 10-21，
    于是 append_today 判「本地末根 >= 权威日」直接跳过全部 710 个，
    当天这根永远写不进去（**自锁**）。而个股日线 15:35 已经由「拉全市场快照+写CSV」
    写成 10-22，所以拿个股末行日主流值当权威才跟得上。
    """
    try:
        fs = [f for f in os.listdir(DAILY_DIR) if f.lower().endswith('.csv')]
    except Exception:
        return ''
    if not fs:
        return ''
    fs.sort()
    step = max(1, len(fs) // int(sample or 80))
    cnt = {}
    for fn in fs[::step][:int(sample or 80)]:
        try:
            with io.open(os.path.join(DAILY_DIR, fn), 'rb') as f:
                f.seek(0, 2)
                n = f.tell()
                f.seek(max(0, n - 200))
                tail = f.read().decode('utf-8', 'replace')
            for line in reversed(tail.strip().split('\n')):
                d = line.split(',')[0].strip()
                if len(d) == 10 and d[4] == '-':
                    cnt[d] = cnt.get(d, 0) + 1
                    break
        except Exception:
            continue
    if not cnt:
        return ''
    return sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def last_trade_date(proxy='000001.SH'):
    """权威交易日 = 上证指数日线最后一根的日期（不吃快照 ts 的亏）"""
    for tc in (proxy, '399001.SZ'):
        try:
            bars = history(tc, years=1)
            if bars:
                return str(bars[-1]['date'])[:10]
        except Exception as e:
            _log('交易日', '%s 失败: %s' % (tc, e))
    return ''


def append_today(verbose=True, tags=('industry', 'cn_concept'), force=False):
    """收盘后把**当天那一根**写进本地日线 —— 走指数快照（710 个约 2 秒）

    为什么用快照而不是逐只拉历史：快照一次给 710 个板块的 开/高/低/收/量，而逐只拉
    历史要 710 次请求（约 30~60 秒）。

    ⚠ 教训（2026-10-22 踩过）：快照响应里的 `timestamp` 是**服务器当前时间**，不是
    数据所属交易日 —— 凌晨 4 点拉到的是昨天收盘的数据，却带着今天的 ts。拿它当 date
    会把同一根日线写两遍（10-22 那根和 10-21 一模一样，710 个文件全写脏）。
    所以日期**既不用 ts 也不用本机钟**，而是拿「上证指数日线最后一根的日期」当权威
    交易日（多花 0.4 秒 1 个请求），并且加三重闸：
      1) 15:05 之前不写（盘中/盘前一律不写）
      2) 本地最后一根已经 >= 权威交易日 → 跳过
      3) 快照的 收/量 与本地末根完全一致 → 视为同一场数据，跳过
    force=True 只跳过第 1 道闸（确认已收盘的手工补跑用）。
    v2.208: 末根日期 == 交易日但数值不同 → 覆盖（不再无条件跳过，可自我修复）。
    """
    now = datetime.datetime.now()
    if not force and (now.hour * 100 + now.minute) < 1505:
        if verbose:
            print('  未到收盘 15:05，不写日线')
        return {'ok': 0, 'skipped': True, 'reason': '未到收盘 15:05',
                'added': 0, 'changed': 0, 'stale': 0, 'fail': 0, 'elapsed': 0.0}
    os.makedirs(KDIR, exist_ok=True)
    t0 = time.time()
    _sd = stock_trade_date()      # 个股日线末行日（15:35 已写今天）—— 主源
    _id = last_trade_date()       # 指数历史末根（可能滞后）—— 取两者较晚
    if _sd and _id:
        date = _sd if _sd >= _id else _id
    else:
        date = _sd or _id
    if not date:
        if verbose:
            print('  取不到上证指数日线，无法确定交易日，跳过')
        return {'ok': 0, 'skipped': True, 'reason': '权威交易日不可得',
                'added': 0, 'changed': 0, 'stale': 0, 'fail': 0, 'elapsed': 0.0}
    # v2.231 (2026-10-26): 指数快照遇限流重试 —— 10-26 15:40 就是被同花顺 429 打挂、无重试，
    #   当天那根 710 个板块全部漏写（事后才手工 append_today(force=True) 补回来）。
    #   710 个约 2 秒，重试 3 次成本很低，远低于"漏一天"的代价。
    snap = None
    _snap_err = ''
    for _i in range(3):
        try:
            snap, ts = snapshot_indexes()
            if snap:
                break
        except Exception as _e:
            _snap_err = str(_e)[:160]
            _log('指数快照', '第%d次失败: %s' % (_i + 1, _snap_err))
        if _i < 2:
            time.sleep(2.0 * (_i + 1))
    if not snap:
        if verbose:
            print('  指数快照取不到（重试 3 次），本轮不写日线: %s' % _snap_err)
        _log('追加', 'index snapshot failed after 3 tries: %s' % _snap_err)
        return {'ok': 0, 'skipped': True, 'reason': '指数快照失败: ' + _snap_err,
                'added': 0, 'changed': 0, 'stale': 0, 'fail': 0, 'elapsed': round(time.time() - t0, 1),
                'date': date}
    nm = names()
    added = changed = stale = fail = 0
    for tc, it in (snap or {}).items():
        info = nm.get(tc) or {}
        if tags and info.get('tag') not in tags:
            continue
        if not it.get('close'):
            fail += 1
            continue
        bar = {'date': date, 'open': it['open'], 'close': it['close'],
               'high': it['high'], 'low': it['low'], 'volume': it['volume']}
        if min(bar['open'], bar['close'], bar['high'], bar['low']) <= 0:
            fail += 1
            continue
        last = read_kline(tc, limit=1)
        if last:
            _ld = str(last[0]['date'])
            if _ld > date:
                continue                      # 本地已有更晚的一根，不动
            _lc = float(last[0]['close'])
            _pv = float(it.get('prev') or 0)
            if _pv > 0 and abs(_pv - _lc) > 1e-3 and abs(float(bar['close']) - _lc) < 1e-3:
                stale += 1                    # 快照的「昨收」对不上本地末根 ⇒ 快照落后一场
                continue
            if _same(last[0], bar):
                stale += 1                    # 与末根完全一致 ⇒ 同一场数据，不用写
                continue
            # ⚠ 同一天但数值不同 ⇒ **不是**跳过，而是覆盖（自我修复）
            # 2026-10-23：11:40 盘中 force 重拉把半天数据写进当天，15:40 收盘后
            # 原判据「本地末根 >= date 就 continue」导致 710 个全部跳过，真值永远盖不回来。
        a, c, n = write_merge(tc, [bar])
        added += a
        changed += c
    if added or changed:
        write_zdf(tags)
        invalidate_zdf()
        try:
            build_ma_cache(verbose=verbose)
        except Exception as e:
            _log('均线缓存', '失败: %s' % e)
    el = time.time() - t0
    res = {'ok': added + changed, 'added': added, 'changed': changed,
           'stale': stale, 'fail': fail, 'date': date, 'elapsed': round(el, 1),
           'date_src': ('stock' if _sd and date == _sd else 'index')}
    if verbose:
        print('  追加 %s：新增 %d / 修正 %d / 快照过期 %d / 跳过 %d  %.1fs'
              % (date, added, changed, stale, fail, el))
    _log('追加', 'date=%s added=%d changed=%d stale=%d fail=%d %.1fs'
         % (date, added, changed, stale, fail, el))
    return res


def drop_date(date_str, tags=('industry', 'cn_concept'), verbose=True):
    """删掉某一天的日线（写坏了 / 误追加时用），返回受影响文件数"""
    n = 0
    for tc in all_codes(tags):
        p = kfile(tc)
        rows = _read_rows(p)
        keep = [r for r in rows if str(r.get('date')) != date_str]
        if len(keep) != len(rows):
            _write_rows(p, keep)
            n += 1
    if n:
        write_zdf(tags)
        invalidate_zdf()
    if verbose:
        print('  已从 %d 个文件删除 %s' % (n, date_str))
    _log('删除', 'date=%s files=%d' % (date_str, n))
    return n


def gap_report(tags=('industry', 'cn_concept'), lag=5):
    """找出本地最后一根落后太多的指数（漏跑 / 停更）→ [thscode]"""
    nm = names()
    out = []
    cut = (datetime.datetime.now() - datetime.timedelta(days=int(lag) * 2)).strftime('%Y-%m-%d')
    for tc in all_codes(tags):
        rows = read_kline(tc, limit=1)
        if not rows or rows[-1]['date'] < cut:
            out.append(tc)
    return out


def sync_daily(workers=WORKERS, days=15, tags=('industry', 'cn_concept'), verbose=True):
    """每日增量：每只指数只拉最近 `days` 个自然日，补新根 / 修正被官方重算的根。"""
    os.makedirs(KDIR, exist_ok=True)
    codes = all_codes(tags)
    nm = names()
    t0 = time.time()
    added = changed = fail = 0
    errs = {}
    done = 0

    def _one(code):
        try:
            bars = history(code, years=1, end=datetime.datetime.now())
            bars = [b for b in bars if b['date'] >= _cut]
            return code, bars, ''
        except Exception as e:
            return code, None, str(e)[:120]

    _cut = (datetime.datetime.now() - datetime.timedelta(days=int(days))).strftime('%Y-%m-%d')
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_one, tc): tc for tc in codes}
        for fu in as_completed(futs):
            code, bars, err = fu.result()
            done += 1
            if bars is None:
                fail += 1
                errs[code] = err
                continue
            a, c, n = write_merge(code, bars)
            added += a
            changed += c
    el = time.time() - t0
    if added or changed:
        write_zdf(tags)
        invalidate_zdf()
    res = {'ok': len(codes) - fail, 'fail': fail, 'added': added, 'changed': changed,
           'elapsed': round(el, 1), 'codes': len(codes), 'errors': errs}
    if verbose:
        print('  增量完成 %.1fs：新增 %d 根 / 修正 %d 根 / 失败 %d 个'
              % (el, added, changed, fail))
    _log('增量', 'ok=%d fail=%d added=%d changed=%d %.1fs'
         % (res['ok'], fail, added, changed, el))
    return res


def rebuild(thscode, years=MAX_YEARS):
    """单只指数强制重拉覆盖（成分股变动 / 官方重算历史时用）"""
    bars = history(thscode, years=years)
    p = kfile(thscode)
    if not bars:
        return 0
    _write_rows(p, bars)
    return len(bars)


# ---------------------------------------------------------------- 均线
def read_kline(thscode, limit=0):
    """读本地日线（升序）。limit>0 只取最后 limit 根。"""
    rows = _read_rows(kfile(thscode))
    if limit and limit > 0:
        return rows[-limit:]
    return rows


ZDF_FILE = os.path.join(KDIR, 'zdf.json')
_ZDF_CACHE = {'t': 0.0, 'd': None}


def _write_json_file(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with io.open(tmp, 'w', encoding='utf-8') as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(',', ':')))
    os.replace(tmp, path)


def zdf_of(rows):
    """(5日涨幅%, 20日涨幅%) —— 数据不足的那一项给 0.0"""
    c = [r['close'] for r in rows if r['close'] > 0]
    if not c:
        return 0.0, 0.0
    z5 = round((c[-1] / c[-6] - 1) * 100, 2) if len(c) >= 6 and c[-6] else 0.0
    z20 = round((c[-1] / c[-21] - 1) * 100, 2) if len(c) >= 21 and c[-21] else 0.0
    return z5, z20


def write_zdf(tags=('industry', 'cn_concept')):
    """由本地日线算出全部指数的 5日/20日涨幅，落一个 JSON 供实时接口秒读

    为什么要落盘：710 个 CSV 每次全解析约 1~2 秒，而板块热度缓存 TTL 只有 60 秒，
    实时路径上不能这么干。build/sync 之后重算一次即可。
    → {thscode: [z5, z20, 根数, 末根日期]}
    """
    out = {}
    for tc in all_codes(tags):
        rows = read_kline(tc)
        if len(rows) < 6:
            continue
        z5, z20 = zdf_of(rows)
        out[tc] = [z5, z20, len(rows), rows[-1]['date']]
    try:
        _write_json_file(ZDF_FILE, out)
    except Exception as e:
        _log('zdf写盘失败', str(e)[:120])
    return out


def zdf_map(ttl=600):
    """{thscode: [z5, z20, n, last_date]}（进程内缓存 ttl 秒，文件不在就现算一次）"""
    now = time.time()
    ent = _ZDF_CACHE
    if ent['d'] is not None and (now - ent['t']) < ttl:
        return ent['d']
    d = None
    try:
        d = json.load(io.open(ZDF_FILE, encoding='utf-8'))
    except Exception:
        d = None
    if not d:
        d = write_zdf()
    ent['t'] = now
    ent['d'] = d or {}
    return ent['d']


def invalidate_zdf():
    _ZDF_CACHE['t'] = 0.0
    _ZDF_CACHE['d'] = None


# ---------------------------------------------------------------- 成分股广度
TICK_FILE = os.path.join(IDX_DIR, 'tickers.json')
TICK_API = '/api/meta/tickers/list'
_MEM_CACHE = {'t': 0.0, 'd': None}
_TICK_CACHE = {'t': 0.0, 'd': None}


def members(tags=('industry', 'cn_concept')):
    """{thscode: [6位裸码...]}（进程内缓存，文件变了按 mtime 失效）"""
    p = os.path.join(IDX_DIR, 'members.json')
    try:
        mt = os.path.getmtime(p)
    except Exception:
        return {}
    ent = _MEM_CACHE
    if ent['d'] is not None and abs(ent['t'] - mt) < 1e-6:
        return ent['d']
    try:
        d = json.load(io.open(p, encoding='utf-8'))
    except Exception:
        d = {}
    nm = names()
    out = {}
    for tc, arr in (d or {}).items():
        info = nm.get(tc) or {}
        if tags and info.get('tag') not in tags:
            continue
        out[tc] = [str(x)[-6:] for x in (arr or []) if str(x).strip()]
    ent['t'] = mt
    ent['d'] = out
    return out


def tickers(ttl_days=7, force=False):
    """{6位裸码: 名称} —— 走 /api/meta/tickers/list 拉全 A 股，缓存 ttl_days 天

    板块要显示「领涨股」的名字，而同花顺的行情快照不带 name（成分股列表也不带），
    所以单独备一份代码→名称表。
    """
    ent = _TICK_CACHE
    if not force and ent['d'] and (time.time() - ent['t']) < 3600:
        return ent['d']
    d = None
    if not force:
        try:
            mt = os.path.getmtime(TICK_FILE)
            if (time.time() - mt) < ttl_days * 86400:
                d = json.load(io.open(TICK_FILE, encoding='utf-8'))
        except Exception:
            d = None
    if not d:
        try:
            out = {}
            off = 0
            for _ in range(4):
                r = _fq()._req(TICK_API, {'asset_type': 'a-share',
                                          'limit': 10000, 'offset': off},
                               api_name='meta/tickers/list')
                items = r.get('item') or []
                if not items:
                    break
                for it in items:
                    tc = str(it.get('thscode') or '')
                    nmv = it.get('name') or ''
                    if tc and nmv:
                        out[tc.split('.')[0]] = nmv
                if len(items) < 10000:
                    break
                off += len(items)
            if out:
                _write_json_file(TICK_FILE, out)
                d = out
        except Exception as e:
            _log('tickers失败', str(e)[:120])
            d = None
    if not d:
        try:
            d = json.load(io.open(TICK_FILE, encoding='utf-8'))
        except Exception:
            d = {}
    ent['t'] = time.time()
    ent['d'] = d or {}
    return ent['d']


def _lead_code(code):
    """6位裸码 → 腾讯写法 'sh600000' / 'sz000001' / 'bj920010'"""
    c = str(code or '')[-6:]
    if not c:
        return ''
    if c[:3] == '920' or c[0] in ('4', '8'):
        return 'bj' + c
    if c[0] in ('6', '9'):
        return 'sh' + c
    return 'sz' + c


def breadth(stock_pct, tags=('industry', 'cn_concept'), with_names=True):
    """由成分股实时涨跌幅算板块「涨跌家数 + 领涨股」

    stock_pct: {6位裸码: 涨跌幅%}  （来自 fuyao.snapshot_all()）
    返回 {thscode: {'up':n, 'total':n, 'lead_stock':名, 'lead_code':'sh600000', 'lead_pct':x}}

    口径：同花顺板块指数是成分股等权平均，所以「涨跌家数」按同一批成分股数一份就对。
    停牌/无数据的成分股（pct is None/0 且 volume=0 的情形）不计入 total 的分母会让
    up/total 看起来偏小，这里按「有涨跌幅数据」计入 total。
    """
    mem = members(tags)
    nm_stock = tickers() if with_names else {}
    out = {}
    for tc, arr in mem.items():
        up = 0
        total = 0
        best = None
        best_pct = None
        for c in arr:
            v = stock_pct.get(c)
            if v is None:
                continue
            total += 1
            if v > 0:
                up += 1
            if best_pct is None or v > best_pct:
                best_pct = v
                best = c
        if total <= 0:
            continue
        out[tc] = {'up': up, 'total': total,
                   'lead_stock': (nm_stock.get(best) or '') if best else '',
                   'lead_code': _lead_code(best) if best else '',
                   'lead_pct': round(float(best_pct), 2) if best_pct is not None else 0.0}
    return out


def ma_seq(thscode, periods=MA_PERIODS, rows=None):
    """各周期均线值 → {period: value or None}（数据不足返回 None）"""
    if rows is None:
        rows = read_kline(thscode)
    closes = [r['close'] for r in rows if r['close'] > 0]
    out = {}
    for p in periods:
        p = int(p)
        if len(closes) >= p and p > 0:
            out[p] = round(sum(closes[-p:]) / float(p), 4)
        else:
            out[p] = None
    return out


def ma_aligned(thscode, periods=None, rows=None):
    """多头排列判定：短周期 > 长周期，逐一对齐（默认 5>10>21>55，若数据够再带上 120/250）"""
    rows = rows if rows is not None else read_kline(thscode)
    got = ma_seq(thscode, periods or MA_PERIODS, rows=rows)
    seq = [p for p in (periods or MA_PERIODS) if got.get(p) is not None]
    # v2.193 (2026-10-23): 原来写死 < 4 —— 三线档 (5,10,21) 只有 3 条线，永远判 False。
    #   与 scan_all 的口径对齐：至少 min(4, len(periods)) 条线齐全才算得出多头排列。
    if len(seq) < min(4, len(periods or MA_PERIODS)):
        return False, got
    vals = [got[p] for p in seq]
    return all(vals[i] > vals[i + 1] for i in range(len(vals) - 1)), got


MA_CACHE = os.path.join(KDIR, 'ma_scan.json')


def build_ma_cache(verbose=True):
    """把 710 个指数的 均线/涨幅 预算好落盘 —— 实时面板不能每次去读 809k 行（16 秒）

    zdf.json 只存 5/20 日涨幅（秒读），这个存 6 条均线 + 1/5/20/60 日涨幅。
    每天 build / sync / append 之后刷一次，面板读它 <50ms。
    """
    nm = names()
    out = {}
    for tc in all_codes(('industry', 'cn_concept')):
        rows = merged_rows(tc)          # v2.212: 官方 + 倒推段（与 K 线图同口径）
        if len(rows) < 20:
            continue
        closes = [r['close'] for r in rows if r['close'] > 0]
        if not closes:
            continue
        last = closes[-1]
        got = ma_seq(tc, periods=MA_PERIODS, rows=rows)
        info = nm.get(tc) or {}

        def _z(n):
            return round((last / closes[-n - 1] - 1) * 100, 2) if len(closes) > n and closes[-n - 1] else 0.0
        out[tc] = {'name': info.get('name', ''), 'tag': info.get('tag', ''),
                   'close': round(last, 2), 'pct': _z(1), 'z5': _z(5),
                   'z20': _z(20), 'z60': _z(60), 'bars': len(rows),
                   'ma': {str(k): v for k, v in got.items()}}
    date = ''
    for v in out.values():
        pass
    try:
        date = merged_rows(next(iter(out)))[-1]['date'] if out else ''
    except Exception:
        date = ''
    data = {'date': date, 'ts': time.strftime('%Y-%m-%d %H:%M:%S'), 'rows': out}
    tmp = MA_CACHE + '.tmp'
    with io.open(tmp, 'w', encoding='utf-8', newline='\n') as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, MA_CACHE)
    if verbose:
        print('  均线缓存 → %s（%d 个指数，末根 %s）' % (MA_CACHE, len(out), date))
    return {'ok': len(out), 'date': date}


def ma_cache_rows():
    """读均线缓存；缓存缺失/过期（末根日期与 zdf 不一致）返回 None"""
    try:
        with io.open(MA_CACHE, encoding='utf-8') as f:
            d = json.load(f)
    except Exception:
        return None
    rows = d.get('rows') or {}
    if not rows:
        return None
    try:
        z = json.load(io.open(ZDF_FILE, encoding='utf-8')) if os.path.exists(ZDF_FILE) else {}
    except Exception:
        z = {}
    if z:
        zdate = ''
        for v in z.values():
            if isinstance(v, list) and len(v) > 3:
                zdate = max(zdate, str(v[3]))
        if zdate and str(d.get('date', '')) < zdate:
            return None            # 缓存比 zdf 旧 ⇒ 有新的日线进来过，得重算
    return rows


def scan_all(periods=None, tags=('industry', 'cn_concept'), min_bars=60,
             overlay=None, overlay_date=''):
    """全量扫（**不只多头**）→ [{thscode,name,tag,close,pct,z5,z20,z60,bars,ma,aligned,name_l2}]

    与 scan_ma 的区别：scan_ma 只回多头排列的，这个是全量 + aligned 布尔，
    给「板块指数均线」面板用（要能按排列状态排序/筛选）。
    """
    periods = tuple(int(p) for p in (periods or MA_PERIODS))
    nm = names()
    out = []
    overlay = overlay or {}

    def _closes_of(tc, rows):
        """收盘序列 + 根数；overlay 有这只票就把当天最新点位接上（不落盘）

        本地末根 == overlay_date（收盘后已写盘）→ **替换**最后一根，amount 不重复算一天；
        否则**追加**一根虚拟日线（午休/盘中）。
        """
        closes = [r['close'] for r in rows if r['close'] > 0]
        bars = len(rows)
        try:
            oc = float(overlay.get(tc) or 0)
        except (TypeError, ValueError):
            oc = 0.0
        if oc > 0 and closes:
            if overlay_date and str(rows[-1].get('date') or '') == overlay_date:
                closes[-1] = oc
            else:
                closes.append(oc)
                bars += 1
        return closes, bars

    def _mas_of(closes):
        got = {}
        for p in periods:
            got[p] = round(sum(closes[-p:]) / float(p), 2) if len(closes) >= p else None
        return got

    def _mk(tc, name, tag, close, pct, z5, z20, z60, bars, got):
        seq = [p for p in periods if got.get(p) is not None]
        ok = False
        if len(seq) >= min(4, len(periods)):
            # v2.209 (2026-10-23): 多头排列必须**收盘价排首位** —— close > MA5 > MA10 > ...
            #   原来只比均线自己是否递减, 漏了 "现价还在 MA5 之上" 这一条:
            #   国有大型银行 10-23 收 3484.19 / MA5 3552.17 (已破 5 日线), 四线仍递减 ⇒
            #   旧写法判成「多头排列」, 与 engine 里合成口径 (seq[0]=现价) 以及
            #   v2.173 跟用户解释过的「中性=均线有序但跌破 MA5」直接打架。
            #   ⇒ 加上现价这一项, 两种口径重新统一。
            vals = [close] + [got[p] for p in seq]
            ok = all(vals[i] > vals[i + 1] for i in range(len(vals) - 1))
        return {'thscode': tc, 'name': name, 'tag': tag, 'close': close,
                'pct': pct, 'z5': z5, 'z20': z20, 'z60': z60,
                'bars': bars, 'ma': got, 'aligned': ok, 'ma_n': len(seq)}

    cached = None if overlay else ma_cache_rows()
    if cached:
        for tc, v in cached.items():
            if tags and v.get('tag') not in tags:
                continue
            if int(v.get('bars', 0)) < min_bars:
                continue
            got = {int(k): val for k, val in (v.get('ma') or {}).items()}
            out.append(_mk(tc, v.get('name', ''), v.get('tag', ''), v.get('close', 0),
                           v.get('pct', 0), v.get('z5', 0), v.get('z20', 0),
                           v.get('z60', 0), int(v.get('bars', 0)), got))
    else:
        for tc in all_codes(tags):
            rows = merged_rows(tc)      # v2.212: 官方 + 倒推段（与 K 线图同口径）
            if len(rows) < min_bars:
                continue
            closes, _bars = _closes_of(tc, rows)
            if len(closes) < min_bars:
                continue
            last = closes[-1]
            got = _mas_of(closes) if overlay else ma_seq(tc, periods=periods, rows=rows)

            def _z(n):
                return round((last / closes[-n - 1] - 1) * 100, 2) if len(closes) > n and closes[-n - 1] else 0.0
            info = nm.get(tc) or {}
            out.append(_mk(tc, info.get('name', ''), info.get('tag', ''), round(last, 2),
                           _z(1), _z(5), _z(20), _z(60), _bars, got))
    out.sort(key=lambda x: (-(1 if x['aligned'] else 0), -(x.get('z20') or 0)))
    return {'rows': out, 'periods': list(periods), 'total': len(out),
            'aligned': sum(1 for x in out if x['aligned'])}


def scan_ma(periods=None, tags=('industry', 'cn_concept'), min_bars=60):
    """全量扫多头排列 → {'aligned': [...], 'total': n, 'by_tag': {...}}"""
    periods = tuple(periods or MA_PERIODS)
    nm = names()
    codes = all_codes(tags)
    aligned = []
    total = 0
    for tc in codes:
        rows = read_kline(tc)
        if len(rows) < min_bars:
            continue
        total += 1
        ok, got = ma_aligned(tc, periods=periods, rows=rows)
        if not ok:
            continue
        closes = [r['close'] for r in rows if r['close'] > 0]
        last = closes[-1] if closes else 0
        z20 = round((last / closes[-21] - 1) * 100, 2) if len(closes) > 21 and closes[-21] else 0.0
        z60 = round((last / closes[-61] - 1) * 100, 2) if len(closes) > 61 and closes[-61] else 0.0
        aligned.append({
            'thscode': tc,
            'name': (nm.get(tc) or {}).get('name', ''),
            'tag': (nm.get(tc) or {}).get('tag', ''),
            'close': round(last, 2),
            'z20': z20, 'z60': z60,
            'bars': len(rows),
            'ma': got,
        })
    aligned.sort(key=lambda x: -(x.get('z20') or 0))
    by_tag = {}
    for a in aligned:
        by_tag[a['tag']] = by_tag.get(a['tag'], 0) + 1
    return {'aligned': aligned, 'total': total, 'by_tag': by_tag, 'periods': list(periods)}


# ---------------------------------------------------------------- CLI
# ------------------------------------------------------- 官方点数 K 线（v2.177）
# 为什么直取官方指数
# ----------------
# 终端 /api/sector_kline 原本是「成分股等权合成、1000 点起」(engine.composite_sector_kline)。
# 2026-10-22 起本地已有官方指数日线(本模块 build_all 建库)，直接读它 ⇒ 点位与同花顺
# 完全一致，也省掉每次重算几百个板块；只有匹配不到官方指数时才回退合成。
#
# 不足 250 根的新题材(886108/886110/886111/886112/886113，2024–2026 才上线)
# 按用户定案补齐：官方有数据的用官方，往前缺的用**现有成分股历史**等权倒推，
# 整段等比缩放到"官方第一根收盘"，接缝处点位连续(等比缩放不改变涨跌幅曲线)。
# 倒推段写到独立文件 *.synth.csv —— 绝不混进官方 CSV，保证官方数据纯净可校验。

DAILY_DIR = os.path.join(BASE_DIR, 'kline_data', 'daily')
MIN_BARS = 250          # 用户要求：至少写够 250 个交易日(250 日均线可算)


def _csv_path(bare):
    """6 位裸码 → 个股日线 CSV 路径(与终端其余地方同规则: 920/4/8 走北交所 BZ)"""
    c = str(bare or '').strip()
    if not c:
        return ''
    if c[:3] == '920' or c[0] in '48':
        pre = 'BZ'
    elif c[0] in '69':
        pre = 'SH'
    else:
        pre = 'SZ'
    return os.path.join(DAILY_DIR, pre + c + '.csv')


def _sma(vals, p):
    """简单移动平均，前 p-1 位 None(与 engine.Indicators.sma 同法)"""
    n = len(vals)
    out = [None] * n
    p = int(p or 0)
    if p <= 0 or n < p:
        return out
    acc = 0.0
    for i in range(n):
        acc += vals[i]
        if i >= p:
            acc -= vals[i - p]
        if i >= p - 1:
            out[i] = round(acc / float(p), 2)
    return out


def _norm_name(s):
    return ''.join(str(s or '').split()).upper()


_NM_CACHE = {'ts': 0.0, 'exact': {}, 'norm': {}}


def name_index(force=False):
    """({名称: thscode}, {归一化名: thscode})

    概念名我们沿用旧写法(WiFi 6 / AI PC / 中国AI 50)，官方是 WiFi6 / AIPC / 中国AI50
    ⇒ 精确匹配会漏 3 个，必须按"去空格转大写"兜底。
    """
    now = time.time()
    if not force and _NM_CACHE['exact'] and (now - _NM_CACHE['ts']) < 3600:
        return _NM_CACHE['exact'], _NM_CACHE['norm']
    exact, norm = {}, {}
    for tc, info in names().items():
        nm = (info or {}).get('name') or ''
        if not nm:
            continue
        exact.setdefault(nm, tc)
        norm.setdefault(_norm_name(nm), tc)
    _NM_CACHE.update({'ts': now, 'exact': exact, 'norm': norm})
    return exact, norm


def resolve(name):
    """板块名 → thscode(精确优先, 再归一化兜底)；找不到返回 ''"""
    if not name:
        return ''
    exact, norm = name_index()
    return exact.get(name) or norm.get(_norm_name(name)) or ''


def _synth_file(tc):
    return os.path.join(KDIR, os.path.basename(kfile(tc)).replace('.csv', '.synth.csv'))


def read_synth(tc):
    return _read_rows(_synth_file(tc))


def merged_rows(tc):
    """官方日线 + 往前倒推段（新概念 250 根补齐）合并后的完整序列

    v2.212 (2026-10-24 DEBUG #3): 板块均线面板(scan_all / build_ma_cache)原来只读
    read_kline() 的**官方**段，而 K 线图走 official_kline() 是**合并**的 —— 同一个板块
    两个口径。后果：
      · 886112 MLCC概念(官方 55 根)/886113 2026三季报预增(官方 10 根) 低于
        min_bars=60 门槛 ⇒ 在板块均线面板里整条消失；
      · 886108/110/111 只有 55~187 根 ⇒ MA120/MA250 永远算不出来。
    统一到合并口径后与 K 线图一致（倒推段本来就已经等比缩放到官方首根收盘，接缝连续）。
    """
    return read_synth(tc) + read_kline(tc)


def _close_series(codes, upto=''):
    """{代码: {date: close}} —— 只保留 <= upto，且至少 20 根"""
    out = {}
    for c in codes:
        rows = _read_rows(_csv_path(c))
        if not rows:
            continue
        d = {}
        for r in rows:
            if upto and r['date'] > upto:
                break
            if r['close'] > 0:
                d[r['date']] = r['close']
        if len(d) >= 20:
            out[c] = d
    return out


def _backcast_prefix(tc, first_date, first_close, need):
    """用现有成分股历史等权倒推 first_date 之前 need 根，缩放到 first_close

    返回 [{date,open,close,high,low,volume}](升序，不含 first_date 那天)
    """
    if need <= 0:
        return []
    try:
        codes = members().get(tc) or []
    except Exception:
        codes = []
    if len(codes) < 3:
        return []
    series = _close_series(codes, upto=first_date)
    if len(series) < 3:
        return []
    pct = {}
    for c, d in series.items():
        ds = sorted(d.keys())
        for i in range(1, len(ds)):
            prev = d[ds[i - 1]]
            if prev > 0:
                pct.setdefault(ds[i], []).append(d[ds[i]] / prev - 1.0)
    axis = sorted([x for x in pct.keys() if x <= first_date])
    if len(axis) < 2:
        return []
    vals, idx = [], 1000.0
    for d in axis:
        arr = pct.get(d) or []
        if arr:
            idx *= (1.0 + sum(arr) / float(len(arr)))
        vals.append((d, idx))
    if not vals or vals[-1][1] <= 0 or not first_close:
        return []
    k = float(first_close) / vals[-1][1]
    pre = vals[:-1][-need:]
    out, pc = [], None
    for d, v in pre:
        c = v * k
        o = pc if pc else c
        out.append({'date': d, 'open': round(o, 4), 'close': round(c, 4),
                    'high': round(max(o, c), 4), 'low': round(min(o, c), 4),
                    'volume': 0.0})
        pc = c
    return out


def fill_short(tc, target=MIN_BARS, verbose=False):
    """给单个指数补 synth 段，返回补了几根(0 = 不用补/补不了)"""
    rows = read_kline(tc)
    if not rows:
        return 0
    need = int(target) - len(rows)
    sf = _synth_file(tc)
    if need <= 0:
        if os.path.exists(sf):
            try:
                os.remove(sf)
            except Exception:
                pass
        return 0
    pre = _backcast_prefix(tc, rows[0]['date'], rows[0]['close'], need)
    if not pre:
        return 0
    _write_rows(sf, pre)
    if verbose:
        print('    %-12s %-16s 官方 %d 根 + 倒推 %d 根 = %d'
              % (tc, (names().get(tc) or {}).get('name', ''), len(rows), len(pre),
                 len(rows) + len(pre)))
    return len(pre)


def fill_all(tags=('industry', 'cn_concept'), target=MIN_BARS, verbose=True):
    """所有不足 target 根的指数批量补 synth 段"""
    t0 = time.time()
    n = 0
    det = []
    for tc in all_codes(tags):
        try:
            k = fill_short(tc, target=target, verbose=verbose)
        except Exception as e:
            _log('补齐', '%s 失败: %s' % (tc, e))
            k = 0
        if k:
            n += 1
            det.append(tc)
    el = time.time() - t0
    if verbose:
        print('  补齐 %d 个指数到 %d 根，%.1fs' % (n, target, el))
    _log('补齐', 'filled=%d target=%d %.1fs' % (n, target, el))
    return {'ok': n, 'n': n, 'codes': det, 'target': target, 'elapsed': round(el, 1)}


def official_kline(kind='sector', name='', target=MIN_BARS, periods=MA_PERIODS):
    """板块/概念 **官方指数** 日K —— 形状与 engine.composite_sector_kline 完全一致

    返回 {name, kind, is_composite:False, source:'fuyao', thscode, tag, count,
          dates, ohlc:[[o,c,lo,hi]], volume, mas:{'maP':[...]},
          first_date, last_date, last_close, synth_prefix}
    匹配不到官方指数(腾讯独有板块名 / 未建库) → None，调用方回退合成。
    """
    tc = resolve(name)
    if not tc:
        return None
    info = names().get(tc) or {}
    if (info.get('tag') or '') in ('tszs', 'region'):
        return None                        # 883 特色统计 / 882 地域 不是真板块
    off = read_kline(tc)
    if len(off) < 2:
        return None
    syn = read_synth(tc)
    rows = (syn + off) if syn else off
    dates = [r['date'] for r in rows]
    closes = [r['close'] for r in rows]
    ohlc = [[r['open'], r['close'], r['low'], r['high']] for r in rows]
    vol = [r['volume'] for r in rows]
    mas = {}
    for p in periods:
        mas['ma%d' % int(p)] = _sma(closes, int(p))
    return {'name': name, 'kind': kind, 'is_composite': False, 'source': 'fuyao',
            'thscode': tc, 'tag': info.get('tag') or '',
            'count': len(dates), 'dates': dates, 'ohlc': ohlc, 'volume': vol,
            'mas': mas, 'first_date': dates[0], 'last_date': dates[-1],
            'last_close': round(closes[-1], 3), 'synth_prefix': len(syn)}


def _cli():
    acts = set(a.lstrip('-') for a in sys.argv[1:])
    if not acts:
        acts = {'scan'}
    fq = _fq()
    if not fq.configured():
        raise SystemExit('未配置同花顺 API Key（数据源设置 → 同花顺 fuyao）')
    print('日线库: %s' % KDIR)
    if 'build' in acts:
        print('[build] 全量建库（710 个 × 5 年）')
        build_all()
    if 'sync' in acts:
        print('[sync] 每日增量（逐只拉最近 15 天补洞）')
        sync_daily()
    if 'append' in acts:
        print('[append] 收盘后追加当天一根（走指数快照）')
        append_today()
    if 'gap' in acts:
        g = gap_report()
        print('[gap] 落后指数 %d 个' % len(g))
        for c in g[:50]:
            rows = read_kline(c, limit=1)
            print('    %-12s %s' % (c, rows[-1]['date'] if rows else '(空)'))
    if 'rebuild' in acts:
        print('[rebuild] 全量重拉 5 年覆盖（710 个约 60 秒）')
        build_all(force=True)
    if 'scan' in acts:
        print('[scan] 多头排列扫描')
        r = scan_ma()
        print('  有数据的指数 %d 个，多头排列 %d 个 %s' % (r['total'], len(r['aligned']), r['by_tag']))
        for a in r['aligned'][:40]:
            print('    %-10s %-14s %-4s 20日%+7.2f%% 60日%+7.2f%%  %d根'
                  % (a['thscode'], a['name'], a['tag'], a['z20'], a['z60'], a['bars']))
    if 'fill' in acts:
        print('[fill] 补齐不足 %d 根的新题材(成分股倒推)' % MIN_BARS)
        r = fill_all()
        print('  补齐 %d 个: %s' % (r['n'], ' '.join(r['codes'])))
    if 'list' in acts:
        codes = all_codes()
        print('  共 %d 个待建/已建指数' % len(codes))
        for tc in codes[:20]:
            p = kfile(tc)
            n = len(_read_rows(p)) if os.path.exists(p) else 0
            print('    %-12s %-16s %d 根' % (tc, (names().get(tc) or {}).get('name', ''), n))


if __name__ == '__main__':
    _cli()
