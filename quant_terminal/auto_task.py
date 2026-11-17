# -*- coding: utf-8 -*-
"""v2.152 (2026-10-18): 四时点自动任务调度器

用户定案 (m07888 等):
  11:31 拉全市场快照落临时快照 → 11:35 更新板块
  15:31 拉全市场快照 + 写 CSV      → 15:35 更新板块

硬规则（每一条都对应一个真实的坑，不是防御性编程）:
  ① **只在终端开着时执行** —— 判据是「最近一次 /api/status 心跳距今 < PAGE_ALIVE_SEC」。
     用户原话: 「如果这个时间终端没启动，11:50 的时候打开了，则不触发」。
  ② **严格在时点那一分钟内** —— 11:31:00~11:31:59 期间只要页面心跳还在就做。
     过了这一分钟没做 = 今天这次放弃，绝不在 11:50 补做。
  ③ **11:35 必须确认 11:31 已落地** —— 用户原话: 「先检测一下本地K线日期是否是最新，
     如果前一个拉取全市场行情落盘的任务没有完成，就不应该触发自动更新板块」。
     判据是「临时快照的 trade_date == 今天 && captured_ts 晚于今天 11:30」, 不看文件名。
  ④ **失败不重试** —— 用户 m07781 定案「失败的话不重试，但要有弹窗提醒成功与否
      并告诉我原因」。`RETRY_MAX = 0`，一次调用定生死。
  ⑤ **同日不重复** —— 每个任务在 `_done` 里记 {key: 'YYYY-MM-DD'}, 内存 + 落盘双份。

为什么不写进 daily/*.csv 的 11:31 轮:
  盘中写盘会毒化历史文件（11:31 的量只是半天量），收盘前数据还会变。
  11:31 只落 `intraday_snapshots.json`，15:31 那轮才真正写 CSV。
"""
import json
import os
import threading
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, 'auto_task_state.json')

PAGE_ALIVE_SEC = 30        # 页面心跳超过这么久没动静 = 终端关了
TICK_SEC = 20              # 每 20 秒醒一次 —— 60 秒容差 ⇒ 最多漏 20 秒也来得及
RETRY_GAP_SEC = 15         # (保留常量; 用户定案"失败不重试", 见 m07781)
RETRY_MAX = 0              # 用户 m07781 明确: 失败不重试, 只弹气泡报原因

# 四时点: (hhmm, 任务名, 说明, 要求的交易状态)
# 🔴 时点对了不等于该做业务 —— 用户 m07767 原话「终端监控时间当模块是交易日的
#   午休时间」⇒ 必须同时是交易日。周末/节假日 11:31 时钟照样走到，但那天拉的是
#   上一交易日的数据，白花 1 分还报个看不懂的错。state 由 server.get_trading_status()
#   注入（本模块不认识交易日历：ds.is_holiday() / 周末判定都在那边）。
TRADING_STATES = {
    1131: 'NOON_BREAK', 1135: 'NOON_BREAK',
    1531: 'POST_MARKET', 1535: 'POST_MARKET',
    # v2.331: 竞价打板只能落在 AUCTION 这一态 (9:25~9:30)。
    #   9:27:20 必在窗口内, 写它就是给周末/节假日兜底 —— 周末时钟照样走到 9:27。
    927: 'AUCTION',
}

# v2.331 (2026-11-17 用户 m24382): 元组加第 5 位 `sec` ——
#   sec=None  = 老口径, 「这一分钟内的任意一秒」都可以跑 (四时点原样);
#   sec=数字  = **精确到那一秒**, 过了一秒就当天放弃。
#   竞价打板定在 09:27:20: 09:25 撮合完成后 2 分钟, 腾讯快照里竞价量/额还是 09:25 的口径
#   (过了 09:30 就变成全天累计, 那 5 分钟一过竞价数据永久丢失 —— 这正是必须自动跑的原因)。
SLOTS = [
    (1131, None, 'snap', '拉全市场快照 (落临时快照)'),
    (1135, None, 'sector', '更新板块 (用临时快照)'),
    (1531, None, 'snap_eod', '拉全市场快照 + 写K线CSV'),
    (1535, None, 'sector_eod', '更新板块 (盘后)'),
    (927, 20, 'auction', '竞价打板 (09:27:20 整)'),
]

_LOCK = threading.RLock()
_done = {}          # {key: 'YYYY-MM-DD'}  最近一次成功的日子
_running = {}       # {key: {'since':ts,'name':str}}  正在跑的
_log = []           # 最近 N 条执行记录（给前端看）
# v2.152: 气泡事件队列 —— 每条带自增 seq，前端记 lastSeq 只要新的。
#   _log 是流水（会被覆盖成 20 条），front 拿它判断「哪条是新的」要 diff 文本，
#   同一秒多条会撞。这里序号单调递增，断线重连不重弹也不漏。
_EVQ = []           # [{seq, title, line, level, ts}]
_EVQ_MAX = 200
_EVQ_SEQ = [0]
_last_heartbeat = [0.0]   # 页面最后一次心跳时刻（用列表以避开 global）
_ui_lines = ['4']         # v2.160: 前端当前均线档位 '3'|'4'|'5'|'6'（心跳带上来）
_started = [False]
THREAD = [None]

# 🔴 绝不能在 auto_task 里 `from quant_terminal import server`：
#   server.py 是 `python -m quant_terminal.server` 起的，它的模块名是 `__main__`。
#   再 import 一次会在同一个进程里**第二次执行 server.py**，造出一整套独立的
#   全局状态（缓存/锁/线程），两套互不相干 —— 静默且难查。
#   ⇒ 由 server.py 在 start() 时把自己的函数传进来（依赖倒置）。
_RUNNERS = {'snap': None, 'sector': None, 'csv': None, 'state_fn': None,
            'scan_auction': None}   # v2.331: 竞价打板扫描 (server 注入)


def bind(snap_fn=None, sector_fn=None, csv_fn=None, state_fn=None,
         scan_auction_fn=None):
    """server.py 启动时注入执行体（幂等）

    snap_fn()      → 拉全市场快照并落临时快照
    csv_fn()       → 用临时快照写 daily/*.csv（15:31 那轮）
    sector_fn(eod) → 更新板块
    state_fn()     → 今天的交易状态字符串（WEEKEND/HOLIDAY/NOON_BREAK/POST_MARKET/…）
    scan_auction_fn() → v2.331: 跑一次竞价打板扫描 (09:27:20 自动触发)
    """
    if snap_fn is not None:
        _RUNNERS['snap'] = snap_fn
    if sector_fn is not None:
        _RUNNERS['sector'] = sector_fn
    if csv_fn is not None:
        _RUNNERS['csv'] = csv_fn
    if state_fn is not None:
        _RUNNERS['state_fn'] = state_fn
    if scan_auction_fn is not None:
        _RUNNERS['scan_auction'] = scan_auction_fn

LOG_MAX = 40


# ---------------------------------------------------------------- 状态
def _load_state():
    try:
        with open(STATE_FILE, encoding='utf-8') as fh:
            d = json.load(fh)
        if isinstance(d, dict):
            for k, v in (d.get('done') or {}).items():
                _done[k] = str(v)
            lg = d.get('log')
            if isinstance(lg, list):
                _log[:] = lg[-LOG_MAX:]
    except Exception:
        pass


def _save_state():
    try:
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump({'done': _done, 'log': _log[-LOG_MAX:]}, fh,
                      ensure_ascii=False, indent=1)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def touch_heartbeat(lines=''):
    """前端每次 /api/status 都会走到这里 —— 「终端开着」的唯一判据

    v2.160 (2026-10-20): 顺便收下前端**当前选中的均线档位** (#lines-sel / qt_lines)。
    自动任务刷「上级行业(90类)趋势」时要跟着这一档 —— 用户 m20094 定案：
    "根据前端面板板块均线档位手动选择的那一档来决定刷新哪一档"。
    ⚠️ 档位是**页面状态**，后端没有别的途径知道；心跳是唯一现成的通道。
    传空/非法值 = 保持上一次（开机时前端可能还没读 localStorage）。
    """
    _last_heartbeat[0] = time.time()
    ln = (lines or '').strip()
    if ln in ('3', '4', '5', '6'):
        _ui_lines[0] = ln


def ui_lines():
    """前端当前选的均线档位，默认 4（与前端 <span id="lines-sel" data-v="4"> 一致）"""
    return _ui_lines[0]


# v2.165 (2026-10-20): 自动任务的总开关 —— 就是数据源界面「通达信 tdx-aidata」那个勾选框。
#   用户 m22000 原话: 「不勾选就静默暂停 11:31, 11:35, 15:31, 15:35 这四个时间点的自动更新
#   K线数据和板块的行为」。因为四时点里前三步的**唯一**数据源就是通达信 (快照 + HqDate),
#   关掉它本来也做不成事, 挡在调度层比让它跑一遍再失败干净得多。
#   读的是 data_sources.json 的 online.tdx.enabled —— 与界面勾选框同一份, 不额外接线。
#   10 秒缓存: TICK_SEC=20, 勾/取消最迟 20 秒生效, 不必每轮都读盘。
_TDX_CHK = {'t0': 0.0, 'paused': False}
_TDX_CHK_SEC = 10.0


def tdx_paused():
    """数据源里通达信是否**未勾选启用** → True = 四时点全部静默暂停"""
    now = time.time()
    if now - _TDX_CHK['t0'] < _TDX_CHK_SEC:
        return _TDX_CHK['paused']
    paused = False
    try:
        from quant_terminal import tdx_fetch
        paused = not bool(tdx_fetch.load_cfg().get('enabled', True))
    except Exception:
        paused = False      # 读不出来就照常跑 —— 宁可按原行为, 也不要静默停摆
    _TDX_CHK['t0'] = now
    _TDX_CHK['paused'] = paused
    return paused



def page_alive():
    return (time.time() - _last_heartbeat[0]) < PAGE_ALIVE_SEC


def _log_add(level, msg):
    t = datetime.now().strftime('%m-%d %H:%M:%S')
    item = {'t': t, 'level': level, 'msg': msg, 'ts': time.time()}
    with _LOCK:
        _log.append(item)
        if len(_log) > LOG_MAX:
            del _log[:len(_log) - LOG_MAX]
    try:
        print(f'[auto_task] {t} {level} {msg}', flush=True)
    except Exception:
        pass
    return item


def _ev_add(title, line, level='info', round_key='', task_key=''):
    """发一条气泡事件。

    round_key: 同一次「轮」的多个步骤用同一个 key ⇒ 前端把它们合并进同一张气泡
               (11:31 拉快照 + 11:35 更新板块 = 一轮; 15:31/15:35 = 另一轮)。
               空字符串 = 不属于任何轮次, 各自独立。
    """
    with _LOCK:
        _EVQ_SEQ[0] += 1
        item = {'seq': _EVQ_SEQ[0], 'title': title, 'line': line,
                'level': level, 'round': round_key, 'key': task_key,
                'ts': time.time(),
                't': datetime.now().strftime('%H:%M:%S')}
        _EVQ.append(item)
        if len(_EVQ) > _EVQ_MAX:
            del _EVQ[:len(_EVQ) - _EVQ_MAX]
    return item


def _round_of(key):
    """哪个时点属于哪一轮 —— 同一轮的气泡会合并成一张

    noon: 11:31 拉快照 + 11:35 更新板块 (午休轮)
    eod : 15:31 拉快照写CSV + 15:35 更新板块 (收盘轮)
    """
    if key in ('snap', 'sector'):
        return 'noon'
    if key in ('snap_eod', 'sector_eod'):
        return 'eod'
    if key == 'auction':
        return 'auc'      # v2.331: 竞价单独一轮, 气泡不与别的合并
    return ''


def ev_since(after=0):
    """给 /api/auto_task/events 用：序号 > after 的事件"""
    try:
        after = int(after)
    except Exception:
        after = 0
    with _LOCK:
        items = [dict(x) for x in _EVQ if x['seq'] > after]
        cur = _EVQ_SEQ[0]
    return {'ok': True, 'seq': cur, 'items': items}


# ---------------------------------------------------------------- 任务体
def _task_snap(eod, dry=False):
    """拉全市场快照。eod=True 时同时写 CSV（15:31 那轮）

    dry=True = 干跑：不碰通达信、**不写临时快照**（验收脚本专用，零积分）。
    绝不能把干跑的假结果落进 intraday_snapshots.json —— 那会把真快照冲掉。
    """
    from quant_terminal import tdx_fetch, intraday
    if eod:
        # 写 CSV 只在收盘后 —— 时钟这道关与 HqDate 校验是**双重**保证:
        #   HqDate 只有日期没有时分秒, 11:31 也等于今天, 单靠它区分不了上午/收盘。
        now = datetime.now()
        if now.hour * 100 + now.minute < 1500:
            return {'ok': False, 'error': f'未到收盘 (现在 {now:%H:%M}), 拒绝写 CSV'}
    res = tdx_fetch.do_snapshot(verbose=False, dry=dry,
                                who=('auto:snap_eod' if eod else 'auto:snap'),
                                # v2.169 (2026-10-21) 用户 m00352: 市值只有收盘写 CSV 用得上
                                #   （intraday.caps/bars 唯一调用方就是下面的 fn_csv），
                                #   盘中那轮拉回来无人读 ⇒ 只有 eod 才拉。
                                with_mcap=eod)
    if not res.get('ok'):
        return {'ok': False, 'error': res.get('error') or '拉取失败'}
    if res.get('dry'):
        return {'ok': True, 'dry': True, 'count': res.get('count'),
                'note': '干跑：未调用通达信，未写临时快照'}
    td = res.get('trade_date') or ''
    today = datetime.now().strftime('%Y-%m-%d')
    if td != today:
        # 数据不是今天的 ⇒ 不落盘（宁可这次失败，也不能把昨天数据当今天用）
        return {'ok': False,
                'error': f'数据日期 {td or "空"} ≠ 今天 {today}，已丢弃（不计入成功）'}
    # 🔴 v2.193 (2026-10-23) 用户要求: 除权安全阀一旦触发（判出 >500 只除权 ⇒ 整体作废），
    #   必须让他在「更新 K 线」时报错看见 —— 别让历史价格被悄悄乘歪却显示"完成"。
    _drop = int(res.get('exrights_dropped') or 0)
    if _drop > 500:
        return {'ok': False,
                'error': (f'⚠ 除权安全阀触发：本轮判出 {_drop} 只除权（阈值 500），'
                          f'已全部作废、历史 CSV 未被改动。多半是快照昨收口径不对'
                          f'（跨零点跑 / 同一轮重复跑）。证据见 quant_terminal/fuyao_fetch.log'),
                'exrights_dropped': _drop, 'trade_date': td}
    doc = intraday.save(res, source='tdx')
    out = {'ok': True, 'count': doc.get('count'), 'trade_date': td,
           'suspects': doc.get('suspects'), 'exrights': len(doc.get('exrights') or {})}
    if eod:
        try:
            fn_csv = _RUNNERS.get('csv')
            if fn_csv is None:
                out['csv'] = {'ok': False, 'error': 'server 没注入写CSV函数'}
            else:
                out['csv'] = fn_csv()
        except Exception as e:
            out['csv'] = {'ok': False, 'error': str(e)[:200]}
    return out


def _task_auction(eod=False):
    """v2.331 (用户 m24382): 跑一次「竞价打板」扫描 —— 与手点按钮同一条路

    为什么不在这里自己调 auction_scan.scan():
      扫描除了算结果, 还要 _cache_scan_slot 落盘槽、save_scan 归档、推 SSE 进度、
      更新 SCAN_STATE —— 那一整套都在 server.run_scan_async 里。自己调 scan()
      只会算出一份没人看得见的结果。所以由 server 注入一个「起后台扫描线程」的函数。

    返回约定与其它 _task_* 一致: {'ok': True/False, ...}。
    注意这里**只负责发起**, 扫描本身在后台线程里跑 (约 1.5~20 秒), 不等它完成 ——
    09:27:20 发起到 09:30 还有 160 秒, 足够跑完; 而且竞价口径只取决于 09:25 的撮合,
    跑得晚一点不影响数据本身。
    """
    fn = _RUNNERS.get('scan_auction')
    if fn is None:
        return {'ok': False, 'error': 'server 没注入竞价扫描函数'}
    return fn()


def _task_sector(eod):
    """更新板块 —— 走 server 注入的刷新函数（与界面「更新板块」按钮同一条路）"""
    fn = _RUNNERS.get('sector')
    if fn is None:
        return {'ok': False, 'error': 'server 没注入板块刷新函数'}
    return fn(eod=eod)


# ---------------------------------------------------------------- 调度
def _slot_now():
    """当前时刻落在哪个时点窗口里 → (hhmm, 分钟内已过秒数) 或 (None, 0)

    v2.331: 支持「精确到秒」的时点 (SLOTS 第 2 位 sec)。
      sec is None → 老口径, 整分钟内任意一秒都算命中;
      sec 是数字   → 只有 now.second >= sec 且还没做过才算 (见 _tick 里的 hhmm + sec 判定)。
        这里仍返回该 hhmm, 让 _tick 有机会按 sec 再筛一道; 秒数由调用方自己比。
    """
    now = datetime.now()
    t = now.hour * 100 + now.minute
    for hhmm, _sec, _key, _desc in SLOTS:
        if hhmm == t:
            return hhmm, now.second
    return None, 0


def _slot_sec(hhmm):
    """该时点要求的秒 (None = 整分钟窗口) —— v2.331"""
    for h, sec, _key, _desc in SLOTS:
        if h == hhmm:
            return sec
    return None


def _already_done(key, today):
    return _done.get(key) == today


def _run_one(hhmm, key, name, fn, eod, today):
    """跑一个任务。必须在独立线程里跑，不能阻塞调度器。

    ⚠️ 失败**不重试** —— 用户 m07781 定案「失败的话不重试，但要有弹窗提醒成功与否
    并告诉我原因」。原来这里写的是重试一次，与定案不符。
    """
    _running[key] = {'since': time.time(), 'name': name}
    try:
        _log_add('info', f'▶ {name} 开始')
        t0 = time.time()
        try:
            res = fn(eod)
        except Exception as e:
            res = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:200]}'}
        took = round(time.time() - t0, 1)
        if res and res.get('ok'):
            with _LOCK:
                _done[key] = today
            extra = ''
            if res.get('count') is not None:
                extra = f" {res['count']}只"
                if res.get('suspects'):
                    extra += f" 可疑{res['suspects']}"
                if res.get('exrights'):
                    extra += f" 除权{res['exrights']}"
            if res.get('csv') is not None:
                c = res['csv']
                extra += (' CSV✓' if c.get('ok') else f" CSV✗({c.get('error', '')[:40]})")
                # v2.249 (2026-11-06 用户 m21803): 顺手建的新股要让用户在气泡里看得见
                _nsx = c.get('new_stock') or {}
                if _nsx.get('created'):
                    extra += f" 新建{len(_nsx['created'])}只"
                elif _nsx.get('failed'):
                    extra += f" 新股✗{_nsx['failed']}只"
            # v2.160: 板块任务顺带刷了上级行业 —— 成没成要让用户从气泡里看得见
            _pc = res.get('parent')
            if isinstance(_pc, dict):
                extra += (f" 上级{_pc.get('lines','')}线✓{_pc.get('n',0)}个"
                          if _pc.get('ok')
                          else f" 上级✗{(_pc.get('error') or '')[:30]}")
            _log_add('ok', f'✓ {name} 完成{extra} ({took}s)')
            # v2.152 气泡: 成功带用量与耗时
            _ev_add(name, f'✓ {name} 成功{extra} · 用时 {took}s', 'ok',
                     _round_of(key), key)
            _save_state()
        else:
            err = (res or {}).get('error') or '未知错误'
            _log_add('err', f'✗ {name} 失败: {err} ({took}s)')
            # v2.152 气泡: 失败必须带原因（用户 m07781 定案）
            _ev_add(name, f'✗ {name} 失败：{err}', 'err', _round_of(key), key)
            _save_state()
    finally:
        _running.pop(key, None)


def _auction_late_window():
    """v2.333 (2026-11-17 用户 m24942): 竞价「漏跑兜底」时间窗 —— 09:28:00 ~ 09:28:59。

    背景: 原来只有 09:27:00~09:27:59 这一分钟命中 _slot_now, 加上 sec>=20 的秒判,
    **实际只有 40 秒**。这 40 秒里终端页面没开 (page_alive 为假) 的话, 当天就再也不会
    自动跑, 只能手动补; 而手动补出来是 locked 档 (走 09:30 那根 bar 还原), 实测换手率
    中位差 31 倍、板块强度差 242%、BIAS 差 50% —— 统计价值完全不同。

    为什么 09:28 还能跑出 live: auction_scan.in_auction_window() 的 live 口径是
    09:25:00 ~ 09:29:59 (T_OPEN=(9,25) / T_CUT=(9,30)), 09:28 采到的快照成交量仍然是
    竞价量, 与 09:27 跑出来的**同一口径**。

    为什么上限卡 09:29:00 而不是 09:29:59: 扫描本身要 1.5~7 秒, 太贴近 09:30 才发起的话
    快照可能落到 09:30:0x 才返回, 成交量里混进连续竞价头几秒, 数值悄悄偏大且**不报错**。
    留 30 秒余量最稳。

    一天只补一次 (见 _tick 里的 '_auc_late' 标记) —— 失败也不再连试, 遵守 m07781
    「失败不重试」的定案。
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 10000 + now.minute * 100 + now.second
    return 92800 <= t < 92900


def _tick():
    """一次心跳：检查是否到了时点，到了就起线程（不阻塞下一轮）"""
    today = datetime.now().strftime('%Y-%m-%d')
    hhmm, _sec = _slot_now()
    _late_auc = False
    if hhmm is None:
        # v2.333 (用户 m24942): 09:27 那一分钟没跑到 ⇒ 09:28 补跑一次(仍是 live 口径)。
        #   只补一次: '_auc_late' 标记一置就不再试, 遵守「失败不重试」。
        if _auction_late_window() and not _already_done('_auc_late', today):
            hhmm = 927
            _late_auc = True
        else:
            return

    # v2.165 (2026-10-20 用户 m22000 定案): 数据源里「通达信」取消勾选 = 四时点**静默暂停**。
    #   取消勾选原本只是「不发通达信请求」, 但实测留下两个尾巴:
    #     ① 11:31 / 15:31 失败后不记 _done ⇒ 同一分钟内每 20 秒重试一次、各弹 3 条红气泡;
    #     ② 若当天 11:31 已拉成功、之后才取消勾选, 11:35 仍会照跑 (它只看临时快照日期)。
    #   用户要的是「静默暂停」⇒ 在闸门**最前面**一次性挡掉四个时点: 不拉、不写CSV、不更新板块、
    #   不弹气泡, 只留一行灰日志 (每个时点每天一次)。
    #   手动按钮 (⚡快照更新 / 🔄更新板块) 不受影响 —— 那是用户主动点的。
    #   🔴 v2.331: 竞价打板**不属于**这四个时点 —— 它走腾讯快照 + 本地 CSV, 与通达信
    #      一点关系都没有 (auction_scan.py 全文不提 tdx)。所以这里显式放过 'auction',
    #      否则用户关掉通达信竞价就再也不自动跑了, 而竞价数据过了 9:30 永久丢失。
    if tdx_paused() and hhmm != 927:
        _key_off = '_warn_tdxoff_' + str(hhmm)
        if not _already_done(_key_off, today):
            _log_add('info', '⏸ %02d:%02d 已按设置暂停 (数据源 → 通达信 未勾选「启用」)'
                             % (hhmm // 100, hhmm % 100))
            with _LOCK:
                _done[_key_off] = today
            _save_state()
        return
    # 🔴 时点对 + 交易日对，两件事都要满足。周末/节假日 11:31 时钟照样走，
    #   那天拉回来的是上一交易日的快照，会白花积分并报「数据日期≠今天」。
    want = TRADING_STATES.get(hhmm)
    if want:
        fn_state = _RUNNERS.get('state_fn')
        if fn_state is None:
            return                  # server 还没注入 ⇒ 宁可不跑
        try:
            cur = (fn_state() or '').strip()
        except Exception:
            return
        if cur != want:
            key_w = '_warn_state_' + str(hhmm)
            if not _already_done(key_w, today):
                _log_add('info', f'⏭ {hhmm // 100:02d}:{hhmm % 100:02d} 时点到了但今天状态是 '
                                 f'{cur or "?"}（需要 {want}），不执行')
                with _LOCK:
                    _done[key_w] = today
                _save_state()
            return
    # v2.331 (用户 m24382): 竞价打板 09:27:20 —— 与上面四个时点同一条闸门, 只是多一道秒判。
    #   秒判用 `now.second >= sec`(不是 ==): TICK_SEC=20 意味着醒来的秒数不可控,
    #   卡 == 会大概率错过; 而 >= 只要求「今天这一分钟里 ≥20 秒时跑过一次」,
    #   加上 _already_done 保证一天只跑一次, 语义正好是「9:27:20 之后马上跑」。
    _want_sec = _slot_sec(hhmm)
    if _want_sec is not None and datetime.now().second < _want_sec and not _late_auc:
        # 09:27 正点跑要等 sec>=20; 09:28 的补跑按定义已经过了 09:27:20, 不再等。
        return                      # 还没到那一秒 ⇒ 下一轮再看（同分钟内）
    for slot_hhmm, key, name, eod in [
            (1131, 'snap', '拉全市场快照', False),
            (1135, 'sector', '更新板块', False),
            (1531, 'snap_eod', '拉全市场快照+写CSV', True),
            (1535, 'sector_eod', '更新板块(盘后)', True),
            (927, 'auction', '竞价打板', False)]:
        if slot_hhmm != hhmm:
            continue
        if _already_done(key, today):
            return
        if key in _running:
            return
        if not page_alive():
            return                  # 终端没开 ⇒ 不触发（下一秒再看）
        # 11:35 / 15:35 的板块更新必须先确认快照已落地（用户 m07781 明确要求）
        if key in ('sector', 'sector_eod'):
            from quant_terminal import intraday
            st = intraday.status()
            if not st.get('ok') or st.get('trade_date') != today:
                # 快照还没好 ⇒ 本次不跑（前置任务失败时本任务就该不跑）
                if not _already_done('_warn_' + key, today):
                    _log_add('warn', f'⚠ {name} 跳过：临时快照不是今天的 '
                                     f"({st.get('trade_date') or '无'})")
                    with _LOCK:
                        _done['_warn_' + key] = today
                return
        # 快照任务在模块内有完整实现 (_task_snap)；板块任务必须由 server 注入
        # (`_task_sector` 本身只是转发器)。这里允许 bind 覆盖快照实现，唯一的用途
        # 就是单测——生产路径 server.py 不传 snap_fn，走默认实现。
        if key.startswith('snap'):
            fn = _RUNNERS.get('snap') or _task_snap
        elif key == 'auction':      # v2.331: 竞价打板 (server 注入「起扫描线程」)
            fn = _task_auction
        else:
            fn = _task_sector
        # v2.333: 09:28 的补跑 —— ①一天只给一次(先落标记再起线程, 失败也不连试)
        #         ②气泡和日志里标出来, 免得跟 09:27:20 正点跑的分不清。
        if _late_auc and key == 'auction':
            with _LOCK:
                _done['_auc_late'] = today
            _save_state()
            name = '竞价打板 (09:28 补跑)'
        th = threading.Thread(target=_run_one,
                              args=(hhmm, key, name, fn, eod, today),
                              name=f'auto-{key}', daemon=True)
        th.start()
        return


def _loop():
    _load_state()
    _log_add('info', '自动任务调度器已启动 (09:27:20 竞价, 09:28 可补跑 / 11:31 / 11:35 / 15:31 / 15:35)')
    while True:
        try:
            _tick()
        except Exception as e:
            _log_add('err', f'调度异常: {type(e).__name__}: {str(e)[:150]}')
        time.sleep(TICK_SEC)


def start():
    """由 server.py 在启动时调用一次（幂等）"""
    if os.name != 'nt':
        return None
    if os.environ.get('QT_NO_AUTO_TASK', '').lower() in ('1', 'true', 'yes'):
        print('[auto_task] QT_NO_AUTO_TASK 已设置 ⇒ 不启用')
        return None
    with _LOCK:
        if _started[0]:
            return THREAD[0]
        _started[0] = True
        _load_state()
        th = threading.Thread(target=_loop, name='auto-task', daemon=True)
        th.start()
        THREAD[0] = th
    return th


# ---------------------------------------------------------------- 对外查询
def status():
    """给 /api/auto_task 用"""
    today = datetime.now().strftime('%Y-%m-%d')
    with _LOCK:
        slots = []
        for hhmm, key, name, eod in [
                (927, 'auction', '竞价打板', False),
                (1131, 'snap', '拉全市场快照', False),
                (1135, 'sector', '更新板块', False),
                (1531, 'snap_eod', '拉全市场快照+写CSV', True),
                (1535, 'sector_eod', '更新板块(盘后)', True)]:
            _sc = _slot_sec(hhmm)
            slots.append({
                'hhmm': (f'{hhmm // 100:02d}:{hhmm % 100:02d}:{_sc:02d}'
                         if _sc is not None else
                         f'{hhmm // 100:02d}:{hhmm % 100:02d}'),
                'key': key, 'name': name,
                'done_today': _done.get(key) == today,
                'running': key in _running,
                'done_date': _done.get(key, ''),
            })
        log = list(_log[-20:])
    return {
        'ok': True,
        'enabled': bool(THREAD[0]),
        'page_alive': page_alive(),
        'heartbeat_age': round(time.time() - _last_heartbeat[0], 1),
        # v2.160: 前端当前均线档位 —— 「更新板块」时会跟着刷这一档的上级行业趋势。
        #   暴露出来是为了**可验证**: 用户能看见「下次会自动刷哪一档」,
        #   不然档位收错了也没人知道 (静默后果 = 刷错档的上级行业)。
        'ui_lines': ui_lines(),
        # v2.165: 直接暴露「自动任务是否被通达信勾选状态停掉」—— 验收和用户看状态
        #   都不必去翻 data_sources.json, 也不必等一个时点到。
        'tdx_paused': tdx_paused(),
        'tick_sec': TICK_SEC,
        'slots': slots,
        'log': log,
    }


def run_now(key, dry=False):
    """手动立即执行某个时点 (调试/验收用; 不写 _done, 不占今日名额)

    key: 'snap' | 'sector' | 'snap_eod' | 'sector_eod' | 'auction'
    dry: 只对 snap / snap_eod 有意义 —— 走假结果，**零积分**（验收专用）
    """
    table = {'snap': (False, _task_snap), 'sector': (False, _task_sector),
             'snap_eod': (True, _task_snap), 'sector_eod': (True, _task_sector),
             'auction': (False, _task_auction)}   # v2.331
    if key not in table:
        return {'ok': False, 'error': f'未知时点: {key}'}
    eod, fn = table[key]
    name = {'snap': '拉全市场快照', 'sector': '更新板块',
            'snap_eod': '拉全市场快照+写CSV', 'sector_eod': '更新板块(盘后)',
            'auction': '竞价打板'}[key]
    _log_add('info', f'▶ [手动] {name} 开始' + (' (干跑·零积分)' if dry else ''))
    t0 = time.time()
    try:
        if dry and fn is _task_snap:
            res = fn(eod, dry=True)
        else:
            res = fn(eod) if fn in (_task_snap, _task_sector) else fn()
        if not isinstance(res, dict):
            res = {'ok': bool(res), 'result': str(res)[:200]}
    except Exception as e:
        res = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:200]}'}
    el = time.time() - t0
    # v2.158: 手动触发也带 key —— 前端靠它决定刷哪个面板。
    #   round_key 传空: 手动执行的各时点互相独立, 不该并进同一张气泡。
    if res.get('ok'):
        _log_add('ok', f'✓ [手动] {name} 完成 ({el:.1f}s)')
        _ev_add(name, f'✓ [手动] {name} 成功 · 用时 {el:.1f}s', 'ok', '', key)
    else:
        _log_add('err', f'✗ [手动] {name} 失败: {res.get("error") or "未知"} ({el:.1f}s)')
        _ev_add(name, f'✗ [手动] {name} 失败：{res.get("error") or "未知"}', 'err', '', key)
    res['elapsed'] = round(el, 2)
    res['key'] = key
    return res


def reset_today():
    """手动清除今日已完成标记（调试用）"""
    today = datetime.now().strftime('%Y-%m-%d')
    with _LOCK:
        for k in list(_done):
            if _done[k] == today:
                del _done[k]
    _save_state()
    return status()
