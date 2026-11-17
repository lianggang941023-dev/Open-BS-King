# -*- coding: utf-8 -*-
"""两市成交额分钟级落盘 + 全天成交预测 (v2.64 2026-09-10)

北交所 (方案A, 2026-09-10 定案):
  同花顺的「总成交额」是沪深北三市, 我们是沪深两市 — 已实测对齐:
    9/10 沪深 16471.5 + 北证50 153.7 = 16625.2 vs 同花顺 16623 (+0.013%)
    9/9  沪深 18556.1 + 北证50 175.8 = 18731.9 vs 同花顺 18730 (+0.010%)
  差额约 150~200 亿/天且逐日变化, 不是常数 → 边界档位 (±100/±500/±1000/±1500/±2000)
  存在被翻转的可能, 故需要对齐。但北交所没有分钟级历史 (腾讯分钟行只有 3 列, 无成交额),
  唯一可得的只有实时快照里当天的累计值 → 只能从今天起往后攒。

  方案A = 后台先攒, 攒够再切:
    · 每个交易日盘后悄悄把北证50 全天成交额记进 days[iso]['bj_yi'], 界面暂时不动
    · 攒满 WINDOW+1 = 6 天(全部日线都带 bj_yi) → store['bj_mode']=True, 一次性切成三市口径
    · 切换是单向的, 永不回退; 全程没有"新旧口径混着"的中间态
    · 切换后口径折算: 该日分钟序列 × (1 + bj_yi / 沪深当日累计)
      过去日 → 精确复现该日三市全天值; 当日 → 用快照实时累计, 当日累计值精确
      盘中形态按沪深曲线等比摊 (北交所占比 ~1%, 形态误差对总量的影响 < 0.2%)

  缺数据兜底 (用户认可): 某天没采到 → 用前一个可用值顺延, 而不是当 0。
    当 0 会在 Δ 上砸出 ±160 亿的假跳变, 直接翻转档位; 顺延的误差只有北交所日间波动
    (约 ±20 亿), 小一个数量级。快照自带日期+时间, 故盘后、次日 9:15 前、周末都能补记,
    只有"15:00 到次日 9:15 之间完全没开终端"的那天会走顺延。

数据源 (北交所): 腾讯 qt.gtimg.cn/q=bj899050 主 (f[30]=时间戳 f[37]=成交额万元),
  新浪 hq.sinajs.cn/list=bj899050 兜底 (f[9]=成交额元 f[30]=日期 f[31]=时间)。两者实测一致。

需求 (用户 2026-09-10 定案):
  · 顶栏「成交额」改为「成交预测:±n亿」= 预计全天 − 昨日全天
  · 分级口径改为「较昨日绝对差值」(平量/微/温和/显著/极大/爆量·无量)
  · 悬停浮窗 5 行: 当前累计 / 较昨日此时 / 预计全天 / 较前五日均额 / 昨日全天
  · 历史落盘, 滚动保留 5 个已收盘交易日, 避免盘中反复拉取被限流
  · 盘中每分钟拉一次当日分钟线并直接落盘; 终端没开则启动后补齐 (minute 端点天然返回当日全部已过分钟)

为什么必须换掉原算法: 原「已过时间比例线性外推」在早盘严重高估 —
A股成交额前 30 分钟占全天约 34%, 2026-09-10 10:00 线性外推得 46000亿,
实际 16471亿 (+179%), 评级从「无量」翻转成「爆量」, 情绪分虚高 12 分。
本模块改用自校准外推: 预计全天 = 今日累计 ÷ (昨日同期累计 ÷ 昨日全天),
同点位误差 -0.6%。

数据源 (腾讯, 主域优先子域兜底, 与 fqkline 同域安全策略):
  · minute/query?code=sh000001 → 当日全部分钟 (含累计成交额) — 盘中每分钟用
  · day/query?code=sh000001    → 最近 5 个交易日分钟          — 启动 / 盘后整窗刷新用
  行格式: '0930 3939.09 4588920 6465487582.70' = 时间 价格 累计成交量(手) 累计成交额(元)

口径: 两市合计 = sh000001 + sz399001, 单位亿元。
  已验证 2026-09-10 收盘 7796.7 + 8674.3 = 16471亿, 与新浪实时一致。

完整性判定: 当日分钟序列含 '1500' → complete。盘后靠它收敛, 不需要额外的状态开关,
  「盘后只在首次启动时检查数据是否最新」= complete 为真就不再拉。

时间口径: phase() 与 server.get_trading_status() 同口径 (v2.24 定案「所有时间判断以时钟模块为准」)。
  v2.118 (2026-09-25): 加第 7 态 HOLIDAY; 节假日判定改为**共用** datasource.is_holiday()
  (原先 server/turnover/engine 各写一份拷贝) —— 口径从此靠"同一份代码", 而不是靠"记得同步改三处"。
  server 侧调用 ensure(phase=...) 时传入时钟模块的权威值; 本模块的副本只作 engine 侧兜底,
  且 engine 只用它判断「休市 / 8点前显示结算值」, 分钟级偏差无影响。

落盘: quant_terminal/turnover_data.json (原子写 tmp + os.replace, 单文件覆盖)
冻结便携版: 打包时 TURNOVER_FILE 需同 MA_BASELINE_FILE 一并改锚 ds._app_root()
"""
import os, json, time, threading
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TURNOVER_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'turnover_data.json')

CODES = ('sh000001', 'sz399001')     # 上证 + 深证成指 (两市合计口径)
BJ_CODE = 'bj899050'                  # 北证50 (北交所总成交额载体, 同 sz399001 之于深市)
SCHEMA = 1
WINDOW = 5                            # 保留 5 个已收盘交易日 (+ 当日, 共 6)
HOSTS = ['ifzq.gtimg.cn', 'web.ifzq.gtimg.cn']
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/120.0 Safari/537.36')
HDR = {'User-Agent': UA, 'Referer': 'https://gu.qq.com/'}
SINA_HDR = {'User-Agent': UA, 'Referer': 'https://finance.sina.com.cn'}

_LOCK = threading.RLock()
_host_cur = [0]
# 进程内运行态: ts/phase=上次拉取, busy=后台拉取中, miss=连续拿不到当日数据的次数(休市退避)
_RT = {'ts': 0.0, 'phase': '', 'busy': False, 'miss': 0}
_CACHE = {'mtime': -1.0, 'data': None}
_PHASE_IV = {'MORNING': 55, 'AFTERNOON': 55, 'NOON_BREAK': 600,
             'AUCTION': 120,   # v2.300 9:25~9:30 竞价中: 成交额基本不动, 不用 55 秒一拉
             'PRE_MARKET': 600, 'POST_MARKET': 180, 'WEEKEND': 0,
             'HOLIDAY': 1800}   # v2.118 休市: 一整天都不会有当日数据, 半小时探一次就够
# 北交所采集运行态: live={iso, yi} 最近一次快照值(三市口径下当日折算要用), 其余为节流
_BJ = {'ts': 0.0, 'busy': False, 'live': {}}
_BJ_IV = {'MORNING': 60, 'AFTERNOON': 60, 'NOON_BREAK': 300,
          'AUCTION': 120,   # v2.300
          'PRE_MARKET': 300, 'POST_MARKET': 180, 'WEEKEND': 1800,
          'HOLIDAY': 1800}   # v2.118 休市同周末 (可补记上一个交易日的北交所值)


# ---------------------------------------------------------------- 时间 / 请求
def _holiday_safe():
    """节假日判定 —— 借 datasource 的权威口径 (一处定义, 见 ds.is_holiday)。
    ⚠ 延迟导入 + 全兜底: 本模块历来**零依赖** (自拉 HTTP, 不 import datasource),
      这里只为一个判断开后门; 拿不到就当"不是节假日" → 退回旧行为。"""
    try:
        from quant_terminal import datasource as _ds
        return _ds.is_holiday()
    except Exception:
        return False


def phase(now=None):
    """八态时段 — 与 server.get_trading_status() 同口径 (v2.24 定案)

    v2.118 (2026-09-25): 加第 7 态 HOLIDAY (工作日但休市)。三处口径**不再是三份拷贝**:
    节假日判定统一走 datasource.is_holiday(), 本函数只保留钟点部分。
    v2.300 (2026-11-13 用户 m15464): 加第 8 态 AUCTION (9:25~9:30 集合竞价),
      PRE_MARKET 从 t<930 改为 t<925 —— 与 server.get_trading_status() 严格同步。"""
    now = now or datetime.now()
    if now.weekday() >= 5: return 'WEEKEND'
    if _holiday_safe(): return 'HOLIDAY'      # v2.118
    t = now.hour * 100 + now.minute
    if t < 925: return 'PRE_MARKET'
    if t < 930: return 'AUCTION'              # v2.300
    if t <= 1130: return 'MORNING'
    if t < 1300: return 'NOON_BREAK'
    if t <= 1500: return 'AFTERNOON'
    return 'POST_MARKET'


def _req_get(url, timeout=8, headers=None):
    """HTTP GET —— **先直连, 失败再走系统代理** (v2.234 2026-10-27, 同 engine._req_get)
    本模块只打腾讯/新浪(境内), 直连更快更稳; 退回代理留作退路。"""
    import requests
    try:
        return requests.get(url, timeout=timeout, headers=headers,
                            proxies={'http': None, 'https': None})
    except Exception:
        return requests.get(url, timeout=timeout, headers=headers)


def _qq(path):
    """腾讯行情接口, 域名自动切换 (主域↔子域风控轮换, 成功一次记住)"""
    hosts = HOSTS[_host_cur[0]:] + HOSTS[:_host_cur[0]]
    for h in hosts:
        try:
            r = _req_get(f'https://{h}{path}', timeout=8, headers=HDR)
            if r.status_code != 200: continue
            j = r.json()
            if j.get('data'):
                _host_cur[0] = HOSTS.index(h)
                return j
        except Exception:
            continue
    return None


# ---------------------------------------------------------------- 解析
def _iso(d):
    """'20260910' → '2026-09-10'"""
    return f'{d[:4]}-{d[4:6]}-{d[6:]}'


def _parse(lines):
    """分钟行 → {HHMM: 亿元累计成交额}"""
    out = {}
    for ln in lines or []:
        p = str(ln).split()
        if len(p) < 4: continue
        k = p[0]
        if len(k) != 4 or not k.isdigit(): continue
        try:
            out[k] = float(p[3]) / 1e8
        except (ValueError, TypeError):
            continue
    return out


def _fetch_day(code):
    """day/query → {iso: {HHMM: 亿元}}  (最近 5 个交易日)"""
    j = _qq(f'/appstock/app/day/query?code={code}')
    if not j: return {}
    lst = ((j.get('data') or {}).get(code) or {}).get('data') or []
    out = {}
    for it in lst:
        if not isinstance(it, dict): continue
        d = str(it.get('date') or '')
        if len(d) != 8 or not d.isdigit(): continue
        m = _parse(it.get('data'))
        if m: out[_iso(d)] = m
    return out


def _fetch_minute(code):
    """minute/query → (iso, {HHMM: 亿元})  (当日全部已过分钟)"""
    j = _qq(f'/appstock/app/minute/query?code={code}')
    if not j: return '', {}
    d = ((j.get('data') or {}).get(code) or {}).get('data') or {}
    dt = str(d.get('date') or '')
    if len(dt) != 8 or not dt.isdigit(): return '', {}
    return _iso(dt), _parse(d.get('data'))


def _add(a, b):
    """两个 {HHMM: 亿元} 逐分钟相加 (两市合计)"""
    out = dict(a)
    for k, v in b.items():
        out[k] = out.get(k, 0.0) + v
    return out


# ---------------------------------------------------------------- 北交所
def _fetch_bj():
    """北证50 快照 → (iso, 亿元, HHMMSS)。腾讯主, 新浪兜底, 都拿不到返回 ('', None, '')。

    快照自带日期与时间, 这是它能跨时段补记的唯一依据 (见 capture_bj)。
    """
    try:                                       # 腾讯 qt: [30]=20260910153342  [37]=成交额(万元)
        r = _req_get(f'https://qt.gtimg.cn/q={BJ_CODE}', timeout=6, headers=HDR)
        f = r.content.decode('gbk', 'replace').split('="', 1)[1].rstrip('";\r\n').split('~')
        ts, amt = str(f[30]), float(f[37])
        if len(ts) >= 14 and ts[:14].isdigit() and amt > 0:
            return _iso(ts[:8]), amt / 1e4, ts[8:14]
    except Exception:
        pass
    try:                                       # 新浪: [9]=成交额(元)  [30]=日期  [31]=时间
        r = _req_get(f'https://hq.sinajs.cn/list={BJ_CODE}', timeout=6, headers=SINA_HDR)
        g = r.content.decode('gbk', 'replace').split('="', 1)[1].rstrip('";\r\n').split(',')
        amt, d, t = float(g[9]), str(g[30]), str(g[31]).replace(':', '')
        if len(d) == 10 and len(t) >= 6 and amt > 0:
            return d, amt / 1e8, t[:6]
    except Exception:
        pass
    return '', None, ''


# ---------------------------------------------------------------- 落盘
def _now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _load():
    """读盘 (mtime 缓存, 免每 5 秒解析 JSON)。返回的 dict 是共享对象, 只读用。"""
    try:
        mt = os.path.getmtime(TURNOVER_FILE)
    except OSError:
        return {'schema': SCHEMA, 'updated': '', 'days': {}}
    if _CACHE['data'] is not None and mt == _CACHE['mtime']:
        return _CACHE['data']
    try:
        with open(TURNOVER_FILE, encoding='utf-8') as fh:
            obj = json.load(fh)
        if not isinstance(obj, dict) or not isinstance(obj.get('days'), dict):
            raise ValueError('bad schema')
    except Exception:
        obj = {'schema': SCHEMA, 'updated': '', 'days': {}}
    _CACHE.update(mtime=mt, data=obj)
    return obj


def _save(store):
    store['schema'] = SCHEMA
    store['updated'] = _now_str()
    tmp = TURNOVER_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(store, fh, ensure_ascii=False, separators=(',', ':'))
    os.replace(tmp, TURNOVER_FILE)          # Windows 下 os.replace 原子覆盖
    try:
        _CACHE.update(mtime=os.path.getmtime(TURNOVER_FILE), data=store)
    except OSError:
        _CACHE.update(mtime=-1.0, data=None)


def _merge_into(store, got):
    """got: {iso: {HHMM: 亿元}} → 并入 store, 按日期裁剪到 WINDOW+1 天"""
    days = store.setdefault('days', {})
    for iso, mins in (got or {}).items():
        if not mins: continue
        rec = days.get(iso)
        if not isinstance(rec, dict) or not isinstance(rec.get('min'), dict):
            rec = {'min': {}}
        for k, v in mins.items():
            rec['min'][k] = round(v, 1)
        rec['complete'] = '1500' in rec['min']
        rec['updated'] = _now_str()
        days[iso] = rec
    for k in sorted(days.keys())[:-WINDOW - 1]:
        days.pop(k, None)
    _flip_check(store)
    return days


def _flip_check(store):
    """攒够 WINDOW+1 天北交所数据 → 一次性切成三市口径。单向, 永不回退。

    为什么必须"一个窗口全有"才切: 只要有一天缺, 那天的比较就成了「三市 vs 沪深」,
    差着 150~200 亿, Δ 上是个假跳变。等整窗齐了再切, 就永远没有混口径的时刻。
    """
    if store.get('bj_mode'):
        return False
    days = store.get('days') or {}
    keys = sorted(days)
    if len(keys) < WINDOW + 1:
        return False
    if any((days.get(d) or {}).get('bj_yi') is None for d in keys):
        return False
    store['bj_mode'] = True
    return True


# ---------------------------------------------------------------- 拉取
def _worker(full):
    try:
        got = {}
        if full:
            for c in CODES:
                for iso, m in _fetch_day(c).items():
                    got[iso] = _add(got.get(iso, {}), m)
        else:
            for c in CODES:
                iso, m = _fetch_minute(c)
                if iso and m:
                    got[iso] = _add(got.get(iso, {}), m)
        if not got: return
        with _LOCK:
            store = _load()
            _merge_into(store, got)
            _save(store)
            today = datetime.now().strftime('%Y-%m-%d')
            has_today = bool((store['days'].get(today) or {}).get('min'))
        _RT['miss'] = 0 if has_today else min(_RT['miss'] + 1, 6)
    except Exception:
        pass
    finally:
        _RT['busy'] = False


def ensure(phase_now=None):
    """节流触发一次后台拉取。返回是否真的发起了拉取 (前端零改动: 挂在 /api/sentiment 与 /api/status 上)

    盘中/盘前 每分钟一次; 午休/盘后 长间隔兜底; 周末不拉。
    盘后: 当日 complete 为真 → 永不拉 (即「只在首次启动时检查一次」)。
    进程内首次调用做整窗刷新 (day/query), 之后只拉当日分钟 (minute/query)。
    """
    try:
        ph = phase_now or phase()
        if ph == 'WEEKEND':
            return False
        today = datetime.now().strftime('%Y-%m-%d')
        with _LOCK:
            if _RT['busy']:
                return False
            if ph == 'POST_MARKET':
                rec = (_load().get('days') or {}).get(today) or {}
                if rec.get('complete'):
                    return False                      # 今日已结算 → 盘后不再检查
            iv = _PHASE_IV.get(ph, 60)
            if ph in ('MORNING', 'AFTERNOON') and _RT['miss']:
                iv = min(55 * (2 ** _RT['miss']), 900)  # 疑似休市/断网 → 指数退避
            if ph == _RT['phase'] and time.time() - _RT['ts'] < iv:
                return False
            full = not _RT['phase']                   # 进程内首次 → 整窗刷新
            _RT['phase'] = ph
            _RT['ts'] = time.time()
            _RT['busy'] = True
        threading.Thread(target=_worker, args=(full,), daemon=True).start()
        return True
    except Exception:
        return False


def capture_bj(phase_now=None):
    """悄悄记一次北交所(北证50)成交额 (方案A 攒数据, 界面不受影响)。返回是否发起了拉取。

    接收条件: 快照时间 >= 15:00 → 该日已结算, 写 days[iso]['bj_yi']。
      盘后当场记 ✓ | 次日 9:15 前快照还停在昨天, 也能补记 ✓ | 周末开着终端补上周五 ✓
      9:15 后快照翻成新一天(时间 09:xx) → 拒绝, 不会把集合竞价的零头写进去 ✗
    顺带把快照值存进 _BJ['live'] — 切成三市后当日盘中折算要用它 (那时它还不是结算值)。

    攒够之前只在收盘后与盘前拉 (盘中不需要, 省一次请求); 切成三市后盘中每分钟拉一次。
    """
    try:
        ph = phase_now or phase()
        triple = bool(_load().get('bj_mode'))
        if not triple and ph in ('MORNING', 'AFTERNOON', 'NOON_BREAK'):
            return False                          # 攒数据阶段盘中用不上
        with _LOCK:
            if _BJ['busy']:
                return False
            if time.time() - _BJ['ts'] < _BJ_IV.get(ph, 300):
                return False
            _BJ['ts'] = time.time()
            _BJ['busy'] = True
        threading.Thread(target=_bj_worker, daemon=True).start()
        return True
    except Exception:
        return False


def _bj_worker():
    try:
        iso, yi, hhmm = _fetch_bj()
        if not iso or not yi:
            return
        _BJ['live'] = {'date': iso, 'yi': yi}     # 三市口径当日折算用 (盘中是累计值)
        if hhmm < '150000':
            return                                # 未收盘 → 只更新 live, 不落盘
        with _LOCK:
            store = _load()
            days = store.setdefault('days', {})
            rec = days.get(iso)
            if not isinstance(rec, dict) or not isinstance(rec.get('min'), dict):
                return                            # 那天没有沪深分钟数据 → 记了也没法比
            if rec.get('bj_yi') == round(yi, 1):
                return
            rec['bj_yi'] = round(yi, 1)           # 已收盘 → 定值, 反复拉到也写同样的数
            days[iso] = rec
            _flip_check(store)
            _save(store)
    except Exception:
        pass
    finally:
        _BJ['busy'] = False


# ---------------------------------------------------------------- 口径
def _bj_map(days):
    """{iso: 当日北交所成交额(亿)} — 缺的那天用前一个可用值顺延。

    为什么顺延而不是当 0: 北交所日量稳定在 150~200 亿, 顺延的误差是它的日间波动(±20亿);
    当 0 则是 160 亿的假跳变, 足以把 ±100/±500 的档位翻转。宁可小错, 不要假信号。
    """
    out, last = {}, None
    for d in sorted(days):
        v = (days.get(d) or {}).get('bj_yi')
        if v is None:
            v = last
        if v is not None:
            last = v
            out[d] = v
    return out


def _apply_bj(days, live):
    """三市口径: 把每天的北交所成交额按沪深曲线等比摊进分钟序列, 返回折算后的副本。

    缩放系数 k = 1 + 北交所 / 沪深当日累计。
      过去日 累计取 min['1500']  → 该日全天值精确 = 沪深全天 + 北交所全天
      当日   累计取 max(min)     → 若是快照实时值, 则当日累计值精确
    盘中形态按沪深曲线等比假设 (北交所占比 ~1%, 形态差异对总量影响 < 0.2%)。
    """
    bjm = _bj_map(days)
    out = {}
    for d, rec in days.items():
        mins = rec.get('min') or {}
        if not mins:
            out[d] = rec
            continue
        v = rec.get('bj_yi')
        if v is None and (live or {}).get('date') == d:
            v = live.get('yi')                    # 当日盘中: 用快照实时累计
        if v is None:
            v = bjm.get(d)                        # 缺失: 前值顺延
        base = mins[max(mins)]
        if not v or not base:
            out[d] = rec
            continue
        k = 1.0 + v / base
        nr = dict(rec)
        nr['min'] = {t: x * k for t, x in mins.items()}
        out[d] = nr
    return out


def _caliber(st):
    """按当前口径取日线: 三市 → 折算, 沪深 → 原样。

    单点接入, context() / daily() 都走这里, 保证顶栏、浮窗、量能历史永远同一口径。
    """
    days = st.get('days') or {}
    if not st.get('bj_mode'):
        return days
    return _apply_bj(days, _BJ.get('live') or {})


def bj_info():
    """北交所采集进度 (自检 / 排查用)"""
    st = _load()
    days = st.get('days') or {}
    keys = sorted(days)
    have = [d for d in keys if (days.get(d) or {}).get('bj_yi') is not None]
    return {'mode': '三市' if st.get('bj_mode') else '沪深',
            'have': len(have), 'need': WINDOW + 1, 'days': len(keys),
            'last': round((days.get(have[-1]) or {}).get('bj_yi') or 0, 1) if have else None,
            'live': dict(_BJ.get('live') or {})}


# ---------------------------------------------------------------- 派生计算
def grade(delta_yi):
    """较昨日绝对差值(亿) → (评级, 情绪分)。用户 2026-09-10 定案口径。

    放量上攻/放量下跌都不算坏, 最怕无量 (无量上攻是诱多, 无量下跌还要跌) —
    故缩量侧分数衰减比放量侧更快, 平量锚定原「正常」档位 12 分。
    """
    if delta_yi is None:
        return '--', 12
    a = abs(delta_yi)
    if a <= 100:  return '平量', 12
    if a <= 500:  return ('微放量', 15) if delta_yi > 0 else ('微缩量', 9)
    if a <= 1000: return ('温和放量', 18) if delta_yi > 0 else ('温和缩量', 6)
    if a <= 1500: return ('显著放量', 21) if delta_yi > 0 else ('显著缩量', 3)
    if a <= 2000: return ('极大放量', 23) if delta_yi > 0 else ('极大缩量', 2)
    return ('爆量', 24) if delta_yi > 0 else ('无量', 1)


def _prev_key(mins, cur_key):
    """昨日分钟序列里取 ≤ cur_key 的最后一根 (昨日缺该分钟时兜底)"""
    if cur_key in mins: return cur_key
    c = [k for k in mins if k <= cur_key]
    return max(c) if c else None


def context():
    """纯读盘派生: 当前累计 / 昨日同期 / 昨日全天 / 预计全天 / 成交预测 / 近5日均额。

    mode: live    = 有当日数据 (盘后 complete 时 est=cur, 即准确值)
          settled = 无当日数据且(周末 或 8点前) → 显示最近一个已收盘交易日的结算值
          none    = 交易日 8点后仍无当日数据 → 前端显示 '--'
    """
    st = _load()
    days = _caliber(st)
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')
    t = now.hour * 100 + now.minute
    fulls = {d: v['min']['1500'] for d, v in days.items()
             if isinstance(v, dict) and isinstance(v.get('min'), dict) and '1500' in v['min']}

    asof = None
    if (days.get(today) or {}).get('min'):
        mode, asof = 'live', today
    elif now.weekday() >= 5 or t < 800 or _holiday_safe():
        mode = 'settled'
        # v2.118: 休市日与周末同一待遇 —— 原先节假日走 else 分支显示 '--',
        # 而周六显示上一个交易日的结算成交额, 同一件事两种表现。
        asof = max(fulls) if fulls else None      # 周末/休市/8点前: 看最近的结算值
        if asof and asof == today: asof = None
    else:
        mode = 'none'                              # 交易日 8点后还没数据 → '--'

    r = {'ok': bool(asof), 'mode': mode, 'asof': asof, 'phase': phase(now),
         'caliber': '三市' if st.get('bj_mode') else '沪深两市',
         'cur_yi': None, 'cur_min': None, 'settled': False,
         'prev_day': None, 'prev_full_yi': None, 'prev_same_yi': None, 'same_delta_yi': None,
         'est_full_yi': None, 'fcst_delta_yi': None,
         'ma5_yi': None, 'ma5_n': 0, 'ma5_delta_yi': None,
         'note': '', 'updated': st.get('updated', '')}
    if not asof:
        return r

    mins = days[asof]['min']
    cur_key = max(mins)
    cur = mins[cur_key]
    complete = '1500' in mins
    prior = sorted(d for d in fulls if d < asof)
    prev_day = prior[-1] if prior else None
    prev_full = fulls.get(prev_day) if prev_day else None
    prev_same = None
    if prev_day:
        pk = _prev_key((days[prev_day].get('min') or {}), cur_key)
        prev_same = (days[prev_day]['min'][pk]) if pk else None

    # 自校准外推: 今日同期占昨日全天的比例 → 反推今日全天
    if complete:
        est, note = cur, ''
    elif prev_same and prev_same > 0 and prev_full:
        est, note = cur / (prev_same / prev_full), '(盘中预测)'
    else:
        est, note = cur, ''

    tail = prior[-WINDOW:]
    ma5 = (sum(fulls[d] for d in tail) / len(tail)) if tail else None

    r.update({
        'cur_yi': round(cur, 1), 'cur_min': cur_key, 'settled': complete,
        'prev_day': prev_day,
        'prev_full_yi': round(prev_full, 1) if prev_full else None,
        'prev_same_yi': round(prev_same, 1) if prev_same else None,
        'same_delta_yi': round(cur - prev_same, 1) if prev_same else None,
        'est_full_yi': round(est, 1),
        'fcst_delta_yi': round(est - prev_full, 1) if prev_full else None,
        'ma5_yi': round(ma5, 1) if ma5 else None,
        'ma5_n': len(tail),
        'ma5_delta_yi': round(est - ma5, 1) if ma5 else None,
        'note': note,
    })
    return r


def daily(n=WINDOW):
    """最近 n 个已收盘交易日的全天成交额 + 较前一交易日差值 + 评级 (由新到旧)

    v2.63 (2026-09-10): 量能历史不单独落盘 — 成交额日线本来就在 days[*].min['1500'] 里,
    评级由同一个 grade() 派生。好处: ① 冷启动即有 5 天, 不必等一周;
    ② 不会像"盘中记一行、收盘又变了"那样失真; ③ 与顶栏口径永远一致。

    需要「前一交易日」算差值, 故取 n+1 个全天值; 不足 n+1 时返回能算出来的部分。
    """
    days = _caliber(_load())
    fulls = {d: v['min']['1500'] for d, v in days.items()
             if isinstance(v, dict) and isinstance(v.get('min'), dict) and '1500' in v['min']}
    dts = sorted(fulls)
    out = []
    for i in range(len(dts) - 1, 0, -1):        # 从最新一天往回, 第 0 天没有前值可减
        d, p = dts[i], dts[i - 1]
        delta = fulls[d] - fulls[p]
        out.append({'date': d, 'full_yi': round(fulls[d], 1),
                    'delta_yi': round(delta, 1), 'status': grade(delta)[0]})
        if len(out) >= n:
            break
    return out


def status():
    """落盘文件的概览 (自检 / 排查用)"""
    st = _load()
    days = st.get('days') or {}
    return {'file': TURNOVER_FILE, 'updated': st.get('updated', ''),
            'bj_mode': bool(st.get('bj_mode')),
            'days': {d: {'n': len(v.get('min') or {}),
                         'complete': bool(v.get('complete')),
                         'full_yi': round((v.get('min') or {}).get('1500', 0), 1),
                         'bj_yi': v.get('bj_yi')}
                     for d, v in sorted(days.items())},
            'bj': bj_info(), 'rt': dict(_RT)}


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    a = sys.argv[1] if len(sys.argv) > 1 else ''
    if a == 'fetch':
        _RT['phase'] = ''                      # 强制整窗刷新
        _worker(True)
        print(json.dumps(status(), ensure_ascii=False, indent=2))
    elif a == 'bj':
        _BJ['ts'] = 0.0                        # 强制拉一次北交所
        _bj_worker()
        print(json.dumps(bj_info(), ensure_ascii=False, indent=2))
        print(json.dumps(status(), ensure_ascii=False, indent=2))
        raise SystemExit
    c = context()
    print(json.dumps(c, ensure_ascii=False, indent=2))
    g = grade(c.get('fcst_delta_yi'))
    print(f"评级: {g[0]}  情绪分: {g[1]}")
