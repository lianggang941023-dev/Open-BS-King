# -*- coding: utf-8 -*-
"""v2.152 (2026-10-18): 通达信 tdx-aidata 数据抓取 —— 子进程隔离版

为什么要子进程（三条都是硬约束，不是偏好）:
  ① DLL 只在 start() 那一刻读一次 lib/TdxAiData.ini 的 [Token] token=
     （tdxaidata.py:208 `_ensure_started()` 里 `if self._is_started: return`），
     之后改 ini **不生效** ⇒ 用户在界面里换 key 必须重启进程。
     走子进程 ⇒ 每次调用都重新 start ⇒ 改 ini **立刻生效**，用户随时可以换 key。
  ② `_ensure_started()` 里 `os.chdir(os.path.dirname(self._dll_path))` 且**永不切回**
     （注释: DLL 依赖同目录 NewTc.dat）⇒ 主进程 cwd 会被改掉。子进程里 chdir 无所谓。
  ③ DLL 崩了不拖垮 8799。

用法（由 server.py / auto_task.py 调用）:
    python -m quant_terminal.tdx_fetch --mode snapshot --out <json路径>
    python -m quant_terminal.tdx_fetch --mode snapshot            # 打印到 stdout
    python -m quant_terminal.tdx_fetch --selftest                 # 只验证 key 可用

输出 JSON:
    {'ok': True, 'mode': 'snapshot', 'elapsed': 3.2, 'trade_date': '2026-10-18',
     'count': 5581, 'hq_date': '20261018', 'stocks': {code: {...}}, 'exrights': {...}}

积分: 快照类接口实测「每次运行 1 分，与只数/字段数无关」（2026-10-18 实测
      3 只 = 5581 只 = 1 分；同一次运行内多次调用合并计 1 分）。
"""
import argparse
import re
import json
import os
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.dirname(HERE)          # D:\AI Working\_运行环境\终端8799
DEFAULT_LIB = os.path.join(BASE, 'tdx', 'lib')

try:                                # v2.234: 腾讯 qt 市值兜底是境内源, 优先直连
    from .net import open_url as _net_open
except Exception:
    from net import open_url as _net_open

# 快照字段: 注意是 Max/Min 不是 High/Low —— 传 High/Low 会被 DLL **静默忽略**
# Zsz/Ltsz = 总市值/流通市值 (单位亿元), 正好补 CSV 第7/8列, 不必再单独联网取市值
# v2.166 (2026-10-20): 去掉 Zsz/Ltsz —— get_market_snapshot 的 26 字段（文档 F.3）
#   压根没有这两个字段，写在 field_list 里也不会被返回 ⇒ 一直静默取到 0。
#   市值改由下方的 get_more_info_batch 整批取（同一个接口顺带拿 HqDate）。
SNAP_FIELDS = ['LastClose', 'Open', 'Max', 'Min', 'Now', 'Volume', 'Amount']
# 除权校验探针（只要这几只拿到今天的 HqDate 就证明数据是今天的）
HQ_PROBES = ['600000.SH', '000001.SZ', '600519.SH', '601318.SH', '000858.SZ']

# ---- v2.164: 本地调用审计账（用户 m21624 批准）------------------------------
# 为什么需要它: 2026-10-20 用户问「11:40 为什么花了 4 积分」，我查到"11:40 那轮根本没碰
#   通达信"，但**无法证明每次运行到底扣了几分** —— 因为
#     ① tdxaidata 的 lib\log 与 lib\data 两个目录都是空的，DLL 不写本地日志
#     ② 包内没有任何余额/积分查询 API（1,288 行公开方法里没有 get_balance 之类）
#   所以只能自己记账：把「谁在什么时候调了几个接口」逐笔钉死。
#   下笔账单来时，缺的只剩「每次几分」这一个系数，而不是"到底发生没发生"。
CALLS_FILE = os.path.join(HERE, 'tdx_calls_log.json')
CALLS_KEEP = 400          # 只留最近 400 次运行（一年 500 次左右，够复盘且不会无限长大）

_CALLS = {'id': 0, 'who': '', 'items': [], 't0': 0.0}


# ---------------------------------------------------------------- 配置
def load_cfg():
    """读 data_sources.json 的 online.tdx 节（与数据源界面共用同一份配置）"""
    cfg = {'enabled': True, 'lib_dir': DEFAULT_LIB, 'timeout': 120}
    fp = os.path.join(HERE, 'data_sources.json')
    try:
        with open(fp, encoding='utf-8') as fh:
            d = json.load(fh)
        t = ((d.get('online') or {}).get('tdx') or {})
        lib = str(t.get('lib_dir') or '').strip()
        if lib:
            cfg['lib_dir'] = lib if os.path.isabs(lib) else os.path.join(BASE, lib)
        cfg['enabled'] = t.get('enabled', True) is not False
        cfg['timeout'] = int(t.get('timeout') or 120)
    except Exception:
        pass
    return cfg


# ---------------------------------------------------------------- token (界面填 key)
# tdxaidata 的 token **只认包内 ini**（Python 层无 API，DLL 自己读），所以"界面填 key"
# 唯一可行的落法就是写这两个文件。两个都写: DLL 读 TdxAiData.ini, 但库自己在 start()
# 时会建/刷新 user.ini（内容相同），只改一个会出现"两个文件不一致"的静默隐患。
INI_NAMES = ('TdxAiData.ini', 'user.ini')


def ini_dir(lib_dir=''):
    """ini 所在目录 = <lib_dir>/tdxaidata/lib"""
    lib = str(lib_dir or '').strip() or load_cfg()['lib_dir']
    lib = lib if os.path.isabs(lib) else os.path.join(BASE, lib)
    return os.path.join(lib, 'tdxaidata', 'lib')


def read_token(lib_dir=''):
    """读盘上生效的 token（只给"是否已配置 + 长度"用，绝不回显真值）"""
    d = ini_dir(lib_dir)
    for nm in INI_NAMES:                      # TdxAiData.ini 优先（DLL 认它）
        p = os.path.join(d, nm)
        try:
            with io_open_text(p) as fh:
                for ln in fh:
                    s = ln.strip()
                    low = s.lower()
                    if low.startswith('token') and '=' in s:
                        v = s.split('=', 1)[1].strip()
                        if v:
                            return v
        except Exception:
            continue
    return ''


def io_open_text(p):
    """utf-8 打开（TDX 的 ini 是纯 LF 无 BOM；容错 BOM/GBK）"""
    try:
        return open(p, 'r', encoding='utf-8-sig')
    except Exception:
        return open(p, 'r', encoding='gbk', errors='replace')


def set_token(key, lib_dir=''):
    """把 key 写进两个 ini。返回 (ok, msg)。

    ⚠ 只改 token 那一行，其余字节原样保留（[TcHqHost]/[TcDsHost] 段是主机配置，
      改坏了连不上服务器）。写盘用 .tmp + os.replace 保证原子。
    """
    key = str(key or '').strip()
    if not key:
        return False, 'key 为空'
    if not re.match(r'^[A-Za-z0-9_.\-]{8,128}$', key):
        return False, 'key 格式不像通达信 Key（只允许字母数字和 _ - .，8~128 位）'
    d = ini_dir(lib_dir)
    if not os.path.isdir(d):
        return False, 'ini 目录不存在: ' + d
    done = []
    for nm in INI_NAMES:
        p = os.path.join(d, nm)
        if nm == 'user.ini' and not os.path.exists(p):
            # 这个文件是库自己建的；不存在也没关系（DLL 认 TdxAiData.ini）
            continue
        try:
            with open(p, 'rb') as fh:
                _raw = fh.read()
            txt = _raw.decode('utf-8-sig', 'replace')
        except Exception as e:
            return False, f'读 {nm} 失败: {e}'
        # ⚠ 行尾必须从**原始字节**判 —— text 模式读会把 CRLF 统一转成 LF，
        #   那样 '\r\n' in txt 永远 False，会把 user.ini(CRLF) 静默改成 LF。
        nl = '\r\n' if b'\r\n' in _raw else '\n'
        lines = txt.split(nl)
        hit = False
        for i, ln in enumerate(lines):
            s = ln.strip()
            if s.lower().startswith('token') and '=' in s:
                lines[i] = 'token=' + key
                hit = True
                break
        if not hit:
            if not lines or lines[-1].strip() != '':
                lines.append('')
            if '[Token]' not in txt:
                lines.append('[Token]')
            lines.append('token=' + key)
        out = nl.join(lines)
        tmp = p + '.tmp'
        try:
            with open(tmp, 'w', encoding='utf-8', newline='') as fh:
                fh.write(out)
            os.replace(tmp, p)
            done.append(nm)
        except Exception as e:
            return False, f'写 {nm} 失败: {e}'
    if not done:
        return False, '没有可写的 ini（先跑一次 selftest 让库生成 user.ini）'
    return True, '已写入 ' + ' / '.join(done)


def token_status(lib_dir=''):
    """给界面用: {configured, length, files:[...], ini_dir}  ← 绝不含真值"""
    d = ini_dir(lib_dir)
    files = []
    for nm in INI_NAMES:
        p = os.path.join(d, nm)
        files.append({'name': nm, 'exists': os.path.exists(p),
                      'size': (os.path.getsize(p) if os.path.exists(p) else 0)})
    tk = read_token(lib_dir)
    return {'configured': bool(tk), 'length': len(tk), 'files': files, 'ini_dir': d}


def bare(code):
    """'SH600000' / 'sh600000' / '600000' → '600000'"""
    c = (code or '').strip().upper()
    return c[2:] if (len(c) > 2 and c[:2] in ('SH', 'SZ', 'BZ', 'BJ') and c[2:].isdigit()) else c


def tdx_sym(code):
    """本地基名 'SH600000' → 通达信 '600000.SH'（北交所 BZ → BJ）"""
    c = (code or '').strip().upper()
    if len(c) < 3:
        return ''
    mkt = c[:2]
    num = c[2:]
    if not num.isdigit():
        return ''
    if mkt in ('BZ', 'BJ'):
        return num + '.BJ'
    if mkt == 'SH':
        return num + '.SH'
    return num + '.SZ'


# ---------------------------------------------------------------- 本地K线
def daily_dir():
    try:
        from quant_terminal import datasource as ds
        return str(ds.resolve(ds.get('local', 'kline_dir', os.path.join('kline_data', 'daily'))))
    except Exception:
        return os.path.join(BASE, 'kline_data', 'daily')


def read_last_row(fpath):
    """只读文件尾部取最后一行 (5581 个文件 ~0.6s) → (date, close, volume)"""
    try:
        sz = os.path.getsize(fpath)
        with open(fpath, 'rb') as fh:
            fh.seek(max(0, sz - 512))
            chunk = fh.read().decode('utf-8', 'replace')
        lines = [l for l in chunk.replace('\r', '').split('\n') if l.strip()]
        if not lines:
            return ('', 0.0, 0.0)
        q = lines[-1].split(',')
        if len(q) < 6 or q[0] == 'date':
            return ('', 0.0, 0.0)
        return (q[0], float(q[2]), float(q[5]))
    except Exception:
        return ('', 0.0, 0.0)


# ---------------------------------------------------------------- 审计账 (v2.164)
def _audit_load():
    try:
        with open(CALLS_FILE, encoding='utf-8') as fh:
            d = json.load(fh)
        if not isinstance(d, dict):
            return {'runs': []}
        d.setdefault('runs', [])
        return d
    except Exception:
        return {'runs': []}


def _audit_begin(who=''):
    """开始一次运行：建上下文。**调用方必须 try/finally 保证 _audit_end**"""
    _CALLS['id'] = int(time.time() * 1000)
    _CALLS['who'] = str(who or '')
    _CALLS['items'] = []
    _CALLS['t0'] = time.time()


def _audit_call(name, ok, detail=''):
    """记一个**计费接口调用**。name 用 DLL 层的名字（GetBatchRealDATsInStr 等）,
    这样将来对账单时能直接按 DLL 调用次数算, 不受 Python 层封装改名影响。
    """
    try:
        _CALLS['items'].append({
            'api': str(name),
            'ok': bool(ok),
            'at': round(time.time() - (_CALLS['t0'] or time.time()), 2),
            'detail': str(detail or '')[:120],
        })
    except Exception:
        pass


def _audit_end(count=0, extra=None):
    """收尾并落盘。**任何异常都不许往上抛** —— 记账失败不能影响抓数据。"""
    try:
        items = list(_CALLS['items'])
        rec = {
            'id': _CALLS['id'],
            'at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'who': _CALLS['who'],
            'calls': len(items),
            'items': items,
            'count': int(count or 0),
            'elapsed': round(time.time() - (_CALLS['t0'] or time.time()), 2),
        }
        if extra:
            rec.update(extra)
        d = _audit_load()
        runs = d['runs']
        runs.append(rec)
        if len(runs) > CALLS_KEEP:
            del runs[:len(runs) - CALLS_KEEP]
        tmp = CALLS_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1)
        os.replace(tmp, CALLS_FILE)
        _CALLS['items'] = []
        return rec
    except Exception as e:
        try:
            print(f'[tdx] 审计账写失败(不影响抓取): {e}', file=sys.stderr, flush=True)
        except Exception:
            pass
        return None


def calls_log(limit=50):
    """给界面/调试用：最近 N 次运行（倒序）"""
    d = _audit_load()
    runs = d.get('runs') or []
    return {'ok': True, 'total': len(runs), 'runs': runs[-int(limit or 50):][::-1]}


# ---------------------------------------------------------------- 抓取
def _dry_run_result(n):
    """QT_TDX_DRY=1 时的假返回 —— 供单测使用，**绝不碰 DLL、不花积分**

    🔴 加这道闸的原因: 2026-10-18 16:09 我跑调度器单测时，dispatch 写成了直接调
       `_task_snap`（绕过 bind 注入的桩），单元测试真的走到 DLL 里去了。虽然进程被
       daemon 线程退出掐断、未确认扣分，但这是**不可接受**的 —— 测试必须物理上
       够不到付费接口。
    """
    return {
        'ok': True, 'mode': 'snapshot', 'dry': True,
        'trade_date': datetime.now().strftime('%Y-%m-%d'),
        'hq_date': datetime.now().strftime('%Y%m%d'),
        'count': n, 'suspects': 0, 'exrights': {},
        'stocks': {}, 'elapsed': 0.0,
        'error': 'QT_TDX_DRY=1 干跑，未调用通达信',
    }


def _tencent_mcap(files, batch=50, timeout=8):
    """v2.177 (2026-10-22): 腾讯免费快照取市值 —— {6位代码: (总市值亿, 流通市值亿)}。

    用户 m02201 定案: 通达信 15:35 拉市值万一失败, 改用腾讯兜底 (不花积分);
    腾讯也失败则保留 CSV 旧值, 绝不在这种情况下把市值写成 0。
    qt 字段: f[44]=流通市值 f[45]=总市值 (亿元)。
    """
    import urllib.request as _u
    out = {}
    codes = [str(f)[:-4] for f in files]
    for i in range(0, len(codes), batch):
        part = codes[i:i + batch]
        _q = []
        for c in part:
            if c[:3] == '920' or c[0] in '48':
                _q.append('bj' + c)
            elif c[0] in '69':
                _q.append('sh' + c)
            else:
                _q.append('sz' + c)
        try:
            req = _u.Request('http://qt.gtimg.cn/q=' + ','.join(_q),
                             headers={'User-Agent': 'Mozilla/5.0'})
            txt = _net_open(req, timeout=timeout).read().decode('gbk', 'ignore')
        except Exception:
            continue
        for line in txt.split(';'):
            if '="' not in line or '~' not in line:
                continue
            try:
                body = line.split('="', 1)[1].rstrip('"')
                f = body.split('~')
                if len(f) < 46:
                    continue
                cd = f[2].strip()
                if not (len(cd) == 6 and cd.isdigit()):
                    continue
                _t = float(f[45]) if f[45] not in ('', '--') else 0.0
                _fm = float(f[44]) if f[44] not in ('', '--') else 0.0
                if _t > 0 or _fm > 0:
                    out[cd] = (_t, _fm)
            except Exception:
                continue
    return out


def do_snapshot(verbose=True, dry=False, who='', with_mcap=True, price_src='auto'):
    """dry=True = 干跑：不导入 DLL、不花积分，只回一个结构相同的假结果

    who: v2.164 审计账里记「谁触发的」（'auto:snap' / 'auto:snap_eod' / 'manual' / 'cli'），
         账单对不上时这栏是唯一能区分「自动任务花的」和「我验收花的」的东西。

    🔴 v2.157c (2026-10-18): 验收脚本必须走这条路。原来只能靠环境变量
       QT_TDX_DRY，而 /api/auto_task/run 没有透传口子 ⇒ 我每跑一次验收
       就真拉一次全市场。17:06 的 20 分账单就是这么来的（m17996 溯源）。

    v2.169 (2026-10-21) 参数 with_mcap: 要不要顺带把全市场市值拉回来。
        True  = 第 2 次调用取 'HqDate','Zsz','Ltsz'（全市场 5582 只）—— 15:35 收盘写 CSV 那轮用。
        False = 第 2 次调用只探 HQ_PROBES 5 只、字段只要 'HqDate' —— 盘中 11:35 那轮用。
    为什么盘中可以不拉市值: 盘中快照写进 intraday_snapshots.json 的 mcap/fmcap 只有
        intraday.caps()/bars() 读，两者的唯一调用方都是 run_snapshot_from_intraday（15:35 写 CSV），
        而 15:35 自己会重新 do_snapshot 一份直接覆盖 —— 11:35 拉回来的市值从写入到被覆盖无人读。
        前端盘中看市值走 /api/mcap（读 K 线 CSV 末行），与这份快照无关。

    v2.170 (2026-10-21) 参数 price_src: 价格从哪个源取。
        'auto' （默认）= data_sources.json 配了 online.fuyao.key 就走同花顺，否则走通达信
        'fuyao'        = 强制同花顺（没配 key 直接报错，不静默退回）
        'tdx'          = 强制通达信（老路，一行行为都不变）
    为什么换源只换「取价」这一段: 组装 / 停牌闸 / 除权系数三个环节完全不动 ——
        除权系数靠的是「快照昨收 ÷ 本地末行收盘」，两家给的昨收同义（见 fuyao.py 文档串第 ③ 条），
        把 fuyao 的字段改成通达信的字段名塞进同一个 snap 就够了，不产生第二套除权实现。
    🔴 11:35（with_mcap=False）在 fuyao 路径下**完全不开通达信审计账、不导入 DLL**；
       而 fuyao 万一失败也**不会自动回退通达信** —— 那一天就不拉、直接报错 + 记日志，
       避免「以为免费了却又被扣积分」。15:35 那条路照旧，失败才回退。
    """
    cfg = load_cfg()
    lib = cfg['lib_dir']
    t0 = time.time()
    ddir = daily_dir()
    if not os.path.isdir(ddir):
        return {'ok': False, 'error': f'K线目录不存在: {ddir}'}

    # ---- 本地文件清单（这就是代码表，不必再调 get_stock_list 浪费一次运行）----
    files = sorted(f for f in os.listdir(ddir) if f.lower().endswith('.csv'))
    codes = [f[:-4] for f in files]
    if verbose:
        print(f'[tdx] 本地 {len(codes)} 个 CSV', file=sys.stderr, flush=True)

    # 干跑闸：放在清单之后 —— 这样假结果的 count 是真实只数，验收断言更有意义
    if dry or os.environ.get('QT_TDX_DRY', '').lower() in ('1', 'true', 'yes'):
        # v2.164: 干跑**不记账**（一分没花，记了只会把真账本刷成噪声）
        return _dry_run_result(len(codes))

    # ---- 取价源选择 (v2.170 2026-10-21 用户 m00787) ---------------------------
    # 同花顺 fuyao 一次请求就回全市场（实测 5580 只 / 1.07 秒 / 免费），
    # 通达信同一件事 2 次计费调用 / 12.7 秒 ⇒ 默认 auto 优先 fuyao。
    # need_dll = 这次是否真的会碰通达信 DLL:
    #   走通达信取价        → 是
    #   走同花顺 + 15:35 拉市值 → 是（市值只有通达信 get_more_info_batch 能给）
    #   走同花顺 + 11:35 不拉市值 → **否**（这才是「通达信仅保留 15:35」的落地点）
    _fq = None
    if price_src in ('auto', 'fuyao'):
        try:
            from . import fuyao as _fq           # 作为包导入（server / auto_task 走这条）
        except Exception:
            try:
                import fuyao as _fq              # 直接跑脚本、CLI 时
            except Exception:
                _fq = None
    use_fuyao = False
    if _fq is not None:
        use_fuyao = True if price_src == 'fuyao' else bool(_fq.configured())
    need_dll = (not use_fuyao) or bool(with_mcap)
    if need_dll:
        if not cfg['enabled']:
            return {'ok': False, 'error': '通达信数据源未启用 (data_sources.json → online.tdx.enabled)'}
        if not os.path.isdir(lib):
            return {'ok': False, 'error': f'tdx 库目录不存在: {lib}'}

    # v2.164: 从这里往下都是真花钱的路径 ⇒ 开账
    # v2.170: 但「同花顺 + 11:35」一分不花 ⇒ 不开账，账本里不该出现这种行
    _audited = [False]

    def _end(cnt, extra):
        if _audited[0]:
            _audit_end(cnt, extra)

    if need_dll:
        _audit_begin(who=who or 'cli')
        _audited[0] = True

    # ---- 调库 ----
    cwd0 = os.getcwd()
    tqs = None
    if need_dll:
        sys.path.insert(0, lib)
        try:
            from tdxaidata import tqs
        except Exception as e:
            # v2.164: 这一个 return 在 _audit_begin 之后，必须收账 —— 否则上下文悬着，
            #   下一次运行的记录会把它顶掉，账本里就看不出「那次连库都没导入成功」。
            _end(0, {'ok': False, 'error': f'导入失败: {str(e)[:120]}'})
            return {'ok': False, 'error': f'导入 tdxaidata 失败: {e} (lib={lib})'}
        finally:
            sys.path.remove(lib)

    result = {'ok': False, 'mode': 'snapshot', 'lib': lib}

    # 快照: **整批一次**（v2.157c 2026-10-18 改）
    # 原来 BATCH=1200 分 5 批，注释理由是"分批更稳"。但实测 5581 只一次调用
    # 2.76 秒就拿全了，分批的"稳"是心理上的，代价是把一次运行变成 5 次底层调用。
    # 17:06 那次 5 个验收脚本并发跑、每个再乘 5 批 ⇒ 账单炸到 20 分（m17996）。
    # 现在: 默认整批一次；只有整批抛异常才退回 1200/批（正常路径永远走不到）。
    syms = [tdx_sym(c) for c in codes]
    syms = [s for s in syms if s]
    snap = {}
    _hq_fq = ''          # 同花顺快照自带数据就绪时间 ⇒ 11:35 不用再问通达信要 HqDate
    if use_fuyao:
        # 同花顺路径: 一次请求全市场，**只把字段名改成通达信那套**塞进 snap，
        # 下面的组装 / 停牌闸 / 除权系数逻辑因此一行都不用动。
        _pref = {str(c)[-6:]: c for c in codes}
        try:
            _rows, _ts = _fq.snapshot_all()
            for _b, _r in _rows.items():
                _c = _pref.get(_b)
                if not _c:
                    continue
                _s = tdx_sym(_c)
                if not _s:
                    continue
                snap[_s] = {'Now': _r.get('close'), 'Open': _r.get('open'),
                            'Max': _r.get('high'), 'Min': _r.get('low'),
                            'Volume': _r.get('volume'), 'LastClose': _r.get('prev_close')}
            try:
                if _ts:
                    _hq_fq = time.strftime('%Y%m%d', time.localtime(int(_ts) / 1000.0))
            except Exception:
                _hq_fq = ''
            if verbose:
                print(f'[fuyao] 快照 {len(_rows)} 只 → 匹配本地 {len(snap)} 只'
                      f' | 数据日 {_hq_fq or "?"}', file=sys.stderr, flush=True)
        except Exception as e:
            try:
                _fq.log('失败', 'do_snapshot', str(e)[:120])
            except Exception:
                pass
            if price_src == 'fuyao':
                _end(0, {'ok': False, 'error': str(e)[:150]})
                return {'ok': False, 'error': f'同花顺快照失败: {str(e)[:200]}'}
            if not need_dll:
                # 11:35 这条路**故意不回退通达信** —— 用户定案「通达信仅保留 15:35 拉市值」，
                # 悄悄回退等于把他以为已经省下的积分又花掉，宁可今天不拉 + 记日志
                return {'ok': False,
                        'error': f'同花顺快照失败且 11:35 不回退通达信: {str(e)[:160]}'}
            if verbose:
                print(f'[fuyao] 失败 ({str(e)[:80]}), 退回通达信', file=sys.stderr, flush=True)
            use_fuyao = False
    if not use_fuyao:
        BATCH = int(os.environ.get('QT_TDX_BATCH', '0') or 0)   # 0 = 不分批
        try:
            if BATCH <= 0:
                try:
                    got = tqs.get_market_snapshot_batch(stock_list=syms,
                                                        field_list=SNAP_FIELDS)
                    if isinstance(got, dict):
                        snap.update(got)
                    # v2.164: 记一笔（这是本次运行的第 1 次计费调用）
                    _audit_call('get_market_snapshot_batch(whole)', True, f'{len(syms)}只')
                    if verbose:
                        print(f'[tdx] 快照 整批 {len(syms)} 只 → {len(snap)} 条',
                              file=sys.stderr, flush=True)
                except Exception as e1:
                    _audit_call('get_market_snapshot_batch(whole)', False, str(e1)[:100])
                    if verbose:
                        print(f'[tdx] 整批失败 ({str(e1)[:80]}), 退回 1200/批',
                              file=sys.stderr, flush=True)
                    for i in range(0, len(syms), 1200):
                        part = syms[i:i + 1200]
                        got = tqs.get_market_snapshot_batch(stock_list=part,
                                                            field_list=SNAP_FIELDS)
                        if isinstance(got, dict):
                            snap.update(got)
                        _audit_call('get_market_snapshot_batch(fallback-1200)', True, f'{len(part)}只')
            else:
                for i in range(0, len(syms), BATCH):
                    part = syms[i:i + BATCH]
                    got = tqs.get_market_snapshot_batch(stock_list=part,
                                                        field_list=SNAP_FIELDS)
                    if isinstance(got, dict):
                        snap.update(got)
                    _audit_call('get_market_snapshot_batch(batch)', True, f'{len(part)}只')
                    if verbose:
                        print(f'[tdx] 快照 {min(i + BATCH, len(syms))}/{len(syms)}',
                              file=sys.stderr, flush=True)
        except Exception as e:
            # v2.164: 失败了也把已发生的调用记账 —— 失败照样扣分，账本不能漏
            _end(0, {'ok': False, 'error': str(e)[:200]})
            return {'ok': False, 'error': f'快照拉取失败: {str(e)[:200]}'}
        finally:
            try:
                os.chdir(cwd0)          # 库里 chdir 永不切回，这里主动还原
            except Exception:
                pass

    if not snap:
        _end(0, {'ok': False, 'error': '快照返回空'})
        return {'ok': False, 'error': '快照返回空'}

    # ---- 第 2 次调用: 扩展信息（HqDate；收盘那轮再加全市场市值）------------------
    # v2.166 (2026-10-20) 用户 m22507: 原来这一发只探 5 只、只要 HqDate；扩成全市场，
    #   同一个接口顺带把总市值/流通市值拿回来 ⇒ **计费调用次数不变（仍是 2 次/运行）**。
    #   实测 5,582 只一次 5.90 秒 / 返回 5,582 条 / 有市值 5,575 只（无值 7 只都是停牌股）。
    #   为什么不改用 get_market_snapshot 取市值: 它的 26 字段（文档 F.3）没有 Zsz/Ltsz。
    #   为什么不用 get_domain_sorthq: 它 wantnum 上限 200 条，5582 只得分 28 页 ⇒ 28 次计费。
    # v2.169 (2026-10-21) 用户 m00352 定案: 市值只有 15:35 写 CSV 那轮用得上（详见 with_mcap
    #   的 docstring），盘中 11:35 那轮不再整批拉，退回只探 5 只拿 HqDate。
    #   ⇒ 盘中一次运行返回条数从 5582 降到 5；若通达信按条计费，这一轮基本不再花钱。
    hq_date = _hq_fq if use_fuyao else ''
    mc = {}          # {6位代码: (总市值亿, 流通市值亿)}  ← 供下方组装段使用
    # v2.273 (2026-11-11 用户 m08653): 第 9 列「自由流通股本」。
    #   通达信 get_more_info_batch 的 FreeLtgb = 自由流通股本(**单位万股**), 与 Zsz/Ltsz 同一个接口,
    #   加进 field_list **不增加计费次数**(仍是 2 次/运行)。用户原话:「新增一列自由流通股本，
    #   先写进去之后我再考虑这个数据用在什么地方」⇒ 只落盘, 不参与任何计算。
    ff = {}          # {6位代码: 自由流通股本(万股)}
    if need_dll:
        if with_mcap:
            _probe_syms, _probe_fields = syms, ['HqDate', 'Zsz', 'Ltsz', 'FreeLtgb']
            _probe_tag = 'get_more_info_batch(HqDate+Zsz+Ltsz+FreeLtgb)'
        else:
            _probe_syms, _probe_fields = list(HQ_PROBES), ['HqDate']
            _probe_tag = 'get_more_info_batch(HqDate)'
        try:
            probe = tqs.get_more_info_batch(stock_list=_probe_syms,
                                            field_list=_probe_fields)
            if isinstance(probe, dict):
                _hq_cnt = {}
                for _k, v in probe.items():
                    v = v or {}
                    h = str(v.get('HqDate') or '').strip()
                    if len(h) >= 8:
                        _hq_cnt[h[:8]] = _hq_cnt.get(h[:8], 0) + 1
                    try:
                        _z = float(v.get('Zsz') or 0)
                        _lz = float(v.get('Ltsz') or 0)
                    except (TypeError, ValueError):
                        _z = _lz = 0.0
                    try:
                        _fr = float(v.get('FreeLtgb') or 0)
                    except (TypeError, ValueError):
                        _fr = 0.0
                    if _z > 0 or _lz > 0:
                        _bd = str(_k).split('.')[0]
                        if _bd.isdigit():
                            mc[_bd] = (_z, _lz)
                            if _fr > 0:
                                ff[_bd] = _fr
                # HqDate 取「出现最多的那个」而不是「第一个非空」—— 全市场里停牌股给空串，
                #   第一个非空可能是只错票；多数决天然抗噪（原来 5 只探针时代无此问题）。
                # v2.170: 同花顺路径下 hq_date 已经有值了，这里只在通达信真的给了日期才覆盖。
                if _hq_cnt:
                    hq_date = max(_hq_cnt.items(), key=lambda kv: kv[1])[0]
            _audit_call(_probe_tag, True,
                        f'{len(_probe_syms)}只 → {len(mc)}只带市值')
        except Exception as e:
            _audit_call(_probe_tag, False, str(e)[:100])
            if verbose:
                print(f'[tdx] 扩展信息失败 ({str(e)[:80]}), 市值将全为 0', file=sys.stderr, flush=True)
        finally:
            try:
                os.chdir(cwd0)
            except Exception:
                pass

    # v2.177 (2026-10-22): 市值兜底 —— 通达信拉不到就换腾讯 (免费), 绝不把两列写成 0。
    if with_mcap and not mc:
        try:
            mc = _tencent_mcap(files)
            if verbose and mc:
                print(f'[tdx] 通达信市值缺失 → 腾讯兜底 {len(mc)} 只', file=sys.stderr, flush=True)
        except Exception as e:
            if verbose:
                print(f'[tdx] 腾讯市值兜底失败: {str(e)[:80]}', file=sys.stderr, flush=True)

    # ---- 组装 ----
    stocks = {}
    for f in files:
        c = f[:-4]
        s = snap.get(tdx_sym(c))
        if not s:
            continue
        try:
            now = float(s.get('Now') or 0)
            op = float(s.get('Open') or 0)
            hi = float(s.get('Max') or 0)
            lo = float(s.get('Min') or 0)
            vol = float(s.get('Volume') or 0)
            prev = float(s.get('LastClose') or 0)
            # v2.166: 市值不再从快照取（快照没这两个字段），改用上面整批拿回来的 mc
            zsz, ltsz = mc.get(bare(c), (0.0, 0.0))
            # v2.273: 自由流通股本(万股), 拿不到就是 0.0 → 写盘时按"没值"处理(留空)
            free = ff.get(bare(c), 0.0)
        except (TypeError, ValueError):
            continue
        if now <= 0 or op <= 0:
            # v2.168 (2026-10-21) 🔴 原来是 `now <= 0 and op <= 0` —— 停牌股的 Now 是**昨收**(>0)、
            #   只有 Open/Max/Min/Volume 是 0 ⇒ 条件整个为假, 于是写出一行
            #   `日期,0.000,昨收,0.000,0.000,0` 的畸形 K 线 (600293 三峡新材 2026-10-19 就是这样,
            #   全市场共 6 行)。没有开盘价 = 当天根本没有交易 ⇒ 一律跳过, 不落盘、不当成一根 K。
            continue           # 停牌 / 当日无行情
        last_date, last_close, _ = read_last_row(os.path.join(ddir, f))
        stocks[bare(c)] = {
            'code': c, 'open': op, 'high': hi, 'low': lo, 'close': now,
            'volume': vol, 'prev_close': prev, 'last_date': last_date,
            'last_close': last_close,
            'mcap': zsz, 'fmcap': ltsz, 'free': free,
        }

    # ---- 除权系数 (v2.159 2026-10-20 重写) ----
    # 系数 = 通达信 LastClose / 本地末行收盘
    #   通达信 LastClose = **已按今日除权调整**的昨收（除权参考价），跟快照一起来，免费
    #   本地末行收盘     = **未调整**的昨收（旧尺度）
    #   ⇒ 相除就是精确系数。15 只除权股实测与交易所公式吻合到 0.06%
    # 🔴 不再调 tqs.get_divid_factors：
    #   ① 那个接口单只调用、要花积分（原来每次运行白查 15~16 只，其实现成的数字就在
    #      快照里）② 它的 Date 带时分秒（'2026-10-19 00:00:00'），原来的日期闸拿 10 位
    #      字符串比 19 位，永远不相等 ⇒ 从 v2.152 上线起一次都没成功过，静默失效。
    td = ''
    if len(hq_date) == 8:
        td = f'{hq_date[:4]}-{hq_date[4:6]}-{hq_date[6:8]}'

    # 「上一个交易日」= 本地 CSV 末行日期里出现最多的那个（且必须早于今天）。
    #   为什么要这道闸：停牌/退市股的末行停在几个月前（如 000004 停在 2026-07-13），
    #   拿它的收盘去比「今天的昨收」不是同一时点，算出来的系数是假的
    #   （实测 000004 得 0.9020、300029 得 1.1667，都会把历史价格改错）。
    from collections import Counter as _Counter
    _cnt = _Counter()
    for _v in stocks.values():
        _d = _v.get('last_date') or ''
        if _d and td and _d < td:
            _cnt[_d] += 1
    prev_day = ''
    if _cnt:
        prev_day = _cnt.most_common(1)[0][0]

    exrights = {}
    stale = 0
    for _code, _v in stocks.items():
        if not (prev_day and _v.get('last_date') == prev_day):
            stale += 1          # 末行不是上一个交易日（停牌/退市/今天已写过）⇒ 不碰
            continue
        _prev = float(_v.get('prev_close') or 0)
        _lc = float(_v.get('last_close') or 0)
        if not (_prev > 0 and _lc > 0):
            continue
        # 🔴 两道闸，缺一不可（否则会在没除权的股票上算出假系数，永久污染历史）:
        #   ① 绝对差 >= 0.005 元 —— A 股报价都是 2 位小数、本地存 3 位，正常情况下
        #      no-ex-div 的两个昨收**完全相等**；半分为界足以吸收舍入噪声。
        #      没有这道闸，1400 元的股票差 1 分就是 7e-6 的相对差，会算出 1.000007 的假系数。
        #   ② 相对差 > 1e-5 —— 挡住「绝对差够大但相对可以忽略」的情形
        #      （例：1 分钱 ÷ 1400 元）。
        if abs(_prev - _lc) < 0.005:
            continue
        _fac = _prev / _lc
        if abs(_fac - 1.0) <= 1e-5:
            continue
        _fac = round(_fac, 6)
        exrights[_code] = {'factor': _fac, 'prev': _prev, 'last_close': _lc}
        _v['factor'] = _fac

    # 🔴 v2.170 安全阀（2026-10-21 新增）：真除权日全市场也就几十只 —— 2026-10-22 有 26 只、
    #   10-23 有 42 只（含 688809 十送四），是今年最密集的两天。如果某一轮判出"超过 500 只除权"，
    #   那不可能是真事件，只可能是**快照口径错了**：典型场景是跨零点/收盘写盘之后再跑，
    #   快照给的 prev_close 还停在上一交易日，于是全市场都被算成"除权 1.7%"。
    #   这种情况下用错因子把整段历史乘歪是**不可逆**的（CSV 没有版本），所以宁可这一轮
    #   全清空不写（factost 置空 ⇒ 下游 append_csv 只追加当行、不做历史修正），并留下证据。
    _eraw = len(exrights)          # v2.193: 留证据给下游气泡（清空后 len 就是 0 了）
    if len(exrights) > 500:
        try:
            _lg = os.path.join(HERE, 'fuyao_fetch.log')
            with open(_lg, 'a', encoding='utf-8') as _f:
                _f.write('%s [除权安全阀] 判出 %d 只除权(阈值500) ⇒ 全部作废, '
                         'price_src=%s trade_date=%s prev_day=%s\n'
                         % (time.strftime('%Y-%m-%d %H:%M:%S'), len(exrights),
                            'fuyao' if use_fuyao else 'tdx', td, prev_day))
        except Exception:
            pass
        for _v in stocks.values():
            _v.pop('factor', None)
        exrights = {}

    result.update({
        'ok': True,
        'price_src': 'fuyao' if use_fuyao else 'tdx',
        # v2.170: 这次真花掉的通达信调用次数 —— 取价走同花顺时只剩 15:35 那 1 次市值,
        #   11:35 是 0。审计账里每笔 _audit_call 才是权威, 这里只是给气泡看的预估。
        'tdx_calls': (0 if use_fuyao else 1) + (1 if with_mcap else 0),
        'trade_date': td,
        'hq_date': hq_date,
        'prev_day': prev_day,
        'count': len(stocks),
        'suspects': len(exrights),
        'exrights': exrights,
        # v2.193: >0 表示安全阀把这一轮判出的除权整体作废了（历史一个字节都没改）
        'exrights_dropped': _eraw if _eraw > 500 else 0,
        'stale': stale,
        'stocks': stocks,
        'elapsed': round(time.time() - t0, 2),
    })
    # v2.164: 收尾记账 —— 正常路径唯一的落账点
    # v2.170: fuyao 路径没开账 ⇒ _end 内部自己跳过
    _end(len(stocks), {'ok': True, 'trade_date': td, 'hq_date': hq_date,
                       'exrights': len(exrights)})
    if verbose:
        _nmc = sum(1 for _v in stocks.values() if _v.get('mcap') or _v.get('fmcap'))
        print(f'[{("fuyao" if use_fuyao else "tdx")}] 上一交易日 {prev_day or "?"} | '
              f'除权 {len(exrights)} 只 | 末行不匹配跳过 {stale} 只 | 市值 {_nmc} 只',
              file=sys.stderr, flush=True)
    return result


def _divid_factor(tqs, code, prev, last_close, cwd0, hq_date=''):
    """🚫 v2.159 (2026-10-20) 起**不再被调用** —— 保留仅供参考，别重新接回去

    停用原因: 它走 tqs.get_divid_factors()，**单只调用、花积分**。而正确做法是
    直接用快照自带的 LastClose 除本地末行收盘（见 do_snapshot 里的说明），
    免费且更准。这个函数留着是因为它记录了交易所参考价公式的推导过程。

    查今日除权因子 → (factor, 明细) 或 (None, '')

    三个价格的尺度必须先讲清楚（这是最容易写反的地方）:
      last_close = 本地 CSV 末行收盘 = **未复权**的昨收（旧尺度）
      prev       = 通达信 LastClose  = **已按今日除权调整**的昨收（今日尺度）= 除权参考价
      历史价在旧尺度 ⇒ 换到今日尺度要乘 factor = prev / last_close

    参考价公式（交易所口径）:
      参考价 = (前收盘 − 每股派息 + 配股价×配股比例) / (1 + 每股送转 + 配股比例)

    校验两层:
      ① 最近一条除权记录的日期必须 **就是今天** —— 否则是多年前的派息记录，套上去必错
         （2026-10-18 实测: 600519 有 30 条记录，最后一条是 2026-06-26，今天根本不是除权日）
      ② 公式算出的参考价要与通达信 LastClose 吻合（差 <2%），否则说明不是除权（是数据错）
    """
    sym = tdx_sym(code)
    try:
        rows = tqs.get_divid_factors(sym)
    except Exception:
        return (None, '')
    finally:
        try:
            os.chdir(cwd0)
        except Exception:
            pass
    # 兼容 DataFrame / list-of-dict
    # 注意: get_divid_factors 返回的 DataFrame 里 **Date 是 index 不是列**，
    #   用 to_dict('records') 会把日期丢掉 ⇒ 这里单独把 index 抓出来。
    recs = []
    try:
        if hasattr(rows, 'to_dict'):
            recs = rows.to_dict('records')
            idx = list(rows.index)
            for i, r in enumerate(recs):
                if i < len(idx) and 'Date' not in r:
                    r['Date'] = str(idx[i])
        elif isinstance(rows, list):
            recs = [r for r in rows if isinstance(r, dict)]
    except Exception:
        recs = []
    if not recs:
        return (None, '')
    rec = recs[-1]
    rdate = str(rec.get('Date') or '').strip()
    # ① 除权日必须是今天（hq_date 形如 20261018，记录的 Date 形如 2026-10-18）
    if hq_date and len(hq_date) == 8:
        want = f'{hq_date[:4]}-{hq_date[4:6]}-{hq_date[6:8]}'
        if rdate.replace('/', '-') != want:
            return (None, '')
    try:
        bonus = float(rec.get('Bonus') or 0) / 10.0        # 每股派息
        share = float(rec.get('ShareBonus') or 0) / 10.0   # 每股送转
        ap = float(rec.get('AllotPrice') or 0)
        an = float(rec.get('Allotment') or 0) / 10.0       # 每股配股
    except (TypeError, ValueError):
        return (None, '')
    if prev <= 0 or last_close <= 0:
        return (None, '')
    denom = 1.0 + share + an
    if denom <= 0:
        return (None, '')
    ref = (last_close - bonus + ap * an) / denom
    if ref <= 0:
        return (None, '')
    # ② 公式参考价 与 通达信 LastClose 互证
    if abs(ref - prev) / prev > 0.02:
        return (None, '')          # 对不上 ⇒ 不是除权, 宁可不动
    fac = prev / last_close
    detail = (f'{rdate} 派{bonus:.4f} 送{share:.4f} 配价{ap:.2f} 配{an:.4f} '
              f'→ 参考价{ref:.3f} (通达信{prev:.3f}) 因子{fac:.6f}')
    return (fac, detail)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', default='snapshot', choices=['snapshot'])
    ap.add_argument('--out', default='')
    ap.add_argument('--selftest', action='store_true')
    ap.add_argument('--quiet', action='store_true')
    ap.add_argument('--who', default='cli')      # v2.164: 审计账里的来源标记
    a = ap.parse_args()

    if a.selftest:
        cfg = load_cfg()
        lib = cfg['lib_dir']
        print(f'lib_dir = {lib}  exists={os.path.isdir(lib)}')
        _ts = token_status(lib)
        print(f'key 已配置={_ts["configured"]} 长度={_ts["length"]} '
              f'ini={_ts["ini_dir"]}')
        sys.path.insert(0, lib)
        try:
            from tdxaidata import tqs
            import tdxaidata
            print(f'tdxaidata version = {getattr(tdxaidata, "__version__", "?")}')
            r = tqs.get_more_info_batch(stock_list=['600000.SH'],
                                        field_list=['HqDate', 'ZTPrice', 'DTPrice'])
            print(f'probe = {r}')
        except Exception as e:
            print(f'失败: {e}')
            return 1
        return 0

    res = do_snapshot(verbose=not a.quiet, who=a.who)
    txt = json.dumps(res, ensure_ascii=False, default=str)
    if a.out:
        tmp = a.out + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fh:
            fh.write(txt)
        os.replace(tmp, a.out)
        if not a.quiet:
            n = res.get('count', 0)
            print(f'[tdx] ok={res.get("ok")} count={n} '
                  f'elapsed={res.get("elapsed")}s → {a.out}', file=sys.stderr, flush=True)
    else:
        sys.stdout.write(txt)
    return 0 if res.get('ok') else 2


if __name__ == '__main__':
    sys.exit(main())
