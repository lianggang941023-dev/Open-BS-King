# -*- coding: utf-8 -*-
"""同花顺快照导入 (2026-08-04 新建)

用法: python import_ths_snapshot.py
用途: 用户从同花顺动态分组导出5个全市场文件后, 一键完成:
  1. K线最后一天增量更新 (快照含当日 O/H/L/C/V, 零网络请求, 秒级)
  2. 重建 industry_map.csv (细分/三级行业) + concept_map.csv (概念) + industry_parent.csv (细分→上级)

数据源文件 (选股池/): 沪市主板.xlsx / 深市主板.xlsx / 创业板.xlsx / 科创板.xlsx / ST股票.xlsx
  (文件名/目录由终端"数据源设置"面板配置, 见 quant_terminal/data_sources.json)
  v4.0 (2026-09-12): **按列名定位**, 列顺序/列数量任意 (旧版按位置取 [21]开盘 等, 用户精简
  表头后全部错位 — 且错位不会报错, 会把成交量当收盘价写进K线 / 把开盘价写进概念map)。
    必需列: 代码 名称 现价 最新成交量 细分行业 所属概念 开盘 昨收 最高 最低
    可选列: 股东户数 — 只与 holder_num.csv 比对出"差异数"提示, **不写入任何文件**
  列名一个字都不能改 (改名 = 找不到 = 拒绝执行, 好过静默写错)。
  v5.0 (2026-09-12): **行业粒度 二级 → 三级(细分行业)**。industry_map.csv 改读「细分行业」
  列并去 Ⅲ 后缀 — 同花顺给"三级与二级同名"的行业加 Ⅲ 区分, 且只在沪市主板/ST 文件里带,
  不去后缀会把「证券」劈成 16深+34沪 两个板块。同时落 industry_parent.csv (细分→上级二级名)。

安全规则:
  - 除权检测 v2.7 (2026-09-03, 9/3 事故后重构): 快照昨收 vs 本地末行收盘, **任何差异都拦**:
      |差| ≤ 0.005元                              = 无除息 (同日同源只可能有舍入差) → 正常追加
      差 < 0 且幅度 ≤ 8% 且末行=前一交易日        = 纯派息除息 → 本地整段平移 (差=每股派息, 离线秒改)
      差 < 0 其他 (含送转比例因子/末行非昨交易日) = 疑似除权, 跳过, 腾讯API兜底重拉
      差 > 0.005元 (昨收反高, 不可能除息)        = 旧规则: >涨跌停上限+1% 才拦 (分板块 11%/21%)
    8% 分界依据: A股现金股息率天花板 ~7% (银行/煤炭), 最小送转 10送1 = 9.09% 起 → 8% 安全。
    纯派息 = 加减平移 (交易所参考价 = 昨收−每股派息, 差值本身=派息额);
    送转 = 比例缩放, 平移法随历史深度放大误差 → 必须腾讯 qfq 官方口径。
  - 平移只改除息日之前的历史, 当天行不动; 平移后校验 历史低价+平移量>0 (打穿0轴改腾讯兜底)
  - 成交量快照单位=股, 本地=手 → 写入时÷100
  - 已含当日K线的股票跳过 (幂等)
"""
import io, os, sys, csv, glob
from collections import Counter
from datetime import datetime, timedelta

# v3.3 (2026-08-24): GBK 控制台 print ❌ 会 UnicodeEncodeError 崩溃 (终端按钮/命令行都崩,
# 拦截信息无法透传)。统一 UTF-8 输出, 无法编码的字符用 ? 代替。
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)
# v1 数据源设置 (2026-08-29): 目录/快照文件清单从配置读 (默认=现状, 不改变现有行为)
from quant_terminal import datasource as ds
# v2.273 (2026-11-11 用户 m08653): 第 9 列「自由流通股本」的列名从 mcap 取, 避免两处各写一遍。
from quant_terminal import mcap as _MC
M_CAP_FREE = getattr(_MC, 'FREE', '')
KLINE_DIR = ds.resolve(ds.get('local', 'kline_dir', os.path.join('kline_data', 'daily')))
POOL_DIR = ds.resolve(ds.get('local', 'snapshot_dir', '选股池'))
MAP_DIR = os.path.dirname(ds.resolve(ds.get('local', 'industry_map', os.path.join('kline_data', 'industry_map.csv'))))

SNAPSHOT_FILES = [
    (fname, 1) for fname in ds.get('local', 'snapshot_files', [
        '选股池/沪市主板.xlsx', '选股池/深市主板.xlsx', '选股池/创业板.xlsx',
        '选股池/科创板.xlsx', '选股池/ST股票.xlsx',
    ])
]
SNAPSHOT_BASE = ds.resolve(ds.get('local', 'snapshot_dir', '选股池'))  # v2.17: 面板多文件路径
# 除权阈值 = 涨跌停上限+1% 容错 (v2.6): 涨停非固定整数(9.98/10.02/20.04),
# 用代码前缀区分: 创业板30/301 + 科创板68/689 = 21%, 主板60/00/001/002/003 = 11%
# v2.36 (2026-09-18): 北交所 30% 板 → 31.0 (用 11% 会把它的涨跌停/除权判据判错)。
# 传进来的 code 对北交所是 'BZ######'(自带前缀), 与 server.py _limit_cap 同款写法。
LIMIT_CAP = lambda code: (31.0 if code[:2].upper() in ('BZ', 'BJ')
                          else 21.0 if code.startswith(('30', '68')) else 11.0)

# ── v4.0 (2026-09-12): 表头名定位 ────────────────────────────────────────
# 必需列: 缺一列即**拒绝执行**(不写任何文件) — 静默跳过会让 K线/map 悄悄写坏
REQUIRED_COLS = ('代码', '名称', '现价', '最新成交量', '细分行业', '所属概念',
                 '开盘', '昨收', '最高', '最低')
# 行业列兼容 (二选一, 不能放进 REQUIRED_COLS — 否则另一名字会被误报缺列):
#   新格式 '所属行业'      = 已解析好的二级行业名 (v5.0 起只用于建 细分→上级 映射)
#   旧格式 '所属同花顺行业' = 【一级-二级-三级】路径 (三级列缺失时回退取第二段)
IND_CANDIDATES = ('所属行业', '所属同花顺行业')
# v5.0 (2026-09-12): 行业粒度 二级(所属行业) → 三级(细分行业)。同花顺对"三级名与二级名
# 相同"的行业加罗马数字 Ⅲ 区分, 且**只在部分导出文件里出现** (沪市主板/ST 带 Ⅲ, 深市主板/
# 创业板/科创板不带) — 不去后缀会把同一行业劈成两个板块 (证券 16深 + 证券Ⅲ 34沪, 各自
# 算指数)。去后缀后 283 类 → 257 类。见 _norm_sub_industry。
SUB_COL = '细分行业'
HOLDER_COL = '股东户数'          # 可选列: 只做哨兵比对, 不写入任何文件


def _index_header(header_line: str):
    """表头行 → ({列名: 索引}, 缺失的必需列)

    重名列取第一个; 空列名(同花顺导出末尾常带一列空表头)直接忽略。
    """
    hdr = [h.strip() for h in header_line.rstrip('\r\n').split('\t')]
    ix = {}
    for i, h in enumerate(hdr):
        if h and h not in ix:
            ix[h] = i
    missing = [c for c in REQUIRED_COLS if c not in ix]
    if not any(c in ix for c in IND_CANDIDATES):
        missing.append('或'.join(IND_CANDIDATES))
    return ix, missing


def _cell(p: list, i) -> str:
    """按索引安全取单元格 (行短于表头时返回 '', 不抛 IndexError)"""
    return p[i].strip() if (i is not None and 0 <= i < len(p)) else ''


def _norm_industry(raw: str) -> str:
    """行业归一 → **二级**行业名 (v5.0 起仅用于建 细分→上级 映射, 不再进 industry_map.csv)

    '元件'                     → '元件'   (新格式 所属行业, 已是二级名)
    '【电子-元件-印制电路板】' → '元件'   (旧格式 所属同花顺行业, 取第二段)
    全市场实测两者等价 5018:0 (差异 3 只均为未上市股的 '--')。
    """
    raw = (raw or '').replace('【', '').replace('】', '').strip()
    parts = [x.strip() for x in raw.split('-') if x.strip()]
    if not parts:
        return ''
    return parts[1] if len(parts) >= 2 else parts[0]


def _norm_sub_industry(raw: str) -> str:
    """细分行业归一 → **三级**行业名, 并去掉 Ⅲ 后缀 (v5.0 2026-09-12)

    '印制电路板'               → '印制电路板'   (新格式 细分行业, 已是三级名)
    '【电子-元件-印制电路板】' → '印制电路板'   (旧格式, 取第三段)
    '证券Ⅲ'                    → '证券'         (与二级同名 → 同花顺加 Ⅲ 区分)

    ⚠ Ⅲ 后缀必须在**入库前**去掉 (实测 2026-09-12): 同一个「证券」二级行业,
      沪市主板/ST 文件写 '证券Ⅲ' (34只), 深市主板/创业板/科创板写 '证券' (16只) —
      不去后缀 = 一个真实行业被劈成两个板块各自合成指数, 成分腰斩且无人察觉。
      27 个带 Ⅲ 的细分行业, 上级**全部**就是去掉后缀后的那个二级名, 去掉安全。
    """
    raw = (raw or '').replace('【', '').replace('】', '').strip()
    parts = [x.strip() for x in raw.split('-') if x.strip()]
    if not parts:
        return ''
    name = parts[2] if len(parts) >= 3 else (parts[1] if len(parts) >= 2 else parts[0])
    return name.replace('Ⅲ', '').strip()


def _cn_to_float(s: str) -> float:
    """同花顺中文数字 → float, 无效返回 0.0

    '3.69万' → 36900.0   '8223' → 8223.0   '1.2亿' → 120000000.0   '--' → 0.0
    ⚠ 同花顺"万"只有 3 位有效数字 (36892 → 显示 '3.69万'), 因此股东户数比对
      必须用**相对误差**判据 — 绝对差会把大量正常舍入误报成差异。
    """
    s = (s or '').strip()
    if not s or s in ('--', '-'):
        return 0.0
    try:
        if s.endswith('万'):
            return float(s[:-1]) * 10000
        if s.endswith('亿'):
            return float(s[:-1]) * 100000000
        return float(s.replace(',', ''))
    except ValueError:
        return 0.0


def check_headers() -> list:
    """校验5个快照的表头 → [(文件名, 问题)]; 空列表 = 全部通过

    ⚠ 必须在写任何文件**之前**调用 (2026-09-12 教训): 用户精简表头后, 旧版按位置
    取值的逻辑会把 p[5]=所属概念 当成行业、p[6]=开盘 当成概念, 而覆盖率检查照样
    100% 通过 → 两张 map 被静默覆盖。
    """
    problems = []
    for fname, _ in SNAPSHOT_FILES:
        fpath = ds.file_path(fname, SNAPSHOT_BASE)
        if not os.path.exists(fpath):
            problems.append((fname, '文件不存在'))
            continue
        try:
            with io.open(fpath, encoding='gbk', errors='replace') as fh:
                first = fh.readline()
        except Exception as e:
            problems.append((fname, f'读取失败: {e}'))
            continue
        _ix, missing = _index_header(first)
        if missing:
            problems.append((fname, '缺列: ' + ' '.join(missing)))
    return problems


def snapshot_date(fpath: str) -> str:
    """快照日期 = 文件修改时间推断 (导出当天收盘; 周末导出则前推上一交易日)
    v2.5 (2026-08-11): 修复跨天运行把昨日快照写入今日日期的问题"""
    mt = datetime.fromtimestamp(os.path.getmtime(fpath)).date()
    while mt.weekday() >= 5:
        mt -= timedelta(days=1)
    return mt.strftime('%Y-%m-%d')


def _prev_trade_day(d: str) -> str:
    """快照日期前一个交易日 (跳过周末近似; 节假日边界退化 → 由腾讯兜底路径兜住, 不会误平移)"""
    dt = datetime.strptime(d, '%Y-%m-%d') - timedelta(days=1)
    while dt.weekday() >= 5:
        dt -= timedelta(days=1)
    return dt.strftime('%Y-%m-%d')


def load_snapshot() -> tuple:
    """读5个快照文件 → ({6位代码: (date, open, close, high, low, volume手, 昨收, 名称)}, 量列健康统计)

    v3.1 (2026-08-21): 返回 (snap, stats)。stats['missing_ratio'] = 量列 '--'/空 占比,
    供 __main__ 在写K线前做健康检查——18:22 事故: 快照量列整列未加载(同概念列), 1947只
    8/21 行被静默写入 volume=0, 幂等机制使其永久滞留 CSV。
    v4.0 (2026-09-12): **按列名定位** (旧版按位置 p[21]开盘/p[23]最高…, 用户精简表头后
    全部错位且不报错)。stats['holders'] = {代码: 股东户数} 仅供 __main__ 哨兵比对,
    不写入任何文件; stats['header_errors'] = 缺列表头文件 (该文件整体跳过)。
    """
    snap = {}
    holders = {}
    total = miss = price_miss = 0
    header_errors = []
    for fname, _ in SNAPSHOT_FILES:
        fpath = ds.file_path(fname, SNAPSHOT_BASE)
        if not os.path.exists(fpath):
            print(f'⚠ 缺少源文件: {fname}'); continue
        sdate = snapshot_date(fpath)
        with io.open(fpath, encoding='gbk', errors='replace') as fh:
            lines = fh.readlines()
        if not lines: continue
        ix, missing = _index_header(lines[0])
        if missing:
            header_errors.append((fname, '缺列: ' + ' '.join(missing)))
            print(f'⚠ {fname} 表头缺列, 跳过: {" ".join(missing)}'); continue
        i_code, i_name, i_price = ix['代码'], ix['名称'], ix['现价']
        i_vol, i_open, i_pc = ix['最新成交量'], ix['开盘'], ix['昨收']
        i_high, i_low = ix['最高'], ix['最低']
        i_holder = ix.get(HOLDER_COL)          # v4.0 可选列: 只收集, 不落盘
        for line in lines[1:]:
            p = line.rstrip('\r\n').split('\t')
            code = _cell(p, i_code)
            if not code: continue
            num = code[2:] if code[:2] in ('SH', 'SZ') else code
            # v2.36 (2026-09-18): 放行北交所, 键保留 'BZ######' 原样 —— 与 update_kline :290
            # 的归一化、与两张 map 的键三者一致, 所以 :291 的匹配一行都不用改。
            # 旧写法 isdigit() 把 344 只北交所全挡在 snap 之外 → 每次误报「快照缺失 344」,
            # 把真实缺失淹没。沪深分支逐字节未变。
            if not num.isdigit() and not (num[:2].upper() == 'BZ' and num[2:].isdigit()):
                continue
            total += 1
            # v3.4 (2026-09-01): 现价列缺失统计 — 区分真停牌 vs 列未加载事故
            # (9/1 事故: 深主板现价列整列未加载 1368/1420 现价 '--' 但量列有值,
            #  旧逻辑走 float('--') ValueError 静默跳过, 健康检查没拦住;
            #  正常停牌基线仅 13/5825 = 0.22%)
            price_raw = _cell(p, i_price)
            if price_raw in ('--', '-', ''):
                price_miss += 1; continue  # 停牌 或 现价列未加载 → 计入 price_ratio
            v = _cell(p, i_vol)
            # v3.3 (2026-08-24): 停牌股 (现价也是 --) 无量是正常的, 不算污染;
            # 只有"现价有但量列 --"才是列未加载 (8/21 事故特征, 8/24 创业板整段复发)
            if v in ('--', '-', ''):
                miss += 1; continue  # 量列缺失, 不收入快照 (避免写 V=0)
            try:
                close = float(price_raw)
                opn = float(_cell(p, i_open)); high = float(_cell(p, i_high))
                low = float(_cell(p, i_low))
            except ValueError:
                continue
            if close <= 0: continue
            # 昨收 (除权检测用, v2.5): 涨停/跌停不误判
            pc_raw = _cell(p, i_pc)
            try:
                pc = float(pc_raw) if pc_raw not in ('', '--', '-') else 0.0
            except ValueError:
                pc = 0.0
            # 成交量: 单位 股 → 手 (÷100); 支持 万/亿/0.00/--
            vol = 0
            try:
                if v.endswith('万'): vol = float(v[:-1]) * 10000 / 100
                elif v.endswith('亿'): vol = float(v[:-1]) * 100000000 / 100
                elif v.replace('.', '').replace('-', '').isdigit(): vol = float(v) / 100
            except ValueError:
                vol = 0
            name = _cell(p, i_name)
            snap[num] = (sdate, opn, close, high, low, vol, pc, name)
            if i_holder is not None:
                hv = _cn_to_float(_cell(p, i_holder))
                if hv > 0: holders[num] = hv
    return snap, {'total': total, 'missing': miss,
                  'missing_ratio': miss / total if total else 0,
                  'price_missing': price_miss,
                  'price_ratio': price_miss / total if total else 0,
                  'holders': holders, 'header_errors': header_errors}


def update_kline(snap: dict) -> dict:
    """K线最后一天增量更新 (幂等)"""
    stats = {'updated': 0, 'shift': [], 'skip_already': 0, 'skip_diff': [],
             'skip_no_data': 0, 'missing': []}
    files = sorted(glob.glob(os.path.join(KLINE_DIR, '*.csv')))
    for fpath in files:
        # 2026-09-18: 兼容北交所 'BZ920002.csv' —— 先剥扩展名再按前缀归一, 键保留 'BZ'。
        # 沪深行为**完全不变** ('SH600519' → '600519', 与旧写法 [2:8] 同值)。
        # ⚠ 但光改这里**没用**: load_snapshot 的 isdigit() 把北交所全挡掉了, snap 里 0 个
        # BZ 键 → 这里照样每次都报「快照缺失」。真正的修复是那边放行 (v2.36), 两处必须成对。
        stem = os.path.basename(fpath)[:-4]
        code = stem[2:] if stem[:2] in ('SH', 'SZ') else stem
        if code not in snap:
            stats['missing'].append(code); continue
        date, opn, close, high, low, vol, prev_close, name = snap[code]
        if vol <= 0 and opn <= 0:
            stats['skip_no_data'] += 1; continue  # 停牌
        with open(fpath, encoding='utf-8') as fh:
            _rd = csv.DictReader(fh)
            rows = list(_rd)
            # v2.133 (2026-10-14): 列名单一来源 = **文件自己的表头**, 不再写死。
            #   本节原先在写回处硬编码 6 列; 2026-10-01 表头扩到 8 列(总市值/流通市值)后,
            #   DictReader 读进来 8 个键 / DictWriter 只认 6 个 -> ValueError,
            #   而 open(...,'w') 已经先把文件截断了 -> **该股整份K线归零**。
            #   (10-14 事故: files 排序后第一个 BZ920000.csv 被截成 56 字节)
            #   跟着文件走: 8 列表头进 8 列出, 老 6 列文件照样不炸。
            _fnames = _rd.fieldnames or ['date', 'open', 'close', 'high', 'low', 'volume']
        if not rows: continue
        last = rows[-1]
        last_date = last['date']
        # v2.5 (2026-08-11): 快照日期 = 文件修改时间推断, 不再用运行日 (跨天运行写错日期bug)
        snap_date = date
        if last_date >= snap_date:
            stats['skip_already'] += 1; continue  # 已最新
        # 除权检测 v2.7 (2026-09-03): 昨收 vs 本地末行收盘 — 详见文件头"安全规则"
        local_close = float(last['close'])
        intercept = False
        shift_yuan = 0.0
        if local_close > 0 and prev_close > 0:
            # 交易所除息参考价 = 昨收 − 每股派息 → 负差本身就是每股派息额 (前复权整段下移)
            dy = prev_close - local_close          # 元, 带符号 (除息 → 负)
            pct = dy / local_close * 100
            if abs(dy) <= 0.005:
                pass                               # 无除息 (同日同源只可能有舍入差)
            elif dy < 0:
                # 纯派息 (≤8%): 末行必须是前一交易日才敢平移 — 否则=缺口/送转 →
                # 腾讯兜底 (送转是比例缩放, 平移法随历史深度放大误差)
                if pct >= -8.0 and last_date == _prev_trade_day(snap_date):
                    shift_yuan = dy                 # 带符号直接加 (9/3 教训: 禁止再叠负号)
                else:
                    intercept = True
            elif pct > LIMIT_CAP(code) or name.startswith(('XD', 'XR', 'DR')):
                # 昨收反高=不可能除息: 旧规则大跳空才拦; XD名称明证除息日却反高
                # = 本地已调过头(半修复态, 如曾误平移2倍) → 拦截让腾讯 qfq 归位
                intercept = True
        elif local_close > 0:
            # 昨收缺失: 无法精确检测, 退回 v2.6 大跳空规则 (ref=快照现价);
            # XD/XR/DR 名称 = 同花顺除息标记, 直接拦 (昨收列缺失时唯一线索)
            diff = (close - local_close) / local_close * 100
            if abs(diff) > LIMIT_CAP(code) or name.startswith(('XD', 'XR', 'DR')):
                intercept = True
        if intercept:
            stats['skip_diff'].append(code); continue
        if shift_yuan:
            # 打穿0轴保护: 历史最低价 + 平移量 ≤ 0 → 平移非法 → 改腾讯兜底
            if any(float(r['low']) + shift_yuan <= 0 for r in rows):
                stats['skip_diff'].append(code); continue
            for r in rows:                          # 只平移除息日之前, 当天行不动
                for k in ('open', 'close', 'high', 'low'):
                    r[k] = f'{float(r[k]) + shift_yuan:.3f}'
            stats['shift'].append((code, shift_yuan))
        _new = {'date': snap_date, 'open': f'{opn:.3f}', 'close': f'{close:.3f}',
                'high': f'{high:.3f}', 'low': f'{low:.3f}', 'volume': f'{vol:.0f}'}
        # v2.273 (2026-11-11 用户 m08653): 第 9 列「自由流通股本」同花顺快照里没有 ——
        #   但它是股本类常数列（不随价格变），新追加的行直接**沿用上一行的值**，
        #   不写就会让新行该列变空 = 把已有数据丢一天。没有该列的文件(_fnames 里没有)则什么也不做。
        if M_CAP_FREE and M_CAP_FREE in _fnames:
            _prev_free = (last.get(M_CAP_FREE) or '').strip()
            if _prev_free:
                _new[M_CAP_FREE] = _prev_free
        rows.append(_new)
        # v2.133 (2026-10-14): 原子写 (临时文件 + os.replace)。
        #   原先是直接 open(fpath,'w') -- 只要 writerows 抛错, 文件已被截断,
        #   该股K线就永久没了 (10-14 BZ920000 就是这么坏的, 只能从备份捞回)。
        _tmp = fpath + '.tmp'
        with open(_tmp, 'w', encoding='utf-8', newline='') as fh:
            # v2.134 (2026-10-14): csv 模块默认 lineterminator='\r\n' —— 必须钉死成 \n。
            #   10-14 事故: 少这一个参数, 5562 个 daily/*.csv 被写成 CRLF,
            #   下一棒 mcap._render 按 '\n' 切行 ⇒ 表头字段带上 '\r' ⇒ 两列市值被重复追加。
            w = csv.DictWriter(fh, fieldnames=_fnames, lineterminator='\n')
            w.writeheader()
            w.writerows(rows)
        os.replace(_tmp, fpath)
        stats['updated'] += 1
    return stats


def _read_map_file(path: str) -> dict:
    """旧 map → {代码: 值}; 缺失/损坏一律返回 {} — 纯提示用, 绝不抛异常打断导入"""
    try:
        if not os.path.exists(path):
            return {}
        with io.open(path, encoding='utf-8-sig', newline='') as fh:
            rd = csv.reader(fh)
            next(rd, None)                                    # 跳过表头
            return {r[0]: r[1] for r in rd if len(r) >= 2 and r[0]}
    except Exception:
        return {}


def _map_change_lines(label: str, old: dict, new: dict, is_concept: bool) -> list:
    """旧/新 map 对比 → 提示行列表

    v5.1 (2026-09-18): 重建一直是**静默覆盖**, 用户看不到"这次到底抓到了什么变化"。
    本函数只读不写 —— 覆盖前把旧文件读进来比一遍, 结果**仅作提示**, 不参与任何写入决策。
    行业名短 → 直接展示 旧→新; 概念是 | 拼接的长串 → 只展示概念增删, 否则一行能刷满屏。
    """
    if is_concept:
        # ⚠ 两种形态: 磁盘读回来是 '|' 串, 内存里的 con_by_code 是 **list** (落盘时才 join)
        j = lambda d: {k: (v if isinstance(v, str) else '|'.join(v)) for k, v in d.items()}
        old, new = j(old), j(new)
    if not old:
        return [f'Map变化({label}): 无旧文件可比 | 本次写入 {len(new)} 只']
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))
    changed = sorted(k for k in set(old) & set(new) if old[k] != new[k])
    if not (added or removed or changed):
        return [f'Map变化({label}): 0 只 (与上一版一致)']
    seg = []
    if added:   seg.append(f'新增{len(added)}')
    if removed: seg.append(f'移除{len(removed)}')
    if changed: seg.append(f'变更{len(changed)}')
    lines = [f'Map变化({label}): ' + ' '.join(seg)]
    shown = 0
    for k in changed[:6]:                                     # 变更最值得看, 优先展示
        if is_concept:
            so, sn = set(old[k].split('|')), set(new[k].split('|'))
            d = [('+' + ','.join(sorted(sn - so))) if sn - so else '',
                 ('-' + ','.join(sorted(so - sn))) if so - sn else '']
            lines.append(f'    {k}: ' + ' '.join(x for x in d if x))
        else:
            lines.append(f'    {k}: {old[k]} → {new[k]}')
        shown += 1
    for k in (added + removed)[:4]:
        lines.append(f'    {"新增" if k in added else "移除"} {k}: {(new.get(k) or old.get(k) or "")[:60]}')
        shown += 1
    total = len(added) + len(removed) + len(changed)
    if total > shown:
        lines.append(f'    …另有 {total - shown} 只未列出')
    return lines


def rebuild_maps(snap_meta=None) -> dict:
    """重建 industry_map.csv (细分/三级行业) + concept_map.csv (概念) + industry_parent.csv (细分→上级)

    v3.0 (2026-08-21): 健康检查 — 快照行业/概念列大面积缺失(同花顺导出列未加载, 如
    深主板'所属同花顺行业'/'所属概念'整列为 --) 时**拒绝写文件**, 保护旧 map 不被
    污染覆盖。K线更新不受影响, 用户重新导出后重跑本脚本即可恢复。
    v5.0 (2026-09-12): 行业粒度 二级 → 三级(细分行业)。industry_map.csv 的「行业」列
    从此装的是**细分行业名**(去 Ⅲ 后缀后); 另落 industry_parent.csv 供前端表头悬停
    显示"上级所属-元件"。
    """
    # 读快照: 代码 -> (细分行业, 概念列表)  先读优先去重
    ind_by_code, con_by_code = {}, {}
    ind_set, con_set = set(), set()
    all_seen = set()  # 快照全部股票(含双空), 健康检查分母
    sub2parent = {}   # 细分行业 -> Counter(上级二级行业名)
    for fname, _ in SNAPSHOT_FILES:
        fpath = ds.file_path(fname, SNAPSHOT_BASE)
        if not os.path.exists(fpath): continue
        with io.open(fpath, encoding='gbk', errors='replace') as fh:
            lines = fh.readlines()
        if not lines: continue
        ix, missing = _index_header(lines[0])
        if missing:
            print(f'⚠ {fname} 表头缺列, 跳过: {" ".join(missing)}'); continue
        i_code = ix['代码']
        i_sub = ix.get(SUB_COL)
        i_ind = ix['所属行业'] if '所属行业' in ix else ix['所属同花顺行业']
        i_con = ix['所属概念']
        for line in lines[1:]:
            p = line.rstrip('\r\n').split('\t')
            code = _cell(p, i_code)
            if not code: continue
            num = code[2:] if code[:2] in ('SH', 'SZ') else code
            # ⚠ 这里**不能**加 isdigit 过滤: 北交所代码是 'BZ920023' 形态 (前缀非 SH/SZ,
            #   原样保留), 过滤会把 3 只北交所从两张 map 里抹掉 (2026-09-12 验证发现)。
            if num in ind_by_code: continue  # 先读优先
            all_seen.add(num)
            # 细分行业(三级, 去 Ⅲ 后缀); 细分列缺失时回退二级名 (旧格式文件)
            sub = _norm_sub_industry(_cell(p, i_sub)) if i_sub is not None else ''
            ind2 = _norm_industry(_cell(p, i_ind))
            if not sub: sub = ind2                    # 回退: 没有细分列就用二级名
            if sub: ind_by_code[num] = sub; ind_set.add(sub)
            if sub and ind2:
                sub2parent.setdefault(sub, Counter())[ind2] += 1
            # 概念 (-- 是同花顺"无数据"占位, 视为缺失)
            cons = [c.strip() for c in _cell(p, i_con).replace('【', '').replace('】', '').split(';')
                    if c.strip() and c.strip() != '--']
            if cons: con_by_code[num] = cons; con_set.update(cons)
    # 健康检查 v3.2: 覆盖率 = 有值股票 / 快照全部去重股票
    # 阈值定案 (2026-08-21, 用户要求严谨化, 历史基线支撑):
    #   正常基线: 行业100%(8/11+8/21均0缺失) / 概念99.9%(8/11:3只缺, 8/21:1只缺)
    #   事故日:   行业62.6% / 概念62.6% (8/21深沪主板整列--)
    #   → MIN_COV=99%: 个位数缺失(0.06%量级)正常通过, 整列/大板块缺失必拦
    n = len(all_seen)
    ind_cov = len(ind_by_code) / n * 100 if n else 0
    con_cov = len(con_by_code) / n * 100 if n else 0
    MIN_COV = 99.0
    if ind_cov < MIN_COV or con_cov < MIN_COV:
        return {
            'stocks': n, 'industries': len(ind_set), 'concepts': len(con_set),
            'error': f'❌ map拒绝写入: 行业覆盖率{ind_cov:.1f}% 概念覆盖率{con_cov:.1f}% '
                     f'(需>{MIN_COV:.0f}%) — 快照"所属同花顺行业/所属概念"列疑似导出时未加载(整列--)。'
                     f'旧map保持原样。请在同花顺重新导出(确认两列有数据)后重跑本脚本。',
        }
    # v5.1 (2026-09-18): 覆盖前先读旧文件 —— 只为出一行"这次变了什么"的提示, 见 _map_change_lines。
    # 放在健康检查**之后**: 拒绝写入的分支一个字节都不动, 自然也没有变化可报。
    old_ind = _read_map_file(os.path.join(MAP_DIR, 'industry_map.csv'))
    old_con = _read_map_file(os.path.join(MAP_DIR, 'concept_map.csv'))
    # 写 industry_map.csv (v5.0: 「行业」列 = 细分行业名)
    with open(os.path.join(MAP_DIR, 'industry_map.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f); w.writerow(['代码', '行业'])
        for code in sorted(ind_by_code): w.writerow([code, ind_by_code[code]])
    # 写 concept_map.csv
    with open(os.path.join(MAP_DIR, 'concept_map.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f); w.writerow(['代码', '概念'])
        for code in sorted(con_by_code): w.writerow([code, '|'.join(con_by_code[code])])
    # 写 industry_parent.csv (v5.0: 细分行业 → 上级二级行业名, 前端表头悬停用)
    # 同名细分若跨多个二级(理论不该有), 取成分股多的那个
    parent_rows = sorted((s, c.most_common(1)[0][0]) for s, c in sub2parent.items() if c)
    with open(os.path.join(MAP_DIR, 'industry_parent.csv'), 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f); w.writerow(['细分行业', '上级行业'])
        for s, par in parent_rows: w.writerow([s, par])
    return {'stocks': n, 'industries': len(ind_set), 'concepts': len(con_set),
            'parents': len(parent_rows),
            'diff': _map_change_lines('行业', old_ind, ind_by_code, False)
                  + _map_change_lines('概念', old_con, con_by_code, True)}


_HOLDER_CACHE = {'mtime': 0.0, 'data': None}


def load_holder_latest() -> dict:
    """holder_num.csv → {6位代码: 最新报告期股东户数}; mtime 未变则复用缓存

    实测全量读 + 取最新 0.055s (53137 行 / 5219 只), 相对逐股扫 K线 CSV 的 ~1s 基线可忽略;
    仍按 mtime 缓存 — 同进程内重复调用零成本。
    """
    # 注意: holder_num.csv 在 kline_data/ (KLINE_DIR 的父目录), 不是 daily/ 里;
    # 且其 code 带 SH/SZ 前缀 (fetch_holder_num.py 写的), 必须归一成 6 位才能与快照对齐
    path = os.path.join(os.path.dirname(KLINE_DIR), 'holder_num.csv')
    if not os.path.exists(path):
        return {}
    mt = os.path.getmtime(path)
    if _HOLDER_CACHE['data'] is not None and _HOLDER_CACHE['mtime'] == mt:
        return _HOLDER_CACHE['data']
    latest = {}          # 代码 -> (报告期, 户数)
    with open(path, encoding='utf-8-sig', newline='') as fh:
        for row in csv.DictReader(fh):
            code = (row.get('code') or '').strip()
            rd = (row.get('report_date') or '').strip()
            if not code or not rd: continue
            if code[:2] in ('SH', 'SZ'): code = code[2:]
            try:
                hn = float(row.get('holder_num') or 0)
            except ValueError:
                continue
            if hn <= 0: continue
            prev = latest.get(code)
            if prev is None or rd > prev[0]:     # 报告期字符串 YYYY-MM-DD 可直接比大小
                latest[code] = (rd, hn)
    data = {c: v[1] for c, v in latest.items()}
    _HOLDER_CACHE['mtime'] = mt
    _HOLDER_CACHE['data'] = data
    return data


def compare_holder(snap_holders: dict) -> dict:
    """快照股东户数 vs holder_num.csv → {'diff': [(代码, 快照值, 本地值)], 'exact','round','nocsv'}

    只认**真差异** (相对误差 ≥1%): 同花顺股东户数只给 3 位有效数字 (36892 → '3.69万'),
    绝对差判据会把大量正常舍入误报成差异 (2026-09-12 实测全市场: 绝对差 160 只 →
    相对差 69 只)。快照 '--'/空 已被 _cn_to_float 滤成 0, 不进比对。
    """
    csv_hn = load_holder_latest()
    exact = rounded = nocsv = 0
    diff = []
    for code, sv in snap_holders.items():
        cv = csv_hn.get(code)
        if not cv:
            nocsv += 1; continue
        if sv == cv:
            exact += 1
        elif abs(sv - cv) / cv < 0.01:
            rounded += 1
        else:
            diff.append((code, sv, cv))
    return {'diff': diff, 'exact': exact, 'round': rounded, 'nocsv': nocsv}


def auto_patch_exright(skip_diff: list) -> dict:
    """除权拦截票自动腾讯补拉 (2026-08-05)

    被拦截票: 前复权价 vs 快照实际价差异>10% = 当日除权 — 直接写入会断档,
    自动调腾讯前复权全量重写这些票的K线 (只写 K线CSV, 不碰 map/行业/概念)。
    返回 {ok, fail}
    """
    if not skip_diff:
        return {'ok': 0, 'fail': 0}
    from patch_exright import patch_one
    print(f'除权拦截 {len(skip_diff)} 只, 自动腾讯补拉 (仅K线, 不影响行业/概念)...')
    ok = fail = 0
    for code in skip_diff:
        # v2.36 (2026-09-18): 北交所的 code 已是 'BZ######'(自带前缀), 文件名就是它本身;
        # 沪深仍是裸 6 位, 需要自己拼前缀。
        if code[:2].upper() in ('BZ', 'BJ'):
            fpath = os.path.join(KLINE_DIR, f'{code}.csv')
        else:
            fpath = os.path.join(KLINE_DIR, f'SH{code}.csv')
            if not os.path.exists(fpath):
                fpath = os.path.join(KLINE_DIR, f'SZ{code}.csv')
        if os.path.exists(fpath) and patch_one(code, fpath):
            ok += 1
        else:
            fail += 1
            print(f'  ✗ {code} 补拉失败(保持原样, 下次再补)')
    print(f'补拉完成: 成功{ok} 失败{fail}')
    return {'ok': ok, 'fail': fail}


TENCENT_KLINE = ('https://ifzq.gtimg.cn/appstock/app/fqkline/get'
                 '?param={m}{c},day,,,2000,qfq')


def _tc_vol_on(code: str, date: str):
    """取腾讯前复权K线里 date 那根的成交量, 归一到「手」。

    返回 (状态, 量):  ok=取到 / absent=该日不在序列 / fail=取数失败。
    后两者一律**不豁免** —— 豁免只能建立在「取到了 且 逐值对上」这一个正面证据上。

    板别口径: 科创板 688/689 腾讯返股、本地存手, 需 ÷100
    (权威出处 = patch_exright.py 的 patch_one, 两处口径必须一致)。
    ⚠ 漂移也只会朝安全方向坏: 少除/多除都让比值恒偏 100 倍 → 永远对不上 → 永远照报, 不会误豁免。
    北交所未接: fqkline 对 bj 只返 1 根, 量口径未实测 → 一律 fail, 不猜。
    """
    if code[:2].upper() in ('BZ', 'BJ'):
        return ('fail', None)
    try:
        import json, urllib.request
        is_688 = code[2:].startswith('688') or code[2:].startswith('689')
        mkt = 'sh' if code[:2] == 'SH' else 'sz'
        req = urllib.request.Request(
            TENCENT_KLINE.format(m=mkt, c=code[2:]),
            headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://gu.qq.com/'})
        raw = urllib.request.urlopen(req, timeout=10).read().decode('utf-8')
        stock = json.loads(raw).get('data', {}).get(mkt + code[2:], {}) or {}
        for b in (stock.get('qfqday') or stock.get('day') or []):
            if str(b[0]) == date:
                v = float(b[5])
                return ('ok', v / 100.0 if is_688 else v)
        return ('absent', None)
    except Exception:
        return ('fail', None)


def verify_volume_units() -> int:
    """导入后成交量单位自检 (v2.10 2026-09-24)。返回未豁免的异常只数。

    在 check_segments() 的段级切换检测之上加一层**取证**:
    对上行异常(新数据偏大, 段级比 > 25)调腾讯前复权K线核对**切换日那一根**,
    逐值对上(±1%) → 判为真实放量, 不计入; 取不到 / 对不上 → 照报。

    - **只在上行方向开口**: 下行(历史偏大)不取证、一律照报 —— 那个方向「尾部」是对的,
      拿它取证必过, 会把「历史被写成股」的真事故洗白 (2026-08-12 那次 573 只正是这个方向)。
    - **成本闸**: 异常超过 5 只 = 系统性事故, 不逐只取证, 整批照报。
    - 输出区分三态: 豁免 / 不一致 / 全部一致, 豁免也留痕, 不静默。
    """
    from check_volume_unit import check_segments
    hits = []
    for fn in sorted(os.listdir(KLINE_DIR)):
        if not fn.endswith('.csv'):
            continue
        with open(os.path.join(KLINE_DIR, fn), encoding='utf-8') as fh:
            rows = list(csv.DictReader(fh))
        seg = check_segments(rows)
        if seg:
            hits.append((fn[:-4], seg, rows))

    exempt, bad = [], []
    for code, seg, rows in hits:
        for d, mr in seg:
            proven = None
            if len(hits) <= 5 and mr > 25:
                st, tv = _tc_vol_on(code, d)
                cv = next((r['volume'] for r in rows if r['date'] == d), None)
                if st == 'ok' and tv and cv and abs(float(cv) / tv - 1) <= 0.01:
                    proven = tv
            (exempt if proven is not None else bad).append((code, d, mr))

    for code, d, mr in [x[:3] for x in exempt]:
        print(f'  ⚠ {code} 放量 {mr:.1f} 倍 — 腾讯K线核对 {d} 逐值一致, 判为真实放量, 不计入')
    for code, d, mr in bad:
        print(f'  ⚠ {code} 成交量单位不一致! (切换日 {d}, 段级比 {mr:.2f}) 需修复, 参考 fix_688_volume.py')
    if bad:
        print(f'❌ 单位校验: {len(bad)} 只不一致 — 请先修复再做选股分析!')
    elif exempt:
        print(f'✅ 单位校验: 全部一致 ({len(exempt)} 只经腾讯核对判定为真实放量)')
    else:
        print('✅ 单位校验: 全部一致 (无股/手混用)')
    return len(bad)


if __name__ == '__main__':
    print('== 同花顺快照导入 ==')
    # v4.0 (2026-09-12): 表头预检 — 列名对不上就整体拒绝, 一个字节都不写。
    # (用户精简表头后, 旧版按位置取值会把 所属概念 当行业、开盘价 当概念 写进两张 map,
    #  而 99% 覆盖率检查照样通过 → 静默污染。宁可不跑, 不可写错。)
    probs = check_headers()
    if probs:
        print('❌ 快照表头校验失败, 已拒绝执行 (未写入任何文件):')
        for fn, why in probs:
            print(f'   {fn}: {why}')
        print(f'   必需列: {" ".join(REQUIRED_COLS)}  (行业列可为 所属行业 或 所属同花顺行业)')
        sys.exit(1)
    print(f'表头校验通过: {len(SNAPSHOT_FILES)} 个文件')
    snap, vstats = load_snapshot()
    print(f'快照: {len(snap)} 只 (量列缺失 {vstats["missing"]}/{vstats["total"]}, {vstats["missing_ratio"]*100:.1f}% | '
          f'现价缺失 {vstats["price_missing"]}/{vstats["total"]}, {vstats["price_ratio"]*100:.1f}%)')
    # 阈值定案 (2026-08-21, 量列): 正常基线 0.09% (5/5818 停牌/新股), 事故日 33.5%
    # 阈值定案 (2026-09-01, 现价列): 正常基线 0.22% (13/5825 真停牌), 事故日 23.5% (深主板整列)
    # → 统一 1% 既有 4~10 倍裕量, 又能拦住任何"列未加载"事故; 1%=约58只, 停牌高峰也到不了
    if vstats['missing_ratio'] > 0.01 or vstats['price_ratio'] > 0.01:
        print(f'❌ 量列缺失 {vstats["missing_ratio"]*100:.1f}% / 现价缺失 {vstats["price_ratio"]*100:.1f}% > 1% '
              f'— 拒绝写入K线! (volume=0 污染 同 8/21 事故; 现价列整列-- 同 9/1 事故) '
              f'请在同花顺重新导出后重跑。Map重建仍尝试。')
        stats = {'updated': 0, 'skip_already': 0, 'skip_diff': [], 'skip_no_data': 0, 'missing': []}
    else:
        stats = update_kline(snap)
        print(f"K线: 更新{stats['updated']} 已最新{stats['skip_already']} 停牌跳过{stats['skip_no_data']} "
              f"平移除息{len(stats['shift'])}({stats['shift'][:5]}...) "
              f"疑似除权{len(stats['skip_diff'])}({stats['skip_diff'][:8]}...) 快照缺失{len(stats['missing'])}")
        # v4.0 (2026-09-12): 股东户数哨兵 — **只提示, 不写任何文件** (快照没有报告期字段,
        # 写进 holder_num.csv 会把季度数据伪造成当日数据)。判据 = 相对误差 ≥1%,
        # 同花顺"万"的 3 位有效数字舍入(36892→'3.69万')不算差异。
        hc = compare_holder(vstats['holders'])
        print(f"股东户数差异数：{len(hc['diff'])}")
        for code, sv, cv in hc['diff'][:8]:
            print(f"   {code}: 快照 {sv:.0f} vs 本地 {cv:.0f} ({(sv / cv - 1) * 100:+.1f}%)")
    # v2.4: 除权拦截自动腾讯补拉 (不碰 map)
    pr = auto_patch_exright(stats['skip_diff'])
    m = rebuild_maps()
    if m.get('error'):
        print(m['error'])
        print(f"Map: 保持旧文件 (本次{len(snap)}只快照中仅{m['stocks']}只有行业/概念)")
    else:
        print(f"Map重建: {m['stocks']} 只 | 细分行业{m['industries']} | 概念{m['concepts']} | "
              f"细分→上级{m.get('parents', 0)}")
        # v5.1 (2026-09-18): 变化提示 —— 纯提示行, **不含 ❌** (server.py 从后往前找 ❌ 当作报错)
        for _ln in m.get('diff', []):
            print(_ln)

    # v2.10 (2026-09-24): 导入后自动校验成交量单位一致性 (防688股/手混用断崖复发)。
    # 判定逻辑已提成函数 (含腾讯K线取证豁免), 见文件上方; 这里只负责兜住异常。
    try:
        verify_volume_units()
    except Exception as e:
        print(f'单位校验跳过: {e}')
