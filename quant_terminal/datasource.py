# -*- coding: utf-8 -*-
"""
数据源配置管理 v1 (2026-08-29)
================================
统一配置: quant_terminal/data_sources.json (不存在 → 内置默认 = 现状硬编码)

设计要点:
- load(): 每次 os.stat 检查 mtime, 变更才重新解析 → 联网源/本地路径改动即时生效 (无需重启)
- save(): validate → 备份 .bak → 原子写 (.tmp + rename), 保存后清缓存
- 损坏/缺字段 → 自动回退默认 (broken 标记, 面板可见告警)
- fetch_realtime(): 实时行情统一入口 — 按 primary 分发 (sina/tencent),
  primary 失败自动降级 backup, LAST_REALTIME_SOURCE 记录实际使用源
- test(source): 真实最小请求连通性测试 (5s 超时)
- v2.37 (2026-09-19): _overlay_names() — 新浪 name 是公司全称, 用腾讯简称覆盖
- resolve(path): 相对路径 → 项目根绝对路径 (d:\\BS King)

铁律:
- 禁用源不提供选项: 东财 push2 / AKShare 个股+行业API
- map 文件 (industry_map/concept_map) 只读, 不在此配置 — 唯一入口=同花顺快照导入
"""
import os, json, time, glob, csv, io, sys, re
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CFG_FILE = os.path.join(BASE_DIR, 'quant_terminal', 'data_sources.json')

DEFAULT_CONFIG = {
    'version': 2,
    'online': {
        # 实时行情: primary 失败自动切 backup (LAST_REALTIME_SOURCE 可见实际源)
        'realtime': {
            'primary': 'sina',
            'backup': 'tencent',
            'sina_url': 'http://hq.sinajs.cn/list={codes}',
            'tencent_url': 'http://qt.gtimg.cn/q={codes}',
            'batch_size': 50,
        },
        # 在线历史K线 (腾讯fqkline; {market}sh/sz + {code} + {count})
        'kline': {
            'provider': 'tencent',
            'enabled': True,
            'url': 'https://ifzq.gtimg.cn/appstock/app/newfqkline/get?param={market}{code},day,,,{count},qfq',
        },
        # 分时图 (主备域名)
        'minute': {
            'url_primary': 'https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}',
            'url_backup': 'https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}',
        },
        # 行业板块实时 (腾讯 board)
        'sector_board': {
            'url': 'http://ifzq.gtimg.cn/appstock/app/board/index?board=industry',
        },
        # 涨停池 (东财直连, 与MCP同源; fallback=snapshot 快照识别当日涨停, 无封板质量)
        'limit_pool': {
            'provider': 'akshare_em',
            'fallback': 'snapshot',
            'cache_minutes': 30,
        },
        # 个股基本面 (东财datacenter财报; {code}=6位数字)
        'fundamental': {
            'provider': 'eastmoney_datacenter',
            'url': ('http://datacenter-web.eastmoney.com/api/data/v1/get'
                    '?reportName=RPT_LICO_FN_CPD&columns=ALL'
                    '&filter=(SECURITY_CODE%3D%22{code}%22)&pageSize=5'
                    '&sortColumns=REPORTDATE&sortTypes=-1'),
        },
        # v2.170 (2026-10-21) 同花顺 fuyao (免费 · 全市场快照一次拿全)
        #   实测: 一次请求 5580 只 / 1.07 秒；通达信同一件事要 2 次计费调用 / 12.7 秒。
        #   官方口径: 不限制累计调用次数，只有 HTTP 429 或信封 code=4001 才算触发限流。
        #   它替的是「11:35 / 15:35 全市场快照取价」这一环；15:35 的**市值**仍走通达信
        #   (fuyao 快照不返回市值，估值接口同样没有市值)。
        #   ⚠ 它的 volume 单位是「股」⇒ fuyao.py 内部已 /100 转成本地口径的「手」。
        'fuyao': {
            'enabled': True,
            'base': 'https://fuyao.aicubes.cn',
            'timeout': 20,
            'batch': 800,                # 自动刷新每批只数（3 源轮换时的单批上限）
            'key': '',                   # 掩码回显; 保存时 '******'/空 = 未修改
        },
        # 通达信 tdx-aidata（付费·补充字段）
        #   它不是实时行情的替代品 —— 实测价格四字段(开高低收)与本地 100% 相同，
        #   成交量还是 floor(股/100) 截断(40% 的票少 0~1 手)。它真正补的是**本地没有的
        #   实时扩展字段**(涨停价/跌停价/封单额/主力净流入/换手/量比/委比/流通市值/连板数)
        #   以及**除权因子**(get_divid_factors，精确到 10派X元)。
        #   key 存在 lib/TdxAiData.ini 里（该库只认包内 ini，不认环境变量）—— 见 tdx_fetch.py。
        # 职责已收窄：取价被 fuyao 取代，现在只剩**每天 15:35 拉一次全市场市值 + 自由流通股本**
        #   （get_more_info_batch 的 Zsz/Ltsz/FreeLtgb，这几项其它源都没有）。
        'tdx': {
            # 与 tdx_fetch.load_cfg() 的默认保持一致 (那边 t.get('enabled', True))。
            # 两边默认反了的话: 界面勾选框初始是关的, 用户一点保存就把能用的键关掉,
            # 而且没有任何提示 —— 静默失败。真正的闸是"有没有 key"(没 key 会直接报错)。
            'enabled': True,
            'lib_dir': 'tdx/lib',        # 相对项目根; 里面是 tdxaidata 包
            'timeout': 120,
            'key': '',                   # 掩码回显; 保存时 '******'/空 = 未修改
        },
    },
    'local': {
        # v2 (2026-08-29): 面板重做 — 8 项可设, 快照/模式池子支持多文件完整路径
        # 路径三种形式: 绝对路径 / 含目录的相对路径(相对项目根) / 纯文件名(相对所属目录, 兼容旧配置)
        'kline_dir': 'kline_data/daily',          # 1. 本地历史K线 (前复权)
        # v2.177 (2026-10-22): 2~7 项 (K线每日快照 / 趋势三档池 / 多头行业·概念文件) 已随
        #   导出快照体系下线 —— 取价与板块成分股都改由同花顺 fuyao 联网提供。
        'limit_history': 'dragon_data/limit_up_history.csv',  # 2. 涨停史文件 (龙头战法)
        # 内部使用 (面板不直接编辑):
        'snapshot_dir': '选股池',                   # 快照/池目录 (v2.177: 仅内部兜底, 面板已不显示)
        'industry_map': 'kline_data/industry_map.csv',   # 🔒 只读 (map铁律)
        'concept_map': 'kline_data/concept_map.csv',     # 🔒 只读
        'sub_to_ind': 'kline_data/sub_to_ind.csv',
        'tdx_export_dir': 'D:/tdx/T0002/export',   # 通达信导出 (兜底K线源)
    },
    # v2.112 (2026-09-25): 新闻 AI 影响分析。跟其它数据源**同一个文件** ⇒ 天然共用
    # 那个「数据源设置」窗口 (用户要求 key 填在那儿, 而不是手改 JSON)。
    # 实际调用逻辑全在 quant_terminal/ai_news.py; 这里只存配置。
    'ai': {
        'enabled': True,
        # ⚠ max_tokens 别调太小: **推理模型**会把预算先喂给 reasoning_content。
        #   实测: glm-4.7-flash 在 1200 预算下不关思考 → 思考 2172 字 / 正文 0 字 /
        #   37.6s。现主源是 4-flash, 但 thinking 开关已预置 (见下), 换 4.7 即生效。
        'timeout': 60,
        'max_tokens': 1200,
        # 主源（智谱免费档）。key 留空 → 自动回退 ~/.modlens/config.json 里同 host 的 key，
        # 所以**开箱即用**：不填也能跑，想换成自己的号再填。
        # 为什么还是 glm-4-flash 而不是 4.7-flash：4.7 是新的免费模型、全球都在挤 ——
        #   同一分钟同一 key 实测连吃 429（模型级拥塞），4-flash 却 1.1s 正常出正文。
        # 🔑 但 `thinking: 'disabled'` **必须留着**：4-flash 收到会静默忽略（实测无害），
        #   等于**预装好升级路径** —— 日后想试 4.7 只改模型名即可，不必再动别处。
        #   （不关思考的话 4.7 会把预算全烧在推理上，正文 0 字。）
        #   取值 ''=不发该字段 / 'disabled' / 'enabled'。
        #   ⚠ 该字段**只对智谱系有效**，故逐槽存，不放在 ai 顶层。
        'primary': {'name': '智谱', 'base': 'https://open.bigmodel.cn/api/paas/v4',
                    'model': 'glm-4-flash', 'key': '', 'thinking': 'disabled'},
        # 备源 (DeepSeek 官方付费)。主源 429/超时/网络失败 → 自动降级到这里。
        # ⚠ 非智谱系用不上 thinking, 留空 = **不发该字段** (别瞎塞给别家)。
        'backup': {'name': 'DeepSeek', 'base': 'https://api.deepseek.com',
                   'model': 'deepseek-chat', 'key': '', 'thinking': ''},
    },
}

# 🔑 key 掩码: GET /api/datasource 用它替换真值; 保存时见到它 = "未修改"。
#    ⚠️ 这两件事必须成对: 少了任何一半, 设置窗口一保存就会**用掩码冲掉真 key**。
MASK = '******'

# 实时行情主备白名单
REALTIME_PROVIDERS = ('sina', 'tencent')
# 禁用源 (不进入可选列表)
DISABLED_SOURCES = [
    'eastmoney_push2',        # 东财push2 行情接口 (被封)
    'akshare_stock_api',      # AKShare 个股行情API (被封)
    'akshare_industry_api',   # AKShare 行业API (被封)
]

_CACHE = {'mtime': 0.0, 'cfg': None, 'broken': False, 'exists': False}
LAST_REALTIME_SOURCE = {'source': 'sina', 'ts': 0.0, 'err': ''}   # 实际使用的实时源

# ---- v2.37 (2026-09-19): 简称一律以腾讯为准 (新浪给的是公司全称) ----
# 实测 5225 只: 新浪 hq.sinajs.cn 的 fields[0] 是**公司全称** —— 盛美上海半导体 /
# 中微半导体设备 / 寒武纪科技 / 苏州华兴源创科技, 与同花顺快照的简称 **383 只**对不上;
# 腾讯 qt.gtimg.cn 的 fs[1] 是交易所简称, 与同花顺快照一致 5222/5225
# (余下 3 只是同花顺自己截断到 4 字: 华润新能 / *ST三六 / 粤芯半导体)。
# → 只覆盖 name 一个字段, 价格/量/额一律不动 —— 新浪 volume=**股**、腾讯=**手**,
#   两个源绝不能混 (见 engine.py:3777 那条成交量单位陷阱)。
_NAME_CACHE = {'ts': 0.0, 'fail_ts': 0.0, 'map': {}}
_NAME_TTL = 6 * 3600.0        # 简称一天最多变一次 (除权当天加 XD), 6 小时足够
_NAME_FAIL_COOLDOWN = 600.0   # 腾讯失败后 10 分钟内不再重试, 免得每次调用都白等


# ============================================================
# 配置读写
# ============================================================
def load() -> dict:
    """读配置; mtime 变化才重解析; 缺失/损坏回退默认
    注意: 文件删除场景 getmtime 抛 OSError → mt=0.0 与缓存相等会误命中,
    需同时比对 exists 状态 (2026-08-29 reset后路径未还原的bug根因)"""
    exists = os.path.exists(CFG_FILE)
    try:
        mt = os.path.getmtime(CFG_FILE)
    except OSError:
        mt = 0.0
    if (_CACHE['cfg'] is not None and _CACHE['mtime'] == mt
            and _CACHE['exists'] == exists):
        return _CACHE['cfg']
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))  # 深拷贝默认
    broken = False
    if mt:
        try:
            with open(CFG_FILE, 'r', encoding='utf-8') as fh:
                user = json.load(fh)
            cfg = _merge(cfg, user)
        except Exception:
            broken = True
    _CACHE.update({'mtime': mt, 'cfg': cfg, 'broken': broken, 'exists': exists})
    return cfg


def _merge(base: dict, user: dict) -> dict:
    """深合并: user 缺的键用默认补齐 (版本兼容), 多出的键保留"""
    out = dict(base)
    for k, v in user.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            out[k] = _merge(base[k], v)
        else:
            out[k] = v
    return out


def is_broken() -> bool:
    load()
    return _CACHE['broken']


def masked() -> dict:
    """给浏览器的配置副本 —— key 一律换成掩码, 真值只留在服务端内存里。

    ⚠️ 必须是**深拷贝**: 直接改 load() 返回的对象会污染 _CACHE, 真 key 就没了。
    """
    cfg = json.loads(json.dumps(load()))
    ai = cfg.get('ai') or {}
    for slot in ('primary', 'backup'):
        p = ai.get(slot)
        if isinstance(p, dict) and str(p.get('key') or '').strip():
            p['key'] = MASK
    # v2.170 (2026-10-21): 同花顺 fuyao key 同一个铁律 —— 真值绝不发给浏览器
    fq = (cfg.get('online') or {}).get('fuyao')
    if isinstance(fq, dict):
        if str(fq.get('key') or '').strip():
            fq['key'] = MASK
        else:
            try:
                from quant_terminal import fuyao as _fq
                if str((_fq.load_cfg() or {}).get('key') or '').strip():
                    fq['key'] = MASK
            except Exception:
                pass
    # v2.153: 通达信 key 同一个铁律 —— 真值绝不发给浏览器
    #   ⚠ key 的**真值只活在 lib/TdxAiData.ini**(DLL 自己读的), json 里那份只是镜像。
    #   所以 json 空 ≠ 没配置 —— 必须回落到 ini 查一次, 否则界面会一直显示"未配置",
    #   用户就会重复填同一个 key。
    tdx = (cfg.get('online') or {}).get('tdx')
    if isinstance(tdx, dict):
        if str(tdx.get('key') or '').strip():
            tdx['key'] = MASK
        else:
            try:
                from quant_terminal import tdx_fetch as _tf
                if _tf.read_token(tdx.get('lib_dir') or ''):
                    tdx['key'] = MASK
            except Exception:
                pass
    return cfg


def _old_cfg() -> dict:
    """盘上当前的配置（不存在/损坏 → 空 dict）。_preserve_keys 用它取旧 key。"""
    try:
        if os.path.exists(CFG_FILE):
            with open(CFG_FILE, 'r', encoding='utf-8') as fh:
                d = json.load(fh)
            return d if isinstance(d, dict) else {}
    except Exception:
        pass
    return {}


def _preserve_keys(cfg: dict) -> dict:
    """🔑 key 铁律②: 回传空串或掩码 = "未修改" → 保留盘上的旧值。

    设置窗口保存时是**把整份配置回传**的, 而它看到的 key 是掩码 `******`。
    不做这一步的话, 那次保存会把掩码当新 key 写盘, **直接把真 key 冲掉**。
    """
    old = {}
    try:
        if os.path.exists(CFG_FILE):
            with open(CFG_FILE, 'r', encoding='utf-8') as fh:
                old = ((json.load(fh) or {}).get('ai') or {})
    except Exception:
        old = {}
    ai = cfg.get('ai')
    if isinstance(ai, dict):
        for slot in ('primary', 'backup'):
            cur = ai.get(slot)
            if not isinstance(cur, dict):
                continue
            v = str(cur.get('key') or '').strip()
            if v and v != MASK:
                continue                                   # 用户真填了新 key → 用新的
            cur['key'] = str(((old.get(slot) or {}).get('key')) or '')   # 其余一律保留旧值
    # v2.170 (2026-10-21): 同花顺 key 同规则（空/掩码 = 未修改）
    fq = ((cfg.get('online') or {}).get('fuyao'))
    if isinstance(fq, dict):
        v = str(fq.get('key') or '').strip()
        if not v or v == MASK:
            old_fq = ((_old_cfg().get('online') or {}).get('fuyao') or {})
            fq['key'] = str(old_fq.get('key') or '')
    # v2.153: 通达信 key 同规则（空/掩码 = 未修改）
    tdx = ((cfg.get('online') or {}).get('tdx'))
    if isinstance(tdx, dict):
        v = str(tdx.get('key') or '').strip()
        if not v or v == MASK:
            old_tdx = ((_old_cfg().get('online') or {}).get('tdx') or {})
            tdx['key'] = str(old_tdx.get('key') or '')
    return cfg


def swap_ai(cfg: dict) -> tuple:
    """主/备**整槽对调** (含各自的 key), 然后立即落盘。返回 (ok, errors[])。

    🔑 为什么必须在服务端做: 浏览器手上那份配置的 key 是掩码 `******`。纯前端对调 DOM 值
       的话, 回传时两边都是"未修改" ⇒ _preserve_keys 会把**原槽位的旧 key 又填回原槽位**
       —— 看着换了, 实际没换。只有服务端拿着真值才能把 key 跟着槽位一起搬。

    key 取值: 表单该槽**真填了新值** → 用新值; 否则 → 搬盘上**该槽**的旧值。
    ⚠️ 解析完必须绕过 _preserve_keys 直写 (save(..., preserve_keys=False)):
       否则"新 backup 槽 key 为空"会被 _preserve_keys 拿**盘上 backup 槽**(= 刚换过来那家)
       的旧 key 填上 → 两家 key 变成同一个, 静默错。
    """
    ok, errs = validate(cfg)
    if not ok:
        return False, errs
    ai = cfg.get('ai')
    if not isinstance(ai, dict):
        return False, ['配置里没有 ai 段']
    p, b = ai.get('primary'), ai.get('backup')
    if not isinstance(p, dict) or not isinstance(b, dict):
        return False, ['ai.primary / ai.backup 结构不对']

    old = {}
    try:
        if os.path.exists(CFG_FILE):
            with open(CFG_FILE, 'r', encoding='utf-8') as fh:
                old = ((json.load(fh) or {}).get('ai') or {})
    except Exception:
        old = {}

    def _eff(form_slot, name):
        v = str(((form_slot or {}).get('key')) or '').strip()
        if v and v != MASK:
            return v                                       # 表单真填了新 key
        return str(((old.get(name) or {}).get('key')) or '')

    kp, kb = _eff(p, 'primary'), _eff(b, 'backup')
    np_, nb = json.loads(json.dumps(b)), json.loads(json.dumps(p))   # 整槽对调
    np_['key'], nb['key'] = kb, kp                                   # key 跟着槽一起走
    ai['primary'], ai['backup'] = np_, nb
    return save(cfg, preserve_keys=False)


def save(cfg: dict, preserve_keys: bool = True) -> tuple:
    """校验 → 备份 .bak → 原子写。返回 (ok, errors[])

    preserve_keys=False: **跳过 _preserve_keys**, 直接把 cfg 里的 key 当真值写盘。
      ⚠️ 只给 swap_ai() 用 —— 它已经把两个槽的 key 都解析成真值了。
      ⚠️ 别在普通保存路径上关掉它, 关了就回到"掩码冲真 key"那个坑。
    """
    ok, errors = validate(cfg)
    if not ok:
        return False, errors
    if preserve_keys:
        cfg = _preserve_keys(cfg)
    if os.path.exists(CFG_FILE):
        try:
            with open(CFG_FILE, 'rb') as src, open(CFG_FILE + '.bak', 'wb') as dst:
                dst.write(src.read())
        except Exception as e:
            return False, [f'备份失败: {e}']
    tmp = CFG_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(cfg, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, CFG_FILE)
    _CACHE['mtime'] = 0.0  # 强制下次 load 重读
    _sync_tdx_token(cfg)
    return True, []


# v2.153: 上一次保存时"通达信 key 同步进 ini"的结果 —— 给界面回显用。
#   {ok: bool|None, msg: str}; None = 本次没动 ini（没填新 key）。
LAST_TDX_SYNC = {'ok': None, 'msg': ''}


def _sync_tdx_token(cfg: dict) -> None:
    """把 online.tdx.key 写进库内 ini（TdxAiData.ini + user.ini）。

    ⚠ 三个不能碰的边界:
      1. key 为空 或 掩码 MASK ⇒ 用户没动这一栏, **绝不能碰 ini**
         （不然每次点保存都重写文件, 还把 MASK 当 key 写进去）
      2. 写 ini 失败**不算配置保存失败** —— 配置本体已经落盘了, 只把原因带回界面
      3. 绝不回显 key 真值, msg 里只有文件名
    """
    try:
        tdx = ((cfg.get('online') or {}).get('tdx') or {})
        key = str(tdx.get('key') or '').strip()
        if not key or key == MASK:
            LAST_TDX_SYNC['ok'], LAST_TDX_SYNC['msg'] = None, ''
            return
        from quant_terminal import tdx_fetch as _tf
        ok, msg = _tf.set_token(key, tdx.get('lib_dir') or '')
        LAST_TDX_SYNC['ok'] = bool(ok)
        LAST_TDX_SYNC['msg'] = msg if ok else ('通达信 key 写 ini 失败: ' + msg)
    except Exception as e:
        LAST_TDX_SYNC['ok'] = False
        LAST_TDX_SYNC['msg'] = '通达信 key 写 ini 异常: ' + str(e)[:80]


def restore_backup() -> tuple:
    """从 .bak 恢复 (面板"恢复上次备份")"""
    if not os.path.exists(CFG_FILE + '.bak'):
        return False, ['无备份文件']
    with open(CFG_FILE + '.bak', 'rb') as src, open(CFG_FILE, 'wb') as dst:
        dst.write(src.read())
    _CACHE['mtime'] = 0.0
    return True, []


def validate(cfg: dict) -> tuple:
    """URL 占位符完整性 + provider 白名单 + 本地路径非空"""
    errs = []
    o = cfg.get('online', {})
    rt = o.get('realtime', {})
    if rt.get('primary') not in REALTIME_PROVIDERS:
        errs.append(f"实时主源必须为 {'/'.join(REALTIME_PROVIDERS)}")
    if rt.get('backup') not in REALTIME_PROVIDERS:
        errs.append(f"实时备源必须为 {'/'.join(REALTIME_PROVIDERS)}")
    if rt.get('primary') == rt.get('backup'):
        errs.append('主源与备源不能相同')
    for key, ph in [('sina_url', '{codes}'), ('tencent_url', '{codes}')]:
        if ph not in str(rt.get(key, '')):
            errs.append(f'realtime.{key} 缺少占位符 {ph}')
    if o.get('kline', {}).get('enabled'):
        ku = str(o.get('kline', {}).get('url', ''))
        for ph in ('{market}', '{code}', '{count}'):
            if ph not in ku:
                errs.append(f'kline.url 缺少占位符 {ph}')
    for key in ('minute.url_primary', 'minute.url_backup'):
        if '{code}' not in str(o.get('minute', {}).get(key.split('.')[1], '')):
            errs.append(f'minute.{key.split(".")[1]} 缺少占位符 {{code}}')
    if '{code}' not in str(o.get('fundamental', {}).get('url', '')):
        errs.append('fundamental.url 缺少占位符 {code}')
    if o.get('limit_pool', {}).get('provider') not in ('akshare_em',):
        errs.append("涨停池 provider 仅支持 akshare_em (东财push2已禁用)")
    # v2.170 (2026-10-21): 同花顺 fuyao —— key 格式 + base 必须是 http(s) + batch 必须正整数
    fq = o.get('fuyao')
    if isinstance(fq, dict):
        kv = str(fq.get('key') or '').strip()
        if kv and kv != MASK and not re.match(r'^[A-Za-z0-9_.\-]{8,128}$', kv):
            errs.append('同花顺 Key 格式不对 (只允许字母数字和 _ - . , 8~128 位)')
        bu = str(fq.get('base') or '').strip()
        if bu and not re.match(r'^https?://', bu):
            errs.append('同花顺 base 必须以 http:// 或 https:// 开头')
        try:
            if int(fq.get('batch') or 0) < 1:
                errs.append('同花顺 batch 必须 ≥ 1')
        except (TypeError, ValueError):
            errs.append('同花顺 batch 必须是数字')
    # v2.153: 通达信段 —— 只在启用的前提下要求 lib_dir 存在
    tdx = o.get('tdx')
    if isinstance(tdx, dict):
        kv = str(tdx.get('key') or '').strip()
        if kv and kv != MASK and not re.match(r'^[A-Za-z0-9_.\-]{8,128}$', kv):
            errs.append('通达信 Key 格式不对 (只允许字母数字和 _ - . , 8~128 位)')
        if not str(tdx.get('lib_dir') or '').strip():
            errs.append('通达信 lib_dir 不能为空')
        elif bool(tdx.get('enabled')):
            lib = str(tdx.get('lib_dir'))
            lib = lib if os.path.isabs(lib) else resolve(lib)
            if not os.path.isdir(lib):
                errs.append('通达信库目录不存在: ' + lib)
    # v2: 多文件列表字段 — 必须是非空字符串列表, 元素非空
    for k in ():
        v = cfg.get('local', {}).get(k)
        if not isinstance(v, list):
            errs.append(f'本地 {k} 必须是文件路径列表')
            continue
        if not v:
            errs.append(f'本地 {k} 至少需要一个文件')
            continue
        for f in v:
            if not str(f).strip():
                errs.append(f'本地 {k} 存在空路径')
    for k, v in cfg.get('local', {}).items():
        if isinstance(v, list):
            continue
        if not str(v).strip():
            errs.append(f'本地路径 {k} 不能为空')
    return (len(errs) == 0), errs


# ============================================================
# 取值工具
# ============================================================
def get(section: str, key: str, default=None):
    return load().get(section, {}).get(key, default)


def resolve(path: str) -> str:
    """相对路径 → 项目根绝对路径; 绝对路径原样返回"""
    if os.path.isabs(path):
        return path
    return os.path.join(BASE_DIR, path)


def file_path(path: str, base: str) -> str:
    """统一路径解析 (v2): 绝对路径原样; 含目录的相对路径 → 相对项目根;
    纯文件名 → join(base)。base 用于快照文件/选股池等按目录组织的文件"""
    path = str(path or '')
    if os.path.isabs(path):
        return path
    if os.path.dirname(path):
        return resolve(path)
    return os.path.join(resolve(str(base)), path)


# ============================================================
# 实时行情统一入口 (主源失败自动降级备源)
# ============================================================
def _req_get(url: str, timeout: int = 8, headers: dict = None):
    """HTTP GET —— **先直连, 失败再走系统代理** (v2.234 2026-10-27, 同 engine._req_get)

    境内源(新浪/腾讯/东财)走本机 SSR 代理实测慢 4~11 倍且更易断, 故顺序反转。
    与 zt_pool_em 的思路一致 —— 那里早就写了「境内源先直连, 系统代理是添乱项」。
    """
    import requests
    try:
        return requests.get(url, timeout=timeout, headers=headers,
                            proxies={'http': None, 'https': None})
    except Exception:
        return requests.get(url, timeout=timeout, headers=headers)


# ---------------------------------------------------------------- 涨停池 (东财直连)
# v2.129 (2026-09-28): 原先走 akshare.stock_zt_pool_em —— 它内部是**裸 requests.get**
#   (无 timeout、无代理兜底), 而 requests 在 Windows 上会读注册表走系统代理。
#   本机 SSR(127.0.0.1:1080) 对 push2ex 接得上但不回话 ⇒ 硬耗 121 秒才抛 SSLError
#   ⇒ 涨停池降级成"快照识别"(封板额/连板/换手/首封时间全丢, 只数还多算)。
#   改为自建请求: **先绕代理直连**(境内源, 代理只会添乱), 失败再走系统默认, 两步都带超时。
ZT_POOL_URL = 'https://push2ex.eastmoney.com/getTopicZTPool'
ZT_POOL_UT = '7eea3edcaed734bea9cbfc24409ed989'
ZT_POOL_HEADERS = {'Referer': 'https://quote.eastmoney.com/'}


def _hhmmss(v) -> str:
    """fbt/lbt 原始值 92500 → '092500' (对齐 akshare 的 zfill(6), 亦对齐历史 CSV)"""
    try:
        return str(int(v)).zfill(6) if v else ''
    except Exception:
        return ''


def zt_pool_em(date: str, timeout: int = 8) -> list:
    """东财涨停池 (ak.stock_zt_pool_em 的替代品, 带超时 + 代理兜底)。

    date='YYYYMMDD'。返回 list[dict], **键名刻意沿用 akshare 的列名** ⇒ 调用点零改动。
    失败抛异常, 由调用方决定是否降级到快照识别。
    """
    import requests
    params = {'ut': ZT_POOL_UT, 'dpt': 'wz.ztzt', 'Pageindex': '0',
              'pagesize': '10000', 'sort': 'fbt:asc', 'date': date}
    r, err = None, None
    # 顺序与 _req_get 相反: 境内源先直连, 系统代理在这里是**添乱项**而非救命稻草
    for proxies in ({'http': None, 'https': None}, None):
        try:
            r = requests.get(ZT_POOL_URL, params=params, timeout=timeout,
                             headers=ZT_POOL_HEADERS, proxies=proxies)
            if r.status_code == 200:
                break
            err, r = Exception('HTTP %d' % r.status_code), None
        except Exception as e:
            err, r = e, None
    if r is None:
        raise err if err else Exception('涨停池: 无响应')
    pool = ((r.json() or {}).get('data') or {}).get('pool') or []
    out = []
    for x in pool:
        code = str(x.get('c') or '')
        if len(code) != 6 or not code.isdigit():
            continue
        out.append({
            '代码': code,
            '名称': str(x.get('n') or code),
            '涨跌幅': x.get('zdp'),
            '最新价': (x.get('p') or 0) / 1000.0,   # 原始价单位需 /1000 (6360 → 6.36)
            '成交额': x.get('amount'),
            '流通市值': x.get('ltsz'),
            '总市值': x.get('tshare'),
            '换手率': x.get('hs'),
            '封板资金': x.get('fund'),
            '首次封板时间': _hhmmss(x.get('fbt')),
            '最后封板时间': _hhmmss(x.get('lbt')),
            '炸板次数': x.get('zbc'),
            '连板数': x.get('lbc'),
            '所属行业': str(x.get('hybk') or ''),
        })
    if pool and not out:
        # 有原始记录却一条都没通过代码校验 = 接口字段改名了, 别静默返回空
        raise Exception('涨停池: %d 条原始记录无一含有效代码 (接口字段可能已变更)' % len(pool))
    return out


# ---------------------------------------------------------------- 最近交易日
# v2.118 (2026-09-25): 「今天到底开没开市」的**唯一**权威来源。
# 🔴 不维护节假日表、不查农历、不问 AKShare (已被封) —— 交易所**自己在行情里报**了
#    成交日期: 新浪 fields[30]='2026-09-24' / 腾讯 fs[30]='20260924161401'
#    (2026-09-25 两个源都实测过, 都在下标 30)。开市日它就是今天, 周末/节假日它就是
#    上一个交易日 ⇒ **自维护, 不会过年过期, 临时休市也认得出**。
# ⚠ 只在 fetch_realtime 成功时顺手记一次 (见 _note_qdate) —— **不额外发任何请求**。
LAST_SESSION = {'date': '', 'ts': 0.0}   # ts = 最近一次取到日期的时刻


def _norm_qdate(s):
    """源里的成交日期 → 'YYYY-MM-DD'。新浪 '2026-09-24' / 腾讯 '20260924161401' 都吃"""
    d = ''.join(ch for ch in str(s or '') if ch.isdigit())
    if len(d) < 8:
        return ''
    return '%s-%s-%s' % (d[:4], d[4:6], d[6:8])


def _note_qdate(rows):
    """把源报的成交日期记进 LAST_SESSION。每次实时取数顺手调一次, 零额外请求。"""
    got = [r.get('qdate') for r in (rows or {}).values()
           if isinstance(r, dict) and r.get('qdate')]
    if got:
        LAST_SESSION['date'] = max(got)     # 取最大: 万一某只票的报价卡在上一日
        LAST_SESSION['ts'] = time.time()


def last_session(max_age=900):
    """源报的最近交易日 'YYYY-MM-DD'。**max_age=None = 永不过期** (只给界面展示用)。

    取不到 / 数据太旧 → '' —— 调用方据此**放弃判定**, 退回"只看星期几"的旧行为。
    🔴 宁可不判, 不可拿一份可能过夜的数据下"今天休市"的结论。
    """
    if not LAST_SESSION['date']:
        return ''
    if max_age is not None and time.time() - LAST_SESSION['ts'] > max_age:
        return ''
    return LAST_SESSION['date']


def is_holiday():
    """今天是不是**休市日** —— 工作日但没开市 (法定节假日 / 临时休市)。周末不算。

    🔴 三道闸, 少一道就是"每个交易日早上都误报休市":
      ① 周一至周五        —— 周末有自己的态 (WEEKEND), 别混进来
      ② 现在 >= 09:35     —— 09:15 集合竞价**撮合前**, 源里报的还是**上一个交易日**
                             ⇒ 那会儿判"日期 ≠ 今天"会把**每一个正常交易日**都误杀。
                             09:25 撮合后源必翻到当天, 再留 10 分钟余量。
      ③ 数据是今天 09:25 之后取的 —— 网络故障 / 没取过数 / 手里是昨夜的值
                             ⇒ 一律 False, **宁可退回旧行为, 不可误报休市**。
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    # v2.187 (2026-10-23): 同花顺官方交易日历优先 —— 一年窗口内能直接判「今天不在交易日」,
    #   比下面「拿快照 trade_date 反推」可靠。日历取不到(离线/超窗)返回 None, 才落到旧三道闸。
    try:
        try:
            from . import fuyao_limit as _fl
        except Exception:
            import fuyao_limit as _fl
        _ft = _fl.is_trading_day(now.strftime('%Y-%m-%d'))
        if _ft is not None:
            return (not _ft)
    except Exception:
        pass
    if now.hour * 100 + now.minute < 935:
        return False
    ts, ls = LAST_SESSION['ts'], LAST_SESSION['date']
    if not (ls and ts):
        return False
    got = datetime.fromtimestamp(ts)
    if got.date() != now.date() or got.hour * 100 + got.minute < 925:
        return False
    return ls != now.strftime('%Y-%m-%d')


def _fetch_sina(codes: list) -> dict:
    """新浪实时 (hq.sinajs.cn 批量, 必带 Referer) → {sc: {...}}"""
    url = get('online', 'realtime', {}).get('sina_url', '').format(codes=','.join(codes))
    resp = _req_get(url, timeout=8, headers={'Referer': 'https://finance.sina.com.cn'})
    resp.encoding = 'gbk'
    results = {}
    for line in resp.text.strip().split('\n'):
        if '=' not in line or '"' not in line:
            continue
        raw = line.split('=')[0]              # 'var hq_str_sh000001' (指数/股票同名, 无 s_ 前缀!)
        parts = raw.split('_')
        sc = parts[-1]
        fields_str = line.split('"')[1] if '"' in line else ''
        fields = fields_str.split(',')
        if len(fields) < 6:
            continue
        name = fields[0]
        # v2.1 bugfix (2026-08-29): 指数 vs 股票 — 实测新浪指数变量名无 s_ 前缀 (旧 startswith('s_')
        # 判定恒假), 但沪指=34字段/深指=33字段(五档恒0), 股票恒32字段(五档有数, 停牌也32字段)。
        # len>=33 即排除全部股票; 五档 float 全0 双保险。指数格式 fields[9]=成交额(元) 照旧版
        # IndexFetcher 口径 /1e8=亿 → 修复顶部成交额 0.00 万亿
        is_index = len(fields) >= 33 and all(
            not f.strip() or abs(float(f)) < 1e-9 for f in fields[10:30])
        if is_index:
            price = float(fields[3]) if fields[3] else 0
            prev = float(fields[2]) if fields[2] else 0
            chg = (price - prev) / prev * 100 if prev > 0 else 0
            results[sc] = {
                'name': name, 'price': price, 'prev_close': prev,
                'high': float(fields[4]) if len(fields) > 4 and fields[4] else 0,
                'low': float(fields[5]) if len(fields) > 5 and fields[5] else 0,
                'open': float(fields[1]) if fields[1] else 0,  # v2.57: 指数分支补OHLC (盘中指数live bar兜底用)
                'change_pct': round(chg, 2),
                'volume': int(float(fields[8])) if len(fields) > 8 and fields[8] else 0,
                'amount_yi': round(float(fields[9]) / 1e8, 0) if len(fields) > 9 and fields[9] else 0,
                'qdate': _norm_qdate(fields[30]) if len(fields) > 30 else '',   # v2.118 成交日期
            }
        elif len(fields) >= 10:
            price = float(fields[3]) if fields[3] else 0
            prev = float(fields[2]) if fields[2] else 0
            chg = (price - prev) / prev * 100 if prev > 0 else 0
            results[sc] = {
                'name': name, 'price': price, 'prev_close': prev,
                'high': float(fields[4]) if fields[4] else 0,
                'low': float(fields[5]) if fields[5] else 0,
                'open': float(fields[1]) if fields[1] else 0,
                'volume': int(fields[8]) if len(fields) > 8 and fields[8] else 0,
                'change_pct': round(chg, 2),
                'qdate': _norm_qdate(fields[30]) if len(fields) > 30 else '',   # v2.118 成交日期
            }
    return results


def _fetch_tencent(codes: list) -> dict:
    """腾讯实时 (qt.gtimg.cn, 字段~分隔) → {sc: {...}} 与新浪同结构"""
    url = get('online', 'realtime', {}).get('tencent_url', '').format(codes=','.join(codes))
    resp = _req_get(url, timeout=8)
    resp.encoding = 'gbk'
    results = {}
    for line in resp.text.strip().split('\n'):
        if '=' not in line or '"' not in line:
            continue
        sc = line.split('=')[0].split('_')[-1]  # 'sh600519'
        fs = line.split('"')[1].split('~')
        # 腾讯字段: [1]名称 [3]现价 [4]昨收 [5]今开 [6]成交量(手) [33]最高 [34]最低
        if len(fs) < 35:
            continue
        try:
            price = float(fs[3]); prev = float(fs[4])
        except ValueError:
            continue
        chg = (price - prev) / prev * 100 if prev > 0 else 0
        results[sc] = {
            'name': fs[1], 'price': price, 'prev_close': prev,
            'high': float(fs[33]) if fs[33] else 0,
            'low': float(fs[34]) if fs[34] else 0,
            'open': float(fs[5]) if fs[5] else 0,
            'volume': int(float(fs[6])) if fs[6] else 0,   # 腾讯成交量=手
            'amount_yi': round(float(fs[37]) / 10000, 2) if len(fs) > 37 and fs[37] else 0,  # 成交额万→亿
            'change_pct': round(chg, 2),
            # v2.118: fs[30]='20260924161401' (14位=日期+时分秒), _norm_qdate 取前 8 位
            'qdate': _norm_qdate(fs[30]) if len(fs) > 30 else '',
        }
    return results


def _tencent_names(codes: list) -> dict:
    """腾讯简称表 {sc: name} —— 只取名字, 不碰价格/量/额。整表 6h 过期, 失败冷却 10 分钟。
    返回缓存全表 (可能不含本次 codes 里腾讯没返回的那几只)。"""
    now = time.time()
    if now - _NAME_CACHE['ts'] > _NAME_TTL:
        _NAME_CACHE['map'] = {}
    if now - _NAME_CACHE['fail_ts'] < _NAME_FAIL_COOLDOWN:
        return _NAME_CACHE['map']
    todo = [c for c in dict.fromkeys(codes) if c not in _NAME_CACHE['map']]
    if not todo:
        return _NAME_CACHE['map']
    try:
        url = get('online', 'realtime', {}).get('tencent_url', '').format(codes=','.join(todo))
        resp = _req_get(url, timeout=4)
        resp.encoding = 'gbk'
        got = 0
        for line in resp.text.strip().split('\n'):
            if '=' not in line or '"' not in line:
                continue
            sc = line.split('=')[0].split('_')[-1]
            fs = line.split('"')[1].split('~')
            if len(fs) > 1 and fs[1]:
                _NAME_CACHE['map'][sc] = fs[1]
                got += 1
        if got:
            _NAME_CACHE['ts'] = now
        else:
            _NAME_CACHE['fail_ts'] = now
    except Exception:
        _NAME_CACHE['fail_ts'] = now
    return _NAME_CACHE['map']


def _overlay_names(out: dict) -> None:
    """把 out 里每个 {sc: {...}} 的 name 换成腾讯简称 (腾讯没返回的保留原值; 腾讯整体失败则不动)"""
    try:
        nm = _tencent_names(list(out.keys()))
    except Exception:
        return
    for sc, dd in out.items():
        n = nm.get(sc) if nm else None
        if n and isinstance(dd, dict):
            dd['name'] = n


def fetch_realtime(codes: list, batch_size: int = 50) -> dict:
    """实时行情统一入口: primary → 失败降级 backup。
    返回 {sc: {...}}, LAST_REALTIME_SOURCE 记录实际源 (降级可见)"""
    if not codes:
        return {}
    cfg = get('online', 'realtime', {})
    primary = cfg.get('primary', 'sina')
    backup = cfg.get('backup', 'tencent')
    last_err = ''
    for provider in (primary, backup):
        try:
            if provider == 'sina':
                out = _fetch_sina(list(codes[:batch_size]))
            else:
                out = _fetch_tencent(list(codes[:batch_size]))
            if out:
                if provider == 'sina':
                    _overlay_names(out)   # v2.37: 新浪 name=公司全称 → 换成腾讯简称
                LAST_REALTIME_SOURCE.update({'source': provider, 'ts': time.time(), 'err': ''})
                _note_qdate(out)      # v2.118: 顺手记下源报的成交日期 (零额外请求)
                return out
            last_err = f'{provider}: 空数据'
        except Exception as e:
            last_err = f'{provider}: {str(e)[:60]}'
        time.sleep(0.3)
    LAST_REALTIME_SOURCE.update({'source': '', 'ts': time.time(), 'err': last_err})
    return {'_err': last_err}


# ============================================================
# 连通性测试 (真实最小请求, 5s 超时)
# ============================================================
def test(source: str) -> dict:
    """source: realtime | kline | minute | sector_board | limit_pool | fundamental
    | fuyao | tdx | ai.primary | ai.backup | local.* (本地路径存在性)。返回 {ok, latency_ms, detail}"""
    t0 = time.time()
    try:
        if source == 'realtime':
            out = fetch_realtime(['sh600519'])
            if '_err' in out:
                return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000), 'detail': out['_err']}
            v = out.get('sh600519', {})
            return {'ok': True, 'latency_ms': int((time.time() - t0) * 1000),
                    'detail': f"实际源={LAST_REALTIME_SOURCE['source']} 茅台 {v.get('price')}"}
        if source == 'kline':
            url = get('online', 'kline', {}).get('url', '').format(market='sh', code='600519', count='5')
            d = _req_get(url, timeout=5).json()
            bars = d.get('data', {}).get('sh600519', {}).get('day') or \
                   d.get('data', {}).get('sh600519', {}).get('qfqday', [])
            if not bars:
                return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000), 'detail': '无K线数据'}
            return {'ok': True, 'latency_ms': int((time.time() - t0) * 1000),
                    'detail': f"最新 {bars[-1][0]} 收{bars[-1][2]}"}
        if source == 'minute':
            url = get('online', 'minute', {}).get('url_primary', '').format(code='sh600519')
            d = _req_get(url, timeout=5, headers={'Referer': 'https://gu.qq.com/'}).json()
            dd = d.get('data', {}).get('sh600519', {})
            arr = (dd.get('data') or {}).get('data', []) if isinstance(dd.get('data'), dict) else []
            return {'ok': bool(arr), 'latency_ms': int((time.time() - t0) * 1000),
                    'detail': f'{len(arr)} 个分时点' if arr else '无分时数据'}
        if source == 'sector_board':
            url = get('online', 'sector_board', {}).get('url', '')
            d = _req_get(url, timeout=5).json()
            plates = d.get('data', {}).get('rank', {}).get('plate', [])
            return {'ok': bool(plates), 'latency_ms': int((time.time() - t0) * 1000),
                    'detail': f'{len(plates)} 个行业板块' if plates else '无板块数据'}
        if source == 'limit_pool':
            try:
                rows = zt_pool_em(time.strftime('%Y%m%d'))
                return {'ok': True, 'latency_ms': int((time.time() - t0) * 1000),
                        'detail': f'东财涨停池 {len(rows)} 只 (直连)'}
            except Exception as e:
                fb = _snapshot_zt_available()
                return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000),
                        'detail': f'涨停池失败: {str(e)[:50]}; 快照降级: {"可用" if fb else "不可用(缺快照)"}'}
        if source == 'fundamental':
            url = get('online', 'fundamental', {}).get('url', '').format(code='600519')
            d = _req_get(url, timeout=5).json()
            rows = (d.get('result') or {}).get('data') or []
            if not rows:
                return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000), 'detail': '无财务数据'}
            return {'ok': True, 'latency_ms': int((time.time() - t0) * 1000),
                    'detail': f"财报期 {str(rows[0].get('REPORTDATE') or '')[:10]}"}
        if source == 'fuyao':
            # v2.170 (2026-10-21): 同花顺 —— 免费，本进程直接拉 2 只，绝不回显 key
            from quant_terminal import fuyao as _fq
            r = _fq.test()
            ms = int(r.get('latency_ms') or 0) or int((time.time() - t0) * 1000)
            return {'ok': bool(r.get('ok')), 'latency_ms': ms,
                    'detail': str(r.get('detail') or '')[:120]}
        if source == 'tdx':
            # v2.153: 走子进程 selftest —— 真连一次（约 1 积分），拿 HqDate 证明 key 可用。
            #   ⚠ 绝不回显 key，只回「可用/失败 + 数据日期」。
            from quant_terminal import tdx_fetch as _tf
            import subprocess as _sp
            r = _sp.run([sys.executable, '-m', 'quant_terminal.tdx_fetch', '--selftest'],
                        cwd=BASE_DIR, capture_output=True, text=True, timeout=90,
                        creationflags=getattr(_sp, 'CREATE_NO_WINDOW', 0))
            out = ((r.stdout or '') + (r.stderr or '')).strip()
            ms = int((time.time() - t0) * 1000)
            if r.returncode != 0:
                return {'ok': False, 'latency_ms': ms, 'detail': out[-160:] or 'selftest 失败'}
            m = re.search(r"'HqDate':\s*'?(\d{8})", out)
            hq = m.group(1) if m else ''
            return {'ok': True, 'latency_ms': ms,
                    'detail': ('通达信可用 数据日期 ' + hq) if hq else '通达信可用'}
        if source in ('ai.primary', 'ai.backup'):
            # v2.112: 发一次真实最小请求 (max_tokens=1)。**只回文本, 绝不回显 key。**
            from quant_terminal import ai_news
            p = dict((load().get('ai') or {}).get(source.split('.')[1]) or {})
            p['_slot'] = source.split('.')[1]
            return ai_news.probe(p)
        if source.startswith('local.'):
            return _test_local(source[6:], t0)
        return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000), 'detail': f'未知源: {source}'}
    except Exception as e:
        return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000), 'detail': str(e)[:80]}


def _test_local(key: str, t0: float) -> dict:
    cfg = load().get('local', {})
    # v2: 多文件字段 (快照/模式池子) — 逐文件存在性
    if key in ('snapshot_files', 'pool_mode_100', 'pool_mode_55', 'pool_mode_20'):
        base = cfg.get('snapshot_dir', '选股池')
        names = cfg.get(key, [])
        ok = [f for f in names if os.path.exists(file_path(f, base))]
        missing = [os.path.basename(str(f)) for f in names if f not in ok]
        detail = f'{len(ok)}/{len(names)} 个存在' + (f', 缺: {",".join(missing)}' if missing else '')
        return {'ok': len(ok) > 0, 'latency_ms': int((time.time() - t0) * 1000), 'detail': detail}
    path = resolve(str(cfg.get(key, '')))
    if not os.path.exists(path):
        return {'ok': False, 'latency_ms': int((time.time() - t0) * 1000), 'detail': f'不存在: {path}'}
    detail = ''
    if key == 'kline_dir':
        n = len(glob.glob(os.path.join(path, '*.csv')))
        detail = f'{n} 个CSV'
    elif key == 'snapshot_dir':
        names = [f for f in cfg.get('snapshot_files', []) if os.path.exists(file_path(f, path))]
        missing = [os.path.basename(str(f)) for f in cfg.get('snapshot_files', []) if f not in names]
        detail = f'快照 {len(names)}/{len(cfg.get("snapshot_files", []))} 个' + (f', 缺: {",".join(missing)}' if missing else '')
    elif os.path.isdir(path):
        detail = '目录存在'
    else:
        mt = time.strftime('%Y-%m-%d', time.localtime(os.path.getmtime(path)))
        detail = f'文件存在 (更新于 {mt})'
    return {'ok': True, 'latency_ms': int((time.time() - t0) * 1000), 'detail': detail}


# ============================================================
# 涨停池快照降级识别 (dragon.py 用)
# ============================================================
# v2.68 (2026-09-12): 表头名定位 — 用户把快照表头从 28 列精简到 13 列后, 旧的按位置取值
#   (p[3]现价 / p[22]昨收 / len(p)>=25) 被长度检查整行挡掉 → **静默返回 0 只** (不报错),
#   涨停池降级路径彻底失效。与 import_ths_snapshot v4.0 同款: 按列名找索引, 列顺序/数量任意。
#   必需列缺一 → 跳过该文件 (宁可不识别, 绝不按错列取值)。
_ZT_COLS = ('代码', '名称', '现价', '昨收')
_ZT_DIAG = {'skipped': []}   # 最近一次调用的诊断 [(文件名, 原因)], 供降级提示透传


def _snapshot_header_ix(header_line: str) -> dict:
    """表头行 → {列名: 索引}; 重名列取第一个, 空列名(同花顺导出末尾常带一列空表头)忽略"""
    hdr = [h.strip() for h in header_line.rstrip('\r\n').split('\t')]
    ix = {}
    for i, h in enumerate(hdr):
        if h and h not in ix:
            ix[h] = i
    return ix


def _cell(p: list, i) -> str:
    """按索引安全取单元格 (行短于表头时返回 '', 不抛 IndexError)"""
    return p[i].strip() if (0 <= i < len(p)) else ''


def _f(s: str) -> float:
    """'--'(停牌/无数据) / '' / 非数字 → 0.0"""
    try:
        return float(s)
    except ValueError:
        return 0.0


def snapshot_zt_codes() -> list:
    """从同花顺快照识别当日涨停: 现价/昨收 >= 分板块阈值。
    返回 [{code, name, pct, price}] — 无封板质量/连板等字段 (降级能力有限, 需提示)"""
    base = str(get('local', 'snapshot_dir', '选股池'))
    files = get('local', 'snapshot_files', [])
    out = []
    _ZT_DIAG['skipped'] = []
    for fname in files:
        fpath = file_path(fname, base)
        if not os.path.exists(fpath):
            continue
        with io.open(fpath, encoding='gbk', errors='replace') as fh:
            lines = fh.readlines()
        if not lines:
            continue
        ix = _snapshot_header_ix(lines[0])
        miss = [c for c in _ZT_COLS if c not in ix]
        if miss:
            _ZT_DIAG['skipped'].append((os.path.basename(str(fname)), '缺列 ' + ' '.join(miss)))
            continue
        i_code, i_name = ix['代码'], ix['名称']
        i_price, i_prev = ix['现价'], ix['昨收']
        for line in lines[1:]:
            p = line.split('\t')
            code = _cell(p, i_code)
            num = code[2:] if code[:2] in ('SH', 'SZ') else code
            if not num.isdigit():
                continue
            price = _f(_cell(p, i_price)); prev = _f(_cell(p, i_prev))
            if price <= 0 or prev <= 0:
                continue
            thr = 19.8 if num.startswith(('30', '68')) else 9.8
            pct = (price - prev) / prev * 100
            if pct >= thr:
                out.append({'code': num, 'name': _cell(p, i_name),
                            'pct': round(pct, 2), 'price': price})
    return out


def _snapshot_zt_available() -> bool:
    base = str(get('local', 'snapshot_dir', '选股池'))
    files = get('local', 'snapshot_files', [])
    ok = sum(1 for f in files if os.path.exists(file_path(f, base)))
    return ok >= 3  # 至少3/5个快照文件才认为降级可用


if __name__ == '__main__':
    cfg = load()
    print(f"配置: {CFG_FILE}")
    print(f"  broken={is_broken()}")
    print(f"  实时主源={cfg['online']['realtime']['primary']} 备源={cfg['online']['realtime']['backup']}")
    print(f"  K线目录={cfg['local']['kline_dir']}")
    print(f"  本地K线={cfg['local']['kline_dir']}  涨停史={cfg['local']['limit_history']}")
    ok, errs = validate(cfg)
    print(f"  校验: {'通过' if ok else errs}")
