# -*- coding: utf-8 -*-
"""新闻 AI 影响分析 (v2.132b, 2026-10-01)
=========================================
点一条新闻 → 调 LLM → 得到这条新闻对 A 股的影响。

设计要点
--------
- **只依赖 datasource (读配置)**, 不 import server —— 避免循环导入。
  新闻条目由 server 传参进来; 正文由 server 拉取时经 stash_raw() 旁路存下。
- 配置住在 data_sources.json 的顶层 `ai` 段 —— 跟其它数据源**同一个文件**,
  于是天然共用那个「数据源设置」窗口 (用户要求 key 填在那儿)。
- key 三级优先: ai.<slot>.key → 环境变量 → ~/.modlens/config.json (按 host 匹配)。
- 流式直连, 产出 (event, payload) 元组流; 由 server 的 SSE handler 负责写帧。
- 内存缓存 (**不落盘**) + 每 id 一把锁做在途去重 (同一条连点不重复计费)。

⚠️ 本文件是**纯 LF**, 无 BOM —— 跟 macro_daily.py / parent_trend.py 同款。
   项目的「纯 CRLF」约束只适用于改 server.py / terminal.html, 别套到这里。

已知实测事实 (别再摸索)
-----------------------
- 正文在两家的原始字段里都有: 财联社 `content` / 同花顺 `digest`。
  归一化时被 `x.get('title') or x.get('brief')` 当兜底吃掉了, 所以从**原始** dict 抄。
- 正文极短 (中位 82~83 字, 实测最长 315), **无 HTML 标签、无空值**。
- **正文常把标题又抄一遍** (2026-10-01 实测池子 192 条): 44 条正文==标题, 31 条正文=标题+尾巴,
  5 条标题夹在正文中间 ⇒ 共 **80 条 (42%) 的标题逐字出现在正文里**。这类**只喂正文**
  (用户定案 2026-10-01, 见 build_messages / _build_batch);
  另外 112 条标题与正文是两段不同的话 (其中 18 条标题措辞在正文里覆盖率 <60%), 两行都要喂。
- **剥壳的 60 字上限只留给"模糊包含"** (v2.132b): 财联社聚合类标题很长 (实测「隔夜全球要闻(…)」
  标题 76 字 / 括号内 77 字), 旧版 `if not (0 < j <= 60)` 直接放弃剥壳 ⇒ 正文开头白重复一遍标题。
  现在**逐字相等无条件剥**, 只有"包含关系"才要求括号 ≤60 字 (防误伤正文自带的 `【重要】`)。
- `glm-4.7-flash` 是**推理模型**: 流式先吐一堆 `reasoning_content`, `max_tokens`
  给小了正文会是 0 字 (实测 max_tokens=40 → 正文 0 块)。故默认用 `glm-4-flash`,
  且解析时 `content` 与 `reasoning_content` 分开走 (后者只报字数, 不给用户看)。
"""
import os, re, json, time, threading

from quant_terminal import datasource as ds

try:
    import requests
except Exception:                                    # pragma: no cover
    requests = None

MODLENS_CFG = os.path.join(os.path.expanduser('~'), '.modlens', 'config.json')

# 两个槽位 → (环境变量名, 内置默认值)。**key 永不进日志/响应/异常消息。**
SLOTS = ('primary', 'backup')
ENV_KEY = {'primary': 'AI_PRIMARY_KEY', 'backup': 'AI_BACKUP_KEY'}

BODY_FIELD = {'cls': ('content', 'brief'), 'ths': ('digest', 'short')}
BODY_MAX = 1500          # 实测最长 315, 留足余量兜住"早报聚合"类
BODY_LRU = 800           # v2.217: 新闻池上限 600 → 留 200 余量 (原 200→400)

CACHE_MAX = 300          # 内存缓存条数上限 (不落盘: 新闻只在一两天内有意义)
CHUNK_N = 3              # 缓存命中时把整段文字分几块回放 (让前端动画与流式一致)

# ============================================================
# System Prompt —— **这就是产品本身**, 改它 = 改输出质量
# ============================================================
# 纯文本 + 【】标签, 前端靠 white-space:pre-wrap 渲染, 零依赖 (不引 Markdown 渲染器)。
# 三条最要紧的约束 (按对交易者的危害排序):
#   ① 不许编造股票代码/数字 —— 编个六位代码比说错话危险得多
#   ② 不许假装有行情数据 —— 本模块**只喂了这一条新闻正文**, 别的什么都没有
#   ③ 不许给买卖建议 —— AI 是用户框架的**输入**, 不是替代品
SYSTEM_PROMPT = """你是一位专注 A 股市场的财经新闻分析师, 服务对象是中线趋势交易者(持股数周到数月, 专注科技板块)。

【你的唯一输入】
一条财经新闻 (标题与正文; 若正文里已经含标题, 就只给正文 —— 那不是缺了标题)。
你没有行情数据、没有本地数据库、不知道当前大盘点位与板块涨跌。

【铁律 —— 违反任何一条即为失败】
1. 不许编造股票代码。新闻里没点名的公司, 一个 6 位数字代码都不许出现。
   新闻里点名了公司, 就用新闻里的写法, 不要补全成代码。
2. 不许编造数字。价格、涨幅、市占率、订单金额 —— 新闻里没有就不许说。
3. 不许假装你有行情数据。禁止"根据当前行情""结合板块走势""从技术面看"这类话。
   你只看到这一条新闻。
4. 不许给买卖建议。禁止出现: 买、卖、加仓、减仓、建仓、清仓、目标价、止损、
   止盈、仓位、建议持有、可以介入。
5. 不许猜。信息不足就明说"信息不足", 并把它写进【不确定项】。
6. 不做新闻未提及的背景假设。要提只能写成条件句: "若…则…"。

【判定噪音 —— 这是你最重要的职责】
舆情里约八成是噪音。**宁可误判为噪音, 也不要把噪音包装成信号。**
以下一律判为噪音:
  a. 行情播报类: "某指数涨跌X%""某期货收涨""两市成交额X亿""某板块午后拉升"
  b. 无 A 股映射的海外数据: 美债中标利率、LME 金属收盘、欧洲股市收盘、某国 CPI
  c. 会议/讲话套话: 只有"表示将推进/深化/加强"而无具体产业指向
  d. 同一事件的第 N 次重复报道, 且无新增事实
  e. 已被充分预期的例行操作 (每月例行的公开市场操作、例行发布会)
  f. **正文是多条快讯的聚合** (形如 "1、… 2、… 3、…", 同花顺早报/晚报常见):
     逐条过一遍, 只有其中某一条本身够得上有效信号, 才按那一条判; 全都不够 → 噪音,
     并在【理由】里写明"聚合条目, 无单条有效信号"。
★ 一致性判据: 如果你写完发现**三个层级全是"无"且【受影响方向】也是"无"**, 那这条
  就是噪音 —— 不要前面写"弱信号"、后面配一整排"无", 那是自相矛盾的。回到短格式。
注意: 判噪音 ≠ 没影响。突发地缘冲突、监管转向、龙头公司重大变化, 即使与科技板块
无关, 也可能是大盘级信号 —— 这些不许判噪音。

【分层判断】
三个层级各判"有/无", 不要为了凑满而硬写:
  个股级 —— 新闻直接点名了某家 A 股上市公司
  板块级 —— 影响某个细分行业或主题 (如"存储芯片""算力租赁""固态电池")
  大盘级 —— 影响整体风险偏好、流动性或指数
层级用**细分口径**: 不要写"科技板块", 要写"存储芯片""光模块"这种具体方向。

【力度锚点 —— 严格按这个打分, 不要凭感觉】
  1 = 情绪扰动, 1 日内消退, 无实质基本面变化
  2 = 有明确方向, 影响 1-3 日
  3 = 影响 1-2 周, 能改变某个细分方向的短期节奏
  4 = 影响数周以上, 改变某方向的景气预期或竞争格局
  5 = 改变大盘级风险偏好, 或产业链级别的重构
弱信号 = 力度 1-2; 有效信号 = 力度 3-5。

【是否已被市场反应】四选一, 并给一句依据 (这是推理, 不是事实):
  未反映 —— 收盘后发布, A 股尚未交易过
  部分反映 —— 市场早有预期, 或相关方向近期已有异动
  已反映 —— "利好落地"型: 相关方向在消息前已充分上涨
  无法判断 —— 你没有行情数据, 确实判不了 (允许且鼓励用这个)

【输出格式 —— 严格遵守, 不要加任何解释性前后缀】

若判定为噪音, 只输出这三行:
【结论】噪音 · <类型: 行情播报/海外数据/套话/重复报道/聚合条目>
【判定】噪音
【理由】<一句话, 40 字以内>

若不是噪音, 按下面骨架输出, 字段名与方括号一字不改:

【结论】<一句话, 40 字以内, 直接说这是什么、对什么有影响>
【判定】弱信号 | 有效信号
【方向】偏多 | 偏空 | 中性 | 双向
【力度】强 | 中 | 弱 (力度分 <1-5 的整数>)
【时间窗】盘中 | 1-3日 | 1-2周 | 1-3月 | 持续
【层级】
  个股 · <有|无> <有则写点名公司 + 一句话; 无则写"新闻未点名 A 股公司">
  板块 · <有|无> <有则写细分方向名 + 一句话>
  大盘 · <有|无> <有则写一句话>
【是否已反映】<未反映|部分反映|已反映|无法判断> —— <一句话依据>
【受影响方向】<受益或受损的细分行业/主题名, 1-3 个, 顿号分隔; 无则写"无">
【不确定项】<你最可能判错的 1 条, 一句话>

【风格】
简体中文, 克制、具体, 不堆形容词, 不写"值得关注""建议留意"。
每条判断都要能追溯到新闻里的某个具体事实。"""


# ============================================================
# 正文旁路 (server 在归一化**之前**调 stash_raw)
# ============================================================
_BODY = {}
_BODY_LOCK = threading.RLock()


def clean_body(s):
    """正文只做最小清洗: 掐掉万一夹带的 HTML、压掉连续空白、硬截断。"""
    s = str(s or '')
    s = re.sub(r'<[^>]{1,200}>', ' ', s)
    s = re.sub(r'[ \t　]+', ' ', s)
    s = re.sub(r'\n{2,}', '\n', s)
    return s.strip()[:BODY_MAX]


def strip_headline(b, title):
    """财联社正文常以「【标题】…」开头 —— 与标题**逐字重复**, 剥掉。

    ⚠️ 只在括号内容**确实就是标题**(或标题的头/尾)时才剥 —— 否则那是正文自带的
       【】小标题, 剥了就丢信息。逐字相等时无条件剥（**不受 60 字上限约束**, v2.132b 修）;
       只是包含关系时要求够长且括号够短, 免得把正文里的 `【重要】` 之类误伤。
    ⚠️ 这是**唯一**的剥壳入口 (build_messages / _clean_body / 前端展示共用)。
       别再各写一份 —— 之前那两份的口径就不一致 (上限 60 vs 40, 两个不同判据),
       同一条正文在单条分析和批量打分上会一个剥一个不剥。
    """
    b = str(b or '').strip()
    t = str(title or '').strip()
    if not t or not b.startswith('【'):
        return b
    j = b.find('】')
    if j <= 0:
        return b
    bc = b[1:j].strip()
    if not bc:
        return b
    if bc == t:                      # 逐字相等 → 无条件剥 (v2.132b: 不再卡 60 字上限)
        return b[j + 1:].strip()
    if j <= 60 and len(bc) >= 6 and (bc in t or t in bc):
        return b[j + 1:].strip()     # 只是包含关系 → 仍要求"括号够短 + 内容够长"
    return b


def stash_raw(src, raw):
    """把**原始**上游条目里的正文抄进旁路。

    ⚠️ 刻意不碰 _news_norm_cls/_news_norm_ths —— 那两个保证 items 恰好 7 个字段,
       同时被 NEWS_CACHE / /api/news 响应 / 前端三处消费, 是条硬契约。
       正文一律走旁路, **任何情况下都不要往 items 里加字段**。
    """
    fields = BODY_FIELD.get(src)
    if not fields or not raw:
        return
    pre = 'C' if src == 'cls' else 'T'
    now = time.time()
    with _BODY_LOCK:
        for x in raw:
            if not isinstance(x, dict):
                continue
            txt = ''
            for f in fields:
                v = x.get(f)
                if v:
                    txt = str(v)
                    break
            if not txt:
                continue
            _BODY[pre + str(x.get('id'))] = {'b': clean_body(txt), 'ts': now}
        if len(_BODY) > BODY_LRU:                      # 只增不删 + LRU 上限
            for k in sorted(_BODY, key=lambda k: -_BODY[k]['ts'])[BODY_LRU:]:
                _BODY.pop(k, None)


def body_of(news_id):
    with _BODY_LOCK:
        e = _BODY.get(news_id)
    return e['b'] if e else ''


def display_body(news_id, title=''):
    """给**前端展示**用的正文 —— 剥掉与标题重复的【…】。空串 = 没有正文。

    ⚠️ `_BODY` 是 LRU (上限 400), 掉出去的老新闻返回空串, 前端据此**不渲染**正文块。
    """
    b = body_of(news_id)
    return strip_headline(b, title) if b else ''


def body_n():
    with _BODY_LOCK:
        return len(_BODY)


# ============================================================
# 配置
# ============================================================
def ai_cfg():
    """读 data_sources.json 的 `ai` 段 (ds.load() 自带 mtime 缓存)。"""
    c = ds.load().get('ai') or {}
    out = {'enabled': bool(c.get('enabled', True)),
           'timeout': int(c.get('timeout', 60) or 60),
           'max_tokens': int(c.get('max_tokens', 1200) or 1200)}
    for s in SLOTS:
        p = c.get(s) or {}
        out[s] = {'name': str(p.get('name') or ('智谱' if s == 'primary' else 'DeepSeek')),
                  'base': str(p.get('base') or '').strip(),
                  'model': str(p.get('model') or '').strip(),
                  'key': str(p.get('key') or '').strip(),
                  # v2.115: 🔴 **必须显式透传** —— 上面这个 dict 是**重建**的,
                  #   漏一个键 = 那个配置项静默失效 (probe/_stream_once 都读不到)。
                  'thinking': str(p.get('thinking') or '').strip()}
    return out


def _thinking_field(slot_cfg):
    """v2.115: 推理模型的「思考开关」→ 请求体里的 `thinking` 字段。

    🔴 为什么需要它: 智谱 GLM-4.5 及以上的 **Flash 档是推理模型** —— 流式先吐
       `reasoning_content`, 把 token 预算全烧在思考上。2026-09-25 实测 `glm-4.7-flash`
       在 `max_tokens=1200`(终端实参) 下: 思考 2172 字 / **正文 0 字** / `finish_reason=length`
       / 37.6s。挂上 `{'type':'disabled'}` 后同一题: 正文 150 字 / 8.6s / `stop`。

    🔒 逐槽存 (`data_sources.json` 的 `ai.<slot>.thinking`), 因为它**只对智谱系有效**。
       实测 `glm-4-flash` 收到该字段**不报错**(静默忽略) ⇒ 日后换回普通模型**不必改回来**。

    ⚠️ 取值**不白名单**: 只判空。写错值(如 'disable')会被上游以 400 挡回来 ——
       **报错好过静默不生效** (本项目铁律: 静默降级比彻底失败更坏)。
    """
    v = str((slot_cfg or {}).get('thinking') or '').strip()
    return {'thinking': {'type': v}} if v else {}


def _host(url):
    try:
        return (urllib_host(url) or '').lower()
    except Exception:
        return ''


def urllib_host(url):
    import urllib.parse
    h = urllib.parse.urlparse(str(url or '')).hostname
    return h or ''


def _modlens_keys():
    """~/.modlens/config.json 的 {host: (apiKey, baseUrl)} —— 只取这两个字段。

    ⚠️ **只取 key 和 baseUrl, `model` 读后即弃**: 那儿的 model 是 glm-4v-flash
       (视觉模型), 拿来跑文本分析是错的。也不要试图用正则去猜"带 v 的是视觉模型"
       (glm-4v-flash、将来的 glm-4.6v-flash…规则只会越写越脆) —— 直接不用。
    """
    out = {}
    try:
        with open(MODLENS_CFG, 'r', encoding='utf-8') as fh:
            cfg = json.load(fh)
        for v in ((cfg.get('providers') or {}).values()):
            if not isinstance(v, dict):
                continue
            k, b = str(v.get('apiKey') or '').strip(), str(v.get('baseUrl') or '').strip()
            if k and b:
                out[_host(b)] = k
    except Exception:
        pass
    return out


def resolve_key(slot_cfg):
    """key 三级优先。返回 (key, 来源说明)。**来源说明里绝不含 key 本身。**"""
    if slot_cfg.get('key'):
        return slot_cfg['key'], 'settings'
    env = os.environ.get(ENV_KEY.get(slot_cfg.get('_slot') or '', '') or '')
    if env and env.strip():
        return env.strip(), 'env'
    ph = _host(slot_cfg.get('base'))
    if ph:
        for mh, mk in _modlens_keys().items():
            if mh == ph or mh.endswith('.' + ph) or ph.endswith('.' + mh):
                return mk, 'modlens'
    return '', ''


def slot_state(slot_cfg):
    """给 /api/news/ai/status 与设置窗口用。**只回 name/base/model/ready/why。**"""
    key, src = resolve_key(slot_cfg)
    return {'name': slot_cfg.get('name') or '', 'base': slot_cfg.get('base') or '',
            'model': slot_cfg.get('model') or '', 'ready': bool(key),
            'thinking': str(slot_cfg.get('thinking') or '').strip(),  # v2.115 诊断用
            'key_source': src,                       # '' 表示没有
            'why': '' if key else '未配置 key'}


def status():
    c = ai_cfg()
    chain = []
    for s in SLOTS:
        d = dict(c[s])
        d['_slot'] = s
        st = slot_state(d)
        st['slot'] = s
        chain.append(st)
    return {'ok': True, 'enabled': c['enabled'], 'chain': chain,
            'in_flight': len(_INFLIGHT), 'cache_n': len(_CACHE), 'body_n': body_n()}


# ============================================================
# 缓存 + 在途去重
# ============================================================
_CACHE = {}                    # news_id -> {'text','prov','model','ms','ts'}
_INFLIGHT = {}                 # news_id -> threading.Event
_LOCK = threading.RLock()


def _cache_get(news_id):
    with _LOCK:
        return _CACHE.get(news_id)


def _cache_put(news_id, rec):
    with _LOCK:
        _CACHE[news_id] = rec
        while len(_CACHE) > CACHE_MAX:                 # dict 保序 → 丢最早插入的
            _CACHE.pop(next(iter(_CACHE)), None)


def _claim(news_id):
    """抢占。返回 (event, 我是不是第一个)。"""
    with _LOCK:
        ev = _INFLIGHT.get(news_id)
        if ev is not None:
            return ev, False
        ev = threading.Event()
        _INFLIGHT[news_id] = ev
        return ev, True


def _release(news_id):
    with _LOCK:
        ev = _INFLIGHT.pop(news_id, None)
    if ev:
        ev.set()


def mark_done(news_id):
    """server 的 SSE handler 在 finally 里调 —— 兜住任何没走到 _release 的路径。"""
    _release(news_id)


def _chunk(text, n):
    """把整段文字切成 n 块 —— 缓存命中时回放, 让前端动画与流式一致。"""
    if n < 2 or len(text) < 24:
        return [text]
    step = max(1, len(text) // n)
    return [text[i:i + step] for i in range(0, len(text), step)]


# ============================================================
# Prompt 组装
# ============================================================
def build_messages(item, body):
    """喂给模型的就【时间/来源/标题/正文】这几行 —— 用户定案: 不喂任何本地个股/板块状态。

    v2.132 (2026-10-01) 用户定案: **正文里已经含标题时只喂正文**, 不再把同一句话喂两遍;
    本来就没有正文的, 就只喂标题 (原样保留"无正文"提示行, 让模型自己标信息不足)。
    标题与正文互不包含时 (112/192) 两行都喂 —— 那是两段不同的话, 删标题会丢信息。
    """
    src = '财联社' if item.get('src') == 'cls' else '同花顺'
    title = str(item.get('title') or '').strip()
    body = str(body or '').strip()
    # 财联社正文常以「【标题】…」开头 → 剥掉, 免得标题重复一遍 (口径见 strip_headline)
    body = strip_headline(body, title)
    t = time.localtime(int(item.get('t') or 0) or time.time())
    lines = ['【时间】%s' % time.strftime('%m-%d %H:%M', t),
             '【来源】%s' % src]
    if body and title and title in body:
        # 正文已逐字包含标题 (实测 80/192) → 只喂正文, 标题不重复
        lines.append('【正文】%s' % body)
    elif body:
        lines += ['【标题】%s' % (title or '(无标题)'),
                  '【正文】%s' % body]
    else:
        # 没正文 → 只有标题这一行信息
        lines += ['【标题】%s' % (title or '(无标题)'),
                  '【正文】(无正文, 仅有标题 —— 请据此判断并注明信息不足)']
    return [{'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': '\n'.join(lines)}]


# ============================================================
# 流式调用
# ============================================================
class _AIErr(Exception):
    def __init__(self, code, msg):
        Exception.__init__(self, msg)
        self.code, self.msg = code, msg


def _http_err(r):
    body = ''
    try:
        body = (r.text or '')[:300]
    except Exception:
        pass
    st = r.status_code
    code = 'http_%d' % st
    if st in (401, 403):
        code = 'auth'
    elif st == 429:
        code = 'rate_limit'
    elif st == 503:
        code = 'overload'
    elif st in (400, 404) and ('模型不存在' in body or '1211' in body or '1210' in body):
        code = 'model_gone'
    return code, 'HTTP %d %s' % (st, body.replace('\n', ' ')[:160])


def _open_stream(url, key, payload, timeout):
    """POST + stream=True。

    ★ **先直连, 失败才回退系统代理** (v2.239 2026-10-29 用户 m13487 定案, 与 net.py 口径统一)。
      DeepSeek(api.deepseek.com) 与 智谱(open.bigmodel.cn) 都是**境内**服务,
      实测 TLS 直连 77ms / 79ms 正常; 本机 SSR 代理只是多一跳, 且代理一挂就整条链路瘫。
      回退代理仍然保留 —— 万一哪天直连被挡(出差/换网络)还有退路。
    """
    if requests is None:
        raise _AIErr('no_dep', 'requests 不可用')
    hdr = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + key}
    last = None
    for proxies in ({'http': None, 'https': None}, None):
        try:
            r = requests.post(url, json=payload, headers=hdr, stream=True,
                              timeout=(10, timeout), proxies=proxies)
            if r.status_code != 200:
                code, msg = _http_err(r)
                try:
                    r.close()
                except Exception:
                    pass
                raise _AIErr(code, msg)
            return r
        except _AIErr:
            raise
        except Exception as e:
            last = e
    # ⚠ 不把 str(e) 往外带: 某些库的异常消息可能带完整 URL (query 里可能有 key)
    raise _AIErr('net', type(last).__name__ if last else 'network error')


def probe(slot_cfg, timeout=8):
    """真实最小请求 (max_tokens=1) —— 给设置窗口的「测试」按钮用。

    跟其它数据源的 ds.test() 同款语义: 返回 {ok, latency_ms, detail}。
    ⚠️ detail 里**只允许出现** name/model/key来源, 绝不出现 key 本身或其任何片段。
    """
    if requests is None:
        return {'ok': False, 'latency_ms': 0, 'detail': 'requests 不可用'}
    key, ksrc = resolve_key(slot_cfg)
    if not key:
        return {'ok': False, 'latency_ms': 0, 'detail': '未配置 key'}
    url = str(slot_cfg.get('base') or '').rstrip('/') + '/chat/completions'
    payload = {'model': slot_cfg.get('model') or '', 'max_tokens': 1,
               'messages': [{'role': 'user', 'content': 'hi'}]}
    payload.update(_thinking_field(slot_cfg))   # v2.115: 「测试」必须跟真实调用同形
    hdr = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + key}
    t0, r, last = time.time(), None, None
    for proxies in ({'http': None, 'https': None}, None):
        try:
            r = requests.post(url, json=payload, headers=hdr,
                              timeout=(6, timeout), proxies=proxies)
            break
        except Exception as e:
            last, r = e, None
    ms = int((time.time() - t0) * 1000)
    if r is None:
        # ⚠ 不往外带 str(e): 某些库的异常消息可能含完整 URL
        return {'ok': False, 'latency_ms': ms,
                'detail': '网络失败: %s' % (type(last).__name__ if last else '?')}
    if r.status_code == 200:
        return {'ok': True, 'latency_ms': ms,
                'detail': '%s · %s · key来自%s' % (slot_cfg.get('name') or '',
                                                   slot_cfg.get('model') or '', ksrc)}
    code, msg = _http_err(r)
    return {'ok': False, 'latency_ms': ms, 'detail': '%s %s' % (code, msg)}


def _stream_once(slot, key, msgs, cfg, tools=None):
    """跑一家。产出 ('think', n) / ('delta', text) / ('done', dict)。

    v2.116: 加 `tools` —— 不传 = **与加这个参数之前逐字节同行为**（新闻分析走的就是这条，
    不能变）；传了才把 tool_calls 累积出来，随 done 一起给（不新开事件类型，
    免得新闻那条 SSE 上多出个没人监听的事件）。

    ⚠️ 参数必须**按 index 累积**再拼：实测 DeepSeek 把一次调用的 arguments 拆成
    11 个分片吐（智谱只 1 个），简单拼接在多家/多调用并存时必错。
    """
    url = slot['base'].rstrip('/') + '/chat/completions'
    payload = {'model': slot['model'], 'messages': msgs, 'stream': True,
               'max_tokens': cfg['max_tokens'], 'temperature': 0.3}
    payload.update(_thinking_field(slot))       # v2.115: 关掉推理模型的思考
    if tools:
        payload['tools'] = tools
        payload['tool_choice'] = 'auto'         # 由模型自己决定调不调 (用户要的就是这个)
    r = _open_stream(url, key, payload, cfg['timeout'])
    buf, think_n, n_delta = [], 0, 0
    tc = {}                     # index -> {id, name, args[]}
    try:
        for line in r.iter_lines():
            if not line:
                continue
            s = line.decode('utf-8', 'replace').strip()
            if not s.startswith('data:'):
                continue
            s = s[5:].strip()
            if s == '[DONE]':
                break
            try:
                j = json.loads(s)
            except Exception:
                continue
            ch = (j.get('choices') or [{}])[0]
            d = ch.get('delta') or {}
            rc = d.get('reasoning_content')          # 推理模型的思考过程: 只报字数
            if rc:
                think_n += len(str(rc))
                yield ('think', {'n': think_n})
            c = d.get('content')
            if c:
                n_delta += 1
                buf.append(str(c))
                yield ('delta', {'t': str(c)})
            for one in (d.get('tool_calls') or []):
                if not isinstance(one, dict):
                    continue
                k = one.get('index')
                k = 0 if k is None else k
                e = tc.setdefault(k, {'id': '', 'name': '', 'args': []})
                if one.get('id'):
                    e['id'] = str(one['id'])        # 只在首片出现
                fn = one.get('function') or {}
                if fn.get('name'):
                    e['name'] += str(fn['name'])     # 理论上只首片, 但拼着更稳
                if fn.get('arguments'):
                    e['args'].append(str(fn['arguments']))
    finally:
        try:
            r.close()
        except Exception:
            pass
    calls = [{'id': v['id'], 'name': v['name'], 'args': ''.join(v['args'])}
             for _, v in sorted(tc.items())]
    text = ''.join(buf)
    # ★ 只吐 tool_calls 不吐正文 = **模型正常行为**(它要先去查数据), 不是空响应。
    #   漏了 `and not calls` 的话这里会误报 stream_empty, 工具调用永远走不到下一步。
    if not text.strip() and not calls:
        # ★ 实测踩到过: HTTP 200、[DONE] 正常, 但 content 累计 0 字
        #   (推理模型 max_tokens 给小了, 预算全被 reasoning 吃光)
        raise _AIErr('stream_empty',
                     '返回 200 但正文为空 (思考 %d 字)' % think_n if think_n else '返回 200 但正文为空')
    yield ('done', {'text': text, 'think_n': think_n, 'n_delta': n_delta,
                    'tool_calls': calls})


# ============================================================
# 降级链
# ============================================================
def _meta(news_id, slot, cached, body, extra=None):
    m = {'id': news_id, 'provider': slot.get('name') or '', 'model': slot.get('model') or '',
         'cached': bool(cached), 'body': body or '', 'body_n': len(body or '')}
    if extra:
        m.update(extra)
    return m


def _run_chain(news_id, item, cfg, body):
    """按 primary → backup 顺序试。**已经吐过 delta 就不再降级** —— 半截答案收不回来。"""
    msgs = build_messages(item, body)
    tried, why = [], []
    for s in SLOTS:
        slot = dict(cfg[s])
        slot['_slot'] = s
        st = slot_state(slot)
        if not st['ready']:
            why.append('%s(%s)' % (slot['name'], '未配置 key'))
            continue
        key, _src = resolve_key(slot)
        tried.append(slot['name'])
        t0 = time.time()
        got_any = False
        try:
            for ev, p in _stream_once(slot, key, msgs, cfg):
                if ev == 'done':
                    rec = {'text': p['text'], 'prov': slot['name'], 'model': slot['model'],
                           'ms': int((time.time() - t0) * 1000), 'ts': time.time()}
                    _cache_put(news_id, rec)
                    yield ('done', dict(p, **{'ms': rec['ms'], 'provider': slot['name'],
                                              'model': slot['model'], 'chars': len(p['text']),
                                              'think_n': p.get('think_n', 0), 'cached': False}))
                    return
                if ev == 'delta':
                    got_any = True       # 吐过字 → 后面任何失败都不再降级 (半截答案收不回来)
                yield (ev, p)            # ⚠ delta/think 都必须原样透传出去
        except _AIErr as e:
            if got_any:
                # 🔴 唯一不降级的情况: 已经往客户端吐过字了
                yield ('err', {'code': e.code, 'msg': e.msg, 'partial': True,
                               'tried': tried})
                return
            why.append('%s(%s)' % (slot['name'], e.msg))
            continue
    yield ('err', {'code': 'all_failed', 'partial': False, 'tried': tried,
                   'msg': '全部数据源失败: ' + ' / '.join(why)})


# ============================================================
# 对外主入口
# ============================================================
def analyze_stream(item, force=False):
    """产出 (event, payload) 元组流, event ∈ {meta, think, delta, done, err}。

    ⚠️ 用 `err` 而不是 `error`: 浏览器 EventSource 的**原生 error 事件**与自定义
       `event: error` 会走同一个监听器, `e.data` 时有时无 (现有 listenSSE 就得靠
       try{JSON.parse}catch 硬分)。换个名字, 两边语义都干净。
    """
    news_id = str(item.get('id') or '')
    cfg = ai_cfg()
    body = body_of(news_id)
    blank = {'name': cfg['primary']['name'], 'model': cfg['primary']['model'], '_slot': 'primary'}

    if not cfg['enabled']:
        yield ('err', {'code': 'disabled', 'msg': 'AI 分析已在数据源设置里关闭', 'partial': False})
        return
    if not news_id:
        yield ('err', {'code': 'bad_id', 'msg': '缺少新闻 id', 'partial': False})
        return

    if not force:
        hit = _cache_get(news_id)
        if hit:
            yield ('meta', _meta(news_id, {'name': hit['prov'], 'model': hit['model']}, True, body))
            for c in _chunk(hit['text'], CHUNK_N):
                yield ('delta', {'t': c})
            yield ('done', {'ms': 0, 'provider': hit['prov'], 'model': hit['model'],
                            'chars': len(hit['text']), 'think_n': 0, 'cached': True})
            return

    ev, first = _claim(news_id)
    if not first:
        # 同一条正在生成中 → **不新起调用**, 等它跑完直接复用结果 (不重复计费)
        if ev.wait(cfg['timeout'] + 30):
            hit = _cache_get(news_id)
            if hit:
                yield ('meta', _meta(news_id, {'name': hit['prov'], 'model': hit['model']},
                                     True, body))
                for c in _chunk(hit['text'], CHUNK_N):
                    yield ('delta', {'t': c})
                yield ('done', {'ms': 0, 'provider': hit['prov'], 'model': hit['model'],
                                'chars': len(hit['text']), 'think_n': 0, 'cached': True})
                return
        yield ('err', {'code': 'timeout', 'msg': '同一条分析等待超时, 请重试', 'partial': False})
        return

    try:
        # meta 先发: 前端据此显示"哪家/哪个模型", 以及这条新闻的正文
        slot = dict(cfg['primary'])
        slot['_slot'] = 'primary'
        first_ready = None
        for s in SLOTS:
            d = dict(cfg[s])
            d['_slot'] = s
            if slot_state(d)['ready']:
                first_ready = d
                break
        yield ('meta', _meta(news_id, first_ready or slot, False, body))
        for out in _run_chain(news_id, item, cfg, body):
            yield out
    finally:
        _release(news_id)


# ============================================================
# 批量打标 —— 「这条新闻对 A 股有没有影响」(v2.121)
# ============================================================
# 与上面「单条分析」的分工:
#   单条分析 = 用户点开某条 → 流式长文 (方向/力度/窗口/逻辑/已反应/留意)
#   批量打标 = 后台把**整个池子**过一遍 → 每行一个标签, 供窗口筛选
# 两者**判据必须一致**: 都是"有没有传导到 A 股价格的链条"。
SCORES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ai_news_scores.json')
SCORE_BATCH = 40         # 一批喂几条 (40 × ~150 字正文 ≈ 可接受的 token 量)
SCORE_BODY = 150         # 每条正文截断字数 —— 判"有没有传导路径"够用, 不必喂全文(最长 1500)
SCORE_MAXTOK = 2000      # 40 行 × ~15 token ≈ 600; 富余留给推理模型(它会把预算烧在思考上)
SCORE_KEEP = 1200        # 落盘上限 (v2.217: 池子 ~500 条, 留 2 倍余量; 原 200→600)

# 逐行解析: 序号|影响|方向|力度|层级|理由
# 键值解析: `影响=1` / `方向= -1` / `理由=xxx`
# ⚠ 值里可能再出现字段名(如 理由=出口受影响), 所以要求名字**后面必须跟 = 或冒号**才算键。
_KV_RE = re.compile(r'(序号|影响|方向|力度|层级|理由)\s*[=:：]\s*')
# 行首序号: `[3]` / `#3` / `3.` / `3、` / `3)` / `3:` / `3|` —— 模型爱怎么写都收
_SER_RE = re.compile(r'^\s*(?:\[\s*(\d{1,4})\s*\]|#\s*(\d{1,4})'
                     r'|(\d{1,4})\s*[.、):：|])\s*')
_NUM_RE = re.compile(r'^[-+]?\d+$')

# 🔴 这段 prompt 就是产品本身。三条最要紧的:
#   ① 判据必须是"传导链条", 不能退化成"看起来像不像财经新闻"
#   ② 海外 ≠ 无影响 (美联储/关税/大宗/海外龙头常有真链条), 但纯指数播报算无
#   ③ **利空也算有影响** —— 漏利空比多看噪音危险得多, 必须写死
SCORE_PROMPT = """你在给一批 A 股财经新闻**批量打标**: 逐条判断这条新闻对 A 股**有没有影响**。

【「有影响」的唯一判据: 存在一条能传导到 A 股价格的链条】
政策/监管 → 行业供需 → 公司经营 → 宏观数据 → 资金面流动性 → 指数与板块
只要链条成立就算有影响, **不需要**它今天就会涨或跌。

【两种必须警惕的误判】
- 海外新闻**不自动等于无影响**: 美联储/关税/大宗商品/海外龙头的动作常有真实传导链条。
  但「美股三大指数收涨」这类**纯行情播报**(只是复述涨跌、没有新增信息)算无影响。
  具体例子: 「某海外云厂商上调 AI 资本开支」**算有影响** —— 它直接传导到国内
  算力/光模块/PCB/服务器供应链。判这类新闻时先问一句: **A 股有没有哪个板块是靠它吃饭的?**
- 利空**同样算有影响**(方向填 -1)。**绝不要因为方向是负就判成无影响。**

【无影响】= 与 A 股没有传导关系(体育/娱乐/与国内无关的地方新闻), 或纯行情播报。

【怎么认出"纯行情播报" —— 用这个动作, 别凭感觉】
  把这条新闻里的**数字全删掉**, 再看还剩什么。
  什么都不剩 ⇒ 无影响。高频漏网的四类:
    「恒生指数收盘跌1%」            → 删掉数字, 空
    「LME金属期货收盘涨跌不一」      → 删掉数字, 空 (连方向都没有)
    「美元指数24日上涨」            → 删掉数字, 空
    「阿克迈美股盘前涨超21%」        → 删掉数字, 空
  **例外 (要算有影响)**: 同一句里给了**原因或事件**的, 有影响的是那个事件, 不是涨幅 ——
    「阿克迈盘前涨超21%, 因与 Anthropic 签署 116 亿美元云计算协议」⇒ 有影响。
  同理「X 股涨停/跌停」本身不算, 但它背后的**公告/中标/涨价**如果被写出来了, 算。

【方向怎么填 —— 🔴 最容易错的一格, 单独说】
  别按"这条新闻利好还是利空世界"填, 按 **它把 A 股哪个板块往哪推** 填。
  **同一类消息, 方向可以完全相反**, 取决于链条落在谁身上:
    原油大涨 → 石油开采/油服 受益, 航空/化纤/炼化 受损
    原油大跌 → 正好反过来
  所以 **绝对不要** 见到"原油/天然气/黄金/美元/金属"就统一盖一个方向 ——
  那是本批次里最常见的错误 (实测: 原油涨和原油跌被判成了同一个方向)。
  判之前先问自己一句: **这条消息具体把哪个板块的营收/成本推动了? 往哪边推?**
  真的两边抵消、或看不出净方向, 再填 0。

【输出格式 —— 一行一条, 字段写成 `键=值`, 字段之间空格分隔】
序号=1 影响=1 方向=-1 力度=2 层级=1 理由=光伏供给侧限产

  序号  照抄我给你的编号
  影响  1=有影响  0=无影响
  方向  1=利好  -1=利空  0=中性或看不出
  力度  3=强  2=中  1=弱  0=无
  层级  3=大盘级  2=板块级  1=个股级  0=无
  理由  不超过 12 个字, 写**传导链条本身**(如"光伏供给侧限产"), 不要写"利好A股"这种空话;
        影响填 0 时, 这里写**为什么没影响**(缺了哪一环导致链条断掉), **不要**只写"无影响"三个字

【格式示例 —— **只示范长什么样**。里面的理由一律写作 `示例文字勿抄`,
   它与你手上的新闻**毫无关系**, 不要照抄, 更不要因为见过这两行就倾向于往某个方向判】
序号=1 影响=1 方向=1 力度=3 层级=3 理由=示例文字勿抄
序号=2 影响=0 方向=0 力度=0 层级=0 理由=示例文字勿抄

⚠️ **不要**用竖线 `|`、不要 Markdown 表格、不要代码块、不要写"下面是结果"之类的开场白 ——
   你的回复**整段都是**这种行, 从第一行开始。字段名就上面这六个, 别自造。

【自检】行数必须**正好等于**我给你的条数; 影响填 0 的行, 方向/力度/层级**一律填 0**。"""

_SCORE = {}                    # news_id -> {'i','d','s','l','w','ts'}
_SCORE_LOCK = threading.RLock()
SCORE_STATE = {'running': False, 'done': 0, 'total': 0, 'ok': 0, 'bad': 0,
               'err': '', 'msg': '', 'started': 0.0, 'ts': 0.0}


def _score_load():
    """启动读盘。🔴 **读失败一律当空** —— 文件坏了不该让终端起不来。"""
    try:
        with open(SCORES_FILE, 'r', encoding='utf-8') as fh:
            d = json.load(fh)
        for k, v in ((d.get('scores') or {}) if isinstance(d, dict) else {}).items():
            if isinstance(v, dict) and 'i' in v:
                _SCORE[str(k)] = v
    except Exception:
        pass


def _score_save():
    """原子写。⚠ 失败**不抛** —— 打标丢了可以重算, 不该掀掉整个流程。"""
    try:
        with _SCORE_LOCK:
            snap = dict(_SCORE)
        tmp = SCORES_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fh:
            json.dump({'version': 1, 'ts': time.time(), 'scores': snap}, fh,
                      ensure_ascii=False)
        os.replace(tmp, SCORES_FILE)
    except Exception:
        pass


def _prune_locked():
    """按 ts 丢最旧的。⚠ 不能按 dict 插入序: 覆盖已有键**不会**把它移到末尾,
    那样会把仍在池子里的条目先淘汰掉。"""
    while len(_SCORE) > SCORE_KEEP:
        old = min(_SCORE, key=lambda k: _SCORE[k].get('ts') or 0)
        _SCORE.pop(old, None)


def score_of(news_id):
    with _SCORE_LOCK:
        return _SCORE.get(str(news_id or ''))


def scores_map():
    """给 server 一次性取走 (避免每条新闻一次加锁)。"""
    with _SCORE_LOCK:
        return dict(_SCORE)


def score_state():
    with _SCORE_LOCK:
        n = len(_SCORE)
    return dict(SCORE_STATE, scored_n=n)


def _clean_body(b, title):
    """压空白 + 剥掉与标题重复的【…】(省 token)。剥壳口径统一走 strip_headline。"""
    return strip_headline(re.sub(r'\s+', ' ', b or '').strip(), title)


def _build_batch(rows):
    """rows = [(序号, item, body), ...] → messages。"""
    lines = []
    for no, it, body in rows:
        src = '财联社' if it.get('src') == 'cls' else '同花顺'
        t = str(it.get('title') or '').strip()
        b = _clean_body(body, t)
        if len(b) > SCORE_BODY:
            b = b[:SCORE_BODY] + '…'
        # ⚠ 标记用 `#N` **不用 `N.`** —— 后者与输出格式都拿数字打头, 实测模型会把两者
        #   合并, 行首一个 `N.` + 后面 6 段, 按位置解就整体错位一格 (v2.121b)。
        # v2.132: 正文已逐字含标题 → **只留正文**。否则标题先占一遍、正文前 150 字里
        #   再重复一遍, 等于把 SCORE_BODY 这点预算花在重复上 (实测 80/192 属此类)。
        if b and t and t in b:
            lines.append('#%d [%s] %s' % (no, src, b))
        else:
            lines.append('#%d [%s] %s%s' % (no, src, t, ('　' + b) if b else ''))
    return [{'role': 'system', 'content': SCORE_PROMPT},
            {'role': 'user', 'content': '\n'.join(lines)}]


def _complete(msgs, cfg, max_tokens, slots=None):
    """跑一遍降级链收全文 (**不给用户流式** —— 批量打标没有观看价值)。

    返回 (text, 用了哪家, 错误说明)。错误说明里**绝不含 key**。

    v2.219: 加 `slots` —— **显式指定降级顺序**。
        · 不传 (None) 仍按 SLOTS 配置顺序 ⇒ 与加这个参数之前逐字节同行为；
        · 新闻跨源去重传「免费源优先」的顺序 (见 news_dedup._ai_slots) ——
          用户可以在数据源设置里「主备互换」, 那时 primary 是付费源,
          按配置顺序走会先烧钱。
    """
    c = dict(cfg)
    c['max_tokens'] = max_tokens
    c['timeout'] = max(int(cfg.get('timeout') or 60), 90)   # 一批 40 条, 比单条慢
    why = []
    _order = tuple(slots) if slots else SLOTS
    for s in _order:
        if not isinstance(c.get(s), dict):
            # slots 传了不存在的槽名 (或配置里缺这一槽): 跳过, 不是 KeyError
            why.append('%s(无此槽)' % s)
            continue
        slot = dict(c[s])
        slot['_slot'] = s
        if not slot_state(slot)['ready']:
            why.append('%s(未配置 key)' % slot['name'])
            continue
        key, _src = resolve_key(slot)
        try:
            for ev, p in _stream_once(slot, key, msgs, c):
                if ev == 'done':
                    return p['text'], slot['name'], ''
        except _AIErr as e:
            why.append('%s(%s)' % (slot['name'], e.msg))
        except Exception as e:                       # 不往外带 str(e): 可能含完整 URL
            why.append('%s(%s)' % (slot['name'], type(e).__name__))
    return '', '', ' / '.join(why) or '全部数据源失败'


def _ok(i, d, st, lv):
    return i in (0, 1) and d in (-1, 0, 1) and st in (0, 1, 2, 3) and lv in (0, 1, 2, 3)


def _fields(s):
    """一行 → (序号 or None, 影响, 方向, 力度, 层级, 理由); 解不出返回 None。

    🔴 为什么不按"第几段"取字段: 位置格式只在**段数永远对得上**时才成立。实测模型
    行首多吐一个 `1. `, 段数就多一段, 于是影响/方向全体错位一格 —— 而且错得**很安静**
    (每段数字都合法, 事后无从发现)。所以主路走 `键=值`, 字段靠**名字**定位。

    竖线格式仍兼容(模型不听话时的保险), 但每一段都过 `_ok()` 范围校验,
    并且**序号剥与不剥各试一次**, 取第一个整体合法的解 —— 宁可这条丢掉, 绝不猜。
    """
    # ① 键值 (主路)
    ms = list(_KV_RE.finditer(s))
    if ms:
        kv = {}
        for j, m in enumerate(ms):
            end = ms[j + 1].start() if j + 1 < len(ms) else len(s)
            kv[m.group(1)] = s[m.end():end].strip(' \t|,，;；、')
        # ⚠ 数值字段**只取第一个空白词** —— 模型偶尔自造字段(如 `信心=9`), 它会落进
        #   前一个字段的值里; 整串不匹配就整行丢掉太亏, 取首词即可 (`1 信心=9` → `1`)。
        def num(k):
            v = (kv.get(k) or '').split()
            return int(v[0]) if v and _NUM_RE.match(v[0]) else None
        if num('影响') is not None:
            g = lambda k: num(k) if num(k) is not None else 0    # noqa: E731
            if _ok(g('影响'), g('方向'), g('力度'), g('层级')):
                no = num('序号')
                if no is None:
                    # 行首 `#4` / `[4]` / `4.` 也算序号 (模型两种混用时别丢了对齐依据)
                    m2 = _SER_RE.match(s)
                    no = int(m2.group(1) or m2.group(2) or m2.group(3)) if m2 else None
                return (no, g('影响'), g('方向'), g('力度'), g('层级'),
                        kv.get('理由', '')[:24])
        return None
    # ② 竖线 (兼容)
    body, no = s, None
    m = _SER_RE.match(s)
    for cand, sn in ((s[m.end():], int(m.group(1) or m.group(2) or m.group(3))) if m else (s, None),
                     (s, None)):
        fs = [x.strip() for x in cand.split('|')]
        if len(fs) < 5 or not all(_NUM_RE.match(x) for x in fs[:4]):
            continue
        i, d, st, lv = (int(x) for x in fs[:4])
        w = fs[-1] if len(fs) > 5 else fs[4]
        if _ok(i, d, st, lv):
            return sn, i, d, st, lv, w[:24]
    return None


def _parse_batch(text, valid):
    """逐行解析 + **可靠的对齐**。返回 (命中 {序号: rec}, 坏行数)。

    🔴 为什么不无脑按行号对齐: 模型少写一行, 后面**全部前移一格** —— 把第 7 条的判断
    安到第 8 条头上, 而且数字看着全对, 事后无从发现。这是本模块最坏的失败模式,
    比"这一批全没评分"严重得多。所以:

        全部带序号        → 按序号 (最稳), 序号非法/重复/越界的那行丢掉
        全都不带序号且
          行数 == 条数    → 按**位置**对齐 (行数吻合才敢这么干)
          行数 != 条数    → **整批拒收** (对不齐宁可不评分)
        混着来            → 只信带序号的那几行 (没序号的无法确认位置)

    另外: 影响=0 的行强制把 方向/力度/层级 归零 —— 模型会写"无影响 但 方向=利好"
    这种自相矛盾, 留着会让下游的利好/利空筛选凭空多出条目。
    """
    rows, bad = [], 0
    for ln in (text or '').splitlines():
        s = ln.strip()
        if not s:
            continue
        f = _fields(s)
        if not f:
            bad += 1
            continue
        rows.append(f)
    if not rows:
        return {}, bad
    keyed = [r for r in rows if r[0] is not None]
    if keyed and len(keyed) != len(rows):
        bad += len(rows) - len(keyed)      # 混着来 —— 没序号的位置无法确认, 只能丢
        rows = keyed
    if keyed:
        out = {}
        for no, i, d, st, lv, w in rows:
            if no not in valid or no in out:
                bad += 1
                continue
            out[no] = _rec(i, d, st, lv, w)
        return out, bad
    # 一个序号都没有 —— 只有**行数正好吻合**时才敢按位置对齐
    if len(rows) != len(valid):
        return {}, bad + len(rows)
    return {n: _rec(*r[1:]) for n, r in zip(sorted(valid), rows)}, bad


def _rec(i, d, st, lv, w):
    if i == 0:
        d = st = lv = 0
    return {'i': i, 'd': d, 's': st, 'l': lv, 'w': w, 'ts': time.time()}


def score_items(items, force=False):
    """线程体。items = server 那份新闻列表 (含 id/src/title)。"""
    try:
        cfg = ai_cfg()
        if not cfg['enabled']:
            SCORE_STATE.update({'running': False, 'err': 'disabled',
                                'msg': 'AI 已在数据源设置里关闭'})
            return
        todo = []
        for it in items:
            nid = str(it.get('id') or '')
            if not nid:
                continue
            if not force and score_of(nid):
                continue
            todo.append((nid, it, body_of(nid)))
        SCORE_STATE.update({'total': len(todo), 'done': 0, 'ok': 0, 'bad': 0, 'err': ''})
        if not todo:
            SCORE_STATE.update({'running': False, 'msg': '没有新条目需要判定'})
            return
        nb = (len(todo) + SCORE_BATCH - 1) // SCORE_BATCH
        for pos in range(0, len(todo), SCORE_BATCH):
            chunk = todo[pos:pos + SCORE_BATCH]
            msgs = _build_batch([(i + 1, c[1], c[2]) for i, c in enumerate(chunk)])
            SCORE_STATE['msg'] = '第 %d/%d 批…' % (pos // SCORE_BATCH + 1, nb)
            text, prov, err = _complete(msgs, cfg, SCORE_MAXTOK)
            if err:
                # 一批失败就不再往下 —— 多半是 key/网络问题, 继续只是白烧配额
                SCORE_STATE.update({'err': err, 'msg': '第 %d/%d 批失败'
                                    % (pos // SCORE_BATCH + 1, nb)})
                break
            got, bad = _parse_batch(text, set(range(1, len(chunk) + 1)))
            with _SCORE_LOCK:
                for no, rec in got.items():
                    _SCORE[chunk[no - 1][0]] = rec
                _prune_locked()
            SCORE_STATE.update({'done': SCORE_STATE['done'] + len(chunk),
                                'ok': SCORE_STATE['ok'] + len(got),
                                'bad': SCORE_STATE['bad'] + bad,
                                'msg': '%s · 第 %d/%d 批' % (prov, pos // SCORE_BATCH + 1, nb)})
            _score_save()
        if not SCORE_STATE['err']:
            SCORE_STATE['msg'] = '完成: %d 条已判定' % SCORE_STATE['ok']
    except Exception as e:
        SCORE_STATE.update({'err': '%s: %s' % (type(e).__name__, e)})
    finally:
        if SCORE_STATE['bad'] and not SCORE_STATE['err']:
            SCORE_STATE['msg'] += ' · %d 行没解析出来(已标未评分)' % SCORE_STATE['bad']
        SCORE_STATE.update({'running': False, 'ts': time.time()})
        _score_save()


def start_scoring(items, force=False):
    """起后台线程。**已在跑就直接返回当前状态** —— 不排队、不叠加(重复点只烧钱)。"""
    if SCORE_STATE.get('running'):
        return dict(SCORE_STATE, ok=False, msg='判定已在运行中')
    SCORE_STATE.update({'running': True, 'done': 0, 'total': 0, 'ok': 0, 'bad': 0,
                        'err': '', 'msg': '启动中…', 'started': time.time(), 'ts': 0.0})
    threading.Thread(target=score_items, args=(items, force), daemon=True).start()
    return dict(SCORE_STATE, ok=True)


_score_load()      # 模块加载即读盘 (文件不存在/坏了 ⇒ 空, 见 _score_load)


# ============================================================
# 自测 (不起 8799)
# ============================================================
def _selftest():
    import sys
    print('providers:')
    c = ai_cfg()
    for s in SLOTS:
        d = dict(c[s])
        d['_slot'] = s
        st = slot_state(d)
        print('  %-8s ready=%-5s model=%-22s base=%s%s'
              % (s, st['ready'], st['model'] or '(空)', st['base'] or '(空)',
                 '' if st['ready'] else '  ← ' + st['why']))
    print('body stash: %d 条' % body_n())
    if '--news' not in sys.argv:
        print('\n(加 --news 抽一条真新闻真跑一次流式)')
        return
    try:
        import requests as _rq
        j = _rq.get('https://news.10jqka.com.cn/tapp/news/push/stock/'
                    '?page=1&tag=&track=website&pagesize=10',
                    timeout=8, headers={'User-Agent': 'Mozilla/5.0'}).json()
        raw = (j.get('data') or {}).get('list') or []
    except Exception as e:
        print('拉新闻失败: %s' % e)
        return
    stash_raw('ths', raw)
    it = None
    for x in raw:
        if x.get('digest'):
            it = {'id': 'T' + str(x.get('id')), 'src': 'ths',
                  't': int(x.get('ctime') or 0), 'title': str(x.get('title') or '')}
            break
    if not it:
        print('没抽到带正文的条目')
        return
    print('\n--- %s ---' % it['title'][:40])
    print('正文 %d 字' % len(body_of(it['id'])))
    for ev, p in analyze_stream(it, force=True):
        if ev == 'delta':
            continue
        print('event %-6s %s' % (ev, json.dumps(p, ensure_ascii=False)[:150]))
    print('\n缓存: %d 条' % len(_CACHE))


if __name__ == '__main__':
    _selftest()
