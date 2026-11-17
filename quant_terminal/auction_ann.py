# -*- coding: utf-8 -*-
"""竞价打板 · 公告硬红线核查  (v2.249 新增模块, 2026-11-06)

⚠ 本文件是「竞价打板」功能的独立模块, 与终端现有任何功能**零耦合**。
  摘除方式见 _uninstall_auction.py; 压舱开关见 auction_off.flag。

===============================================================================
为什么要这个模块
===============================================================================
原策略 (F:\\竞价选股 v2.14) 的「公告硬红线一票否决」是人肉查的:
  停牌 / 一字板 / 减持 / 业绩预亏 / 次新破发 / 主营亏损靠投资收益
这些必须程序化, 否则竞价打板在 9:25~9:30 那 5 分钟里根本来不及人工核。

===============================================================================
数据源选型 (2026-11-06 02:50 实测, 五个源对比)
===============================================================================
  巨潮 cninfo hisAnnouncement/query   ❌ HTTP 403 —— 连打 8 次全挂, 等 30 秒仍 403
                                          (WAF 拦非浏览器请求, 且已限流本机 IP)
  同花顺 basic.10jqka.com.cn 公告接口  ❌ 返回空 / 公告页需 JS 渲染
  东财 np-anotice-stock.eastmoney.com  ✅ 10 次连打 10 次成功, 干净 JSON
  东财 datacenter-web.eastmoney.com    ✅ 200  (server.py:801 本来就在用它拉业绩)
  现有新闻流 (财联社/同花顺快讯)        ⚠ 100 条里 0 条含特定个股 —— 新闻是大盘级的

★ 关于 AGENTS.md:104「禁用: 东财 push2」
  本模块用的是 np-anotice-stock / datacenter-web, **不是 push2**。
  datacenter-web 在 server.py:801 生产代码里本来就在用 (拉业绩报表)。
  且本模块做成可一键切走 —— 若被风控, 只需改 ANN_URL 一处。

===============================================================================
设计原则: 规则层兜底, AI 只判模糊
===============================================================================
  第一层 (本文件): 关键词硬规则匹配 —— 确定性强, 漏判风险低, 结果可复现
  第二层 (可选): 模糊标题才交给 AI —— 即使 AI 判错, 也影响不了硬红线

  这个分层是为了防「AI 编造」: 硬红线永远是程序判的, AI 只处理
  「关于全资子公司减资的公告」这种关键词判不出来的模糊项。

===============================================================================
接口
===============================================================================
  fetch_anns(code, size=20, timeout=8)      → (ok, list[dict] | err_str)
  check_one(code, days=30, size=20)         → dict  单票核查结果
  check_batch(codes, days=30, workers=12)   → dict  {裸码: 核查结果}
  summarize(res)                            → (级别, 文案)  级别 ∈ block/warn/ok/unknown
"""
import datetime
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# ============================================================
# 网络层 —— 复用终端 net.py (先直连, 失败才回退系统代理)
# ============================================================
try:
    from quant_terminal import net as _net
except Exception:
    try:
        import net as _net
    except Exception:
        _net = None

_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

# ⚠ 换源只需改这一行 (实测 2026-11-06: 10/10 成功, 单只 0.1s, 100 只并发 16 = 0.84s)
ANN_URL = ('https://np-anotice-stock.eastmoney.com/api/security/ann?'
           'sr=-1&page_size=%d&page_index=1&ann_type=A&client_source=web&stock_list=%s')

_DEBUG = bool(os.environ.get('AUCTION_ANN_DEBUG'))


def _log(*a):
    if _DEBUG:
        print('[ann]', *a)


def _get_json(url, referer, timeout=8):
    """GET → (ok, (status, text))  —— HTTPError 不算回退(由 net.py 保证)"""
    req = urllib.request.Request(url, headers={
        'User-Agent': _UA,
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': referer,
        'Host': urllib.parse.urlparse(url).netloc,
    })
    try:
        if _net is not None:
            r = _net.open_url(req, timeout=timeout)
        else:
            r = urllib.request.urlopen(req, timeout=timeout)
        with r:
            return True, r.read().decode('utf-8', 'ignore')
    except urllib.error.HTTPError as e:
        return False, 'HTTP%s' % e.code
    except Exception as e:
        return False, '%s: %s' % (type(e).__name__, str(e)[:80])


def fetch_anns(code, size=20, timeout=8):
    """取单只股票的公告列表 → (ok, list[dict] | 错误字符串)

    每条 dict 的键: date(YYYY-MM-DD) / title / cols(分类名列表) /
                    art(str, 用于拼详情链接) / time(完整时间戳)
    """
    c = re.sub(r'\D', '', code or '')
    if len(c) != 6:
        return False, 'bad_code'
    url = ANN_URL % (size, c)
    ok, txt = _get_json(url, 'https://data.eastmoney.com/notices/stock/%s.html' % c, timeout)
    if not ok:
        return False, txt
    try:
        j = json.loads(txt)
    except Exception as e:
        return False, 'json: %s' % str(e)[:60]
    data = j.get('data') or {}
    lst = data.get('list') or []
    out = []
    for it in lst:
        nd = it.get('notice_date') or it.get('display_time') or ''
        cols = []
        for col in (it.get('columns') or []):
            nm = col.get('column_name') if isinstance(col, dict) else None
            if nm:
                cols.append(nm)
        out.append({
            'date': (nd or '')[:10],
            'time': nd or '',
            'title': (it.get('title') or it.get('title_ch') or '').strip(),
            'cols': cols,
            'art': it.get('art_code') or '',
        })
    return True, out


# ============================================================
# 硬红线 / 软瑕疵 词表
# ------------------------------------------------------------
# ⚠ 词表顺序有意义: 先判硬红线, 命中即 block, 不再往下看软瑕疵。
#   每个词都配了「为什么是硬红线」, 来源于 F:\\竞价选股 v2.14 的 FORMULA_NOTE
#   与《开始阅读.html》第 3 节风控条款。
# ============================================================

# --- 硬红线: 一票否决 (block) ---
HARD = [
    ('减持',      '股东减持 —— 打板日砸盘主力'),
    ('清仓',      '股东清仓式减持'),
    ('预亏',      '业绩预亏 —— 主营不赚钱'),
    ('首亏',      '首次亏损'),
    ('续亏',      '连续亏损'),
    ('立案',      '被立案调查 —— 退市风险'),
    ('调查',      '被监管调查'),
    ('处罚',      '被行政处罚'),
    ('警示函',    '收到警示函'),
    ('问询函',    '收到问询函 / 关注函'),
    ('关注函',    '收到关注函'),
    ('退市',      '退市风险警示'),
    ('暂停上市',  '暂停上市'),
    ('终止上市',  '终止上市'),
    ('停牌',      '停牌 —— 买不进'),
    ('司法冻结',  '股份被冻结'),
    ('违规担保',  '违规担保'),
    ('财务造假',  '财务造假'),
    ('非标意见',  '审计非标意见'),
    ('无法表示意见', '审计无法表示意见'),
    ('被出具保留意见', '审计保留意见'),
]

# --- 需要「同时包含」两个词的规则 (kw2) ---
# ★ 2026-11-06 自测抓到的严重误报, 必须记下来:
#   「资金占用」单独作硬红线会 100% 误报 —— A 股每家公司随半年报/年报都会发
#   《非经营性资金占用及其他关联资金往来情况汇总表》, 这是**常规披露文件**,
#   不是违规。实测 002081/002029/000858 三只全部命中, 全是这种。
#   ⇒ 真正的违规是「违规占用 / 被占用 / 占用资金未归还」, 必须双词同现才判。
HARD_PAIR = [
    (('资金占用', '违规'),   '违规资金占用'),
    (('资金占用', '未归还'), '资金占用未归还'),
    (('资金占用', '被占用'), '资金被占用'),
    (('减持', '被动'),       '被动减持 —— 通常是质押爆仓'),
    (('立案', '调查'),       '被立案调查'),
    (('业绩预告', '亏损'),   '业绩预告亏损'),
    (('业绩预告', '下降'),   '业绩预告下滑'),
]

# --- 软瑕疵: 只标 ⚠️ 不否决 (warn) ---
# ⚠ 2026-11-06 修正: 「股票交易异常波动」原放 HARD, 自测发现它会毙掉 002029 七匹狼
#   —— 而原策略 11-03 的备选正是七匹狼。核对 v2.14 原文, 异动澄清属于
#   「软瑕疵只标 ⚠️」那一档, 不是硬红线。故下移到 SOFT。
#   另: 质押/解禁/问询函/关注函/警示函 也都不在 v2.14 的六条硬红线里 → 一律 SOFT。
SOFT = [
    ('股票交易异常波动', '股价异动 —— 原策略归软瑕疵, 但说明已被交易所关注'),
    ('异常波动',    '股价异动 —— 原策略归软瑕疵'),
    ('质押',        '股权质押 —— 爆仓风险, 需看比例'),
    ('解禁',        '限售解禁 —— 抛压, 需看占比'),
    ('问询函',      '收到问询函 —— 需看是否涉及财务'),
    ('关注函',      '收到关注函 —— 需看是否涉及财务'),
    ('警示函',      '收到警示函 —— 需看事由'),
    ('监管函',      '收到监管函'),
    ('减资',        '子公司/注册资本减资 —— 需看是否影响主营'),
    ('股权结构拟发生变动', '控股股东股权结构变动 —— 需看是否导致控制权变更'),
    ('业绩预减',    '业绩预减 —— 尚未亏损'),
    ('业绩下滑',    '业绩下滑 —— 尚未亏损'),
    ('计提减值',    '计提减值 —— 尚未亏损, 需看金额占比'),
    ('商誉减值',    '商誉减值 —— 需看是否已消化'),
    ('辞职',        '高管辞职 —— 中性偏负, 看是否接连离职'),
    ('退休离任',    '董事长退休 —— 中性'),
    ('澄清',        '澄清公告 —— 说明已被关注'),
    ('定向增发',    '定增 —— 摊薄, 需看价格'),
    ('可转债',      '可转债 —— 转股摊薄'),
    ('回购',        '回购 —— 偏正面'),
    ('增持',        '增持 —— 偏正面'),
]

# --- 观察期红线: 30~90 天内的减持/立案 —— 不否决但必须标出来 ---
# 依据: 减持计划通常 3~6 个月内执行, 30 天窗口会漏。原策略 11-05 主推的 000506
#   招金黄金 就有一条 2026-09-03 的「股东拟减持不超过 1%」(距今 64 天) ——
#   原作者没把它当否决项 (仍然选了它), 所以我们也不能否决, 只能标 warn。
WATCH_DAYS = 90

# --- 次新股判定: 上市不足 N 个自然日 (v2.14 原文「上市 <60 日」) ---
NEW_STOCK_DAYS = 60


def _parse_date(s):
    try:
        return datetime.date(*[int(x) for x in s[:10].split('-')])
    except Exception:
        return None


def _match(title, table):
    """在标题里找词表命中项 → (词, 说明) | None"""
    for kw, why in table:
        if kw in title:
            return kw, why
    return None


def _match_pair(title, table):
    """双词同现规则 → (说明) | None

    为什么需要: 单词匹配在 A 股公告标题上误报率极高 —— 比如「资金占用」
    单独出现 100% 是半年报/年报的常规汇总表, 不是违规。必须两个词同时出现
    才算真命中。
    """
    for kws, why in table:
        if all(k in title for k in kws):
            return '%s+%s' % (kws[0], kws[1]), why
    return None


def check_one(code, days=30, size=25, today=None):
    """单票公告核查

    返回 dict:
      code      裸码
      ok        True=抓到了 / False=抓失败 (fail-open, 见下)
      err       抓失败原因
      level     block / warn / ok / unknown
      hits      [{date,title,kw,why,hard}]  命中的条目
      recent    最近 N 天公告条数
      latest    最新一条的日期+标题 (界面展示用)
      note      一句话结论 (中文, 直接给用户看)

    ★ 失败策略 = fail-open (标 unknown, 不否决)
      理由: 9:25~9:30 只有 5 分钟, 网络抖一下就把整只票毙掉会让当天直接空仓。
      正确做法是「标红提示 + 让人工兜底」, 而不是让程序替人下否决。
    """
    today = today or datetime.date.today()
    ok, res = fetch_anns(code, size=size)
    if not ok:
        return {'code': re.sub(r'\D', '', code), 'ok': False, 'err': res,
                'level': 'unknown', 'hits': [], 'recent': 0,
                'latest': '', 'note': '公告抓取失败(%s) · 请人工确认' % res}

    cutoff = today - datetime.timedelta(days=days)
    watch_cut = today - datetime.timedelta(days=WATCH_DAYS)
    hits, recent, old_hard = [], 0, []
    for a in res:
        d = _parse_date(a['date'])
        if d is None:
            continue
        if d < watch_cut:
            continue                       # 三个月以前的不看了
        t = a['title']
        in_window = d >= cutoff
        if in_window:
            recent += 1
        # ① 双词同现规则优先 (精度最高, 防「资金占用」这类常规披露误报)
        h = _match_pair(t, HARD_PAIR) or _match(t, HARD)
        if h:
            if in_window:
                hits.append({'date': a['date'], 'title': t, 'kw': h[0],
                             'why': h[1], 'hard': True})
            else:
                # 30 天窗口外的硬红线 → 不否决, 但要标出来 (见 WATCH_DAYS 注释)
                old_hard.append({'date': a['date'], 'title': t, 'kw': h[0],
                                 'why': h[1], 'hard': False})
            continue
        if in_window:
            s = _match(t, SOFT)
            if s:
                hits.append({'date': a['date'], 'title': t, 'kw': s[0],
                             'why': s[1], 'hard': False})

    hard_hits = [x for x in hits if x['hard']]
    soft_hits = [x for x in hits if not x['hard']]

    if hard_hits:
        level = 'block'
        note = '⛔ %s' % hard_hits[0]['why']
        if len(hard_hits) > 1:
            note += ' (另有%d条硬红线)' % (len(hard_hits) - 1)
    elif old_hard:
        # 近 90 天有硬红线但已过 30 天窗口 —— 原策略仍会选 (000506 就是这种情况)
        level = 'warn'
        note = '⚠️ %s %s(已过%d天, 原策略不否决)' % (
            old_hard[0]['date'][5:], old_hard[0]['why'], days)
    elif soft_hits:
        level = 'warn'
        note = '⚠️ %s' % soft_hits[0]['why']
        if len(soft_hits) > 1:
            note += ' (另有%d条)' % (len(soft_hits) - 1)
    else:
        level = 'ok'
        note = '无硬红线' if recent else ('近%d天无公告' % days)

    latest = ''
    if res:
        latest = '%s %s' % (res[0]['date'], res[0]['title'])

    return {'code': re.sub(r'\D', '', code), 'ok': True, 'err': '',
            'level': level, 'hits': hits[:8], 'old_hard': old_hard[:4],
            'recent': recent, 'latest': latest, 'note': note}


# ============================================================
# 批量并发 (竞价打板真实工作量: 主推+备选+观察共约 20~30 只)
# ============================================================
def check_batch(codes, days=30, workers=12, size=25):
    """并发核查 → {裸码: check_one 结果}

    ⚠ 数量上限: 竞价打板只需要核查**最终候选** (通常 <40 只)。
      不要把 2334 只全量送进来 —— 那会把别人的接口打爆, 也会拖慢 9:25 的 5 分钟窗口。
    """
    codes = [re.sub(r'\D', '', c) for c in (codes or [])]
    codes = [c for c in codes if len(c) == 6]
    if not codes:
        return {}
    out = {}
    lock = threading.Lock()

    def _one(c):
        try:
            r = check_one(c, days=days, size=size)
        except Exception as e:
            r = {'code': c, 'ok': False, 'err': str(e)[:60], 'level': 'unknown',
                 'hits': [], 'recent': 0, 'latest': '',
                 'note': '核查异常(%s) · 请人工确认' % str(e)[:40]}
        with lock:
            out[c] = r

    try:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(codes)))) as ex:
            list(ex.map(_one, codes))
    except Exception:
        for c in codes:                       # 并发不可用就退串行, 保证有结果
            _one(c)
    return out


def summarize(res):
    """核查结果 → (级别, 文案) —— 级别 ∈ block / warn / ok / unknown"""
    if not res:
        return 'unknown', '未核查'
    lv = res.get('level') or 'unknown'
    return lv, res.get('note') or ''


# ============================================================
# 自测 (python auction_ann.py)
# ============================================================
if __name__ == '__main__':
    import sys
    print('=' * 74)
    print('竞价打板 · 公告核查自测   源 = %s' % urllib.parse.urlparse(ANN_URL).netloc)
    print('=' * 74)

    tests = ['000506', '600519', '002081', '002029', '601857', '000858']
    t0 = time.time()
    bad = check_batch(tests, workers=6)
    print('\n单批 6 只耗时 %.2f 秒\n' % (time.time() - t0))
    for c in tests:
        r = bad.get(c) or {}
        print('  %-8s %-8s %s' % (c, r.get('level'), r.get('note')))
        for h in (r.get('hits') or [])[:3]:
            print('           %s %s %s' % (h['date'],
                                           '⛔' if h['hard'] else '⚠️',
                                           h['title'][:44]))

    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        code = sys.argv[1]
        print('\n%s 完整公告列表:' % code)
        ok, lst = fetch_anns(code, size=15)
        if ok:
            for a in lst:
                print('  %s  %-18s %s' % (a['date'], '/'.join(a['cols'])[:18],
                                          a['title'][:50]))
        else:
            print('  抓取失败:', lst)
