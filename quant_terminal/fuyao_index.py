# -*- coding: utf-8 -*-
"""同花顺 fuyao 板块指数目录 + 成分股（终端 8799 · 替换清单 T1/T2/T3 的数据底座）

口径（2026-10-22 真实调用确认，不是文档抄的）:
    tag=industry / 320 个 = 881 上级 90 + 884 细分 230
        881 上级**互斥**：90 个去重覆盖 5578 只，每只票恰好属 1 个上级行业
        881 成分 = 其下所有 884 细分的并集（881101 种植业与林业 29 = 10+2+9+5+3，交集全中）
    tag=cn_concept / 390 个 = 885 传统概念 293 + 886 新题材 97（多对多，一只票可挂十几个）
    tag=tszs / 105 个 = 883 同花顺特色统计指数（涨停家数、昨日涨停…，不是真实板块）
    tag=region / 33 个 = 882 地域
    成分股接口只回 3 列：thscode / ticker / name（无权重、无市值）

为什么不直接覆盖 industry_map.csv / concept_map.csv:
    datasource.py:119 那条「map 铁律」—— industry_map / concept_map 是 🔒 只读，
    唯一写入入口是 import_ths_snapshot.py。本模块只往 kline_data/ths_index/ 落**新文件**，
    与旧 map 并存；等对照报告确认无误，再由人决定是否切换读取路径。

落盘:
    kline_data/ths_index/catalog.json     4 个 tag 全部指数（722 个）
    kline_data/ths_index/members.json     {thscode: [6位码...]}
    kline_data/ths_index/meta.json        抓取时间/数量/失败项
    kline_data/ths_index/ths_l1.csv       代码,上级行业        ← 881（互斥）
    kline_data/ths_index/ths_l2.csv       代码,细分行业        ← 884
    kline_data/ths_index/ths_concept.csv  代码,概念(a|b|c)     ← 885+886
    kline_data/ths_index/report.txt       与旧 map 的差异对照

CLI:
    python -m quant_terminal.fuyao_index --dump       # 拉全量目录+成分股（722 次请求，约 1-2 分钟）
    python -m quant_terminal.fuyao_index --build      # 由 members.json 派生 3 个 csv
    python -m quant_terminal.fuyao_index --report     # 出对照报告
    python -m quant_terminal.fuyao_index --all        # 三件一起做
"""
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

try:
    from . import fuyao as _fq
except ImportError:                                    # 直接跑脚本（非包内）时
    import fuyao as _fq

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(HERE)                       # …\终端8799
OUT_DIR = os.path.join(BASE_DIR, 'kline_data', 'ths_index')

CATALOG_API = '/api/a-share-index/catalog/ths-index-list'
CONS_API = '/api/a-share-index/constituents/ths-stock-list'

TAGS = ('industry', 'cn_concept', 'tszs', 'region')
TAG_CN = {'industry': '行业', 'cn_concept': '概念', 'tszs': '特色统计', 'region': '地域'}

# 881 上级 / 884 细分 / 885+886 概念 —— 前缀是稳定约定，比 tag 更精确（同 tag 混着两级）
P_L1, P_L2 = '881', '884'
P_CON = ('885', '886')

WORKERS = 6          # 并发。官方口径「不限累计次数，429/4001 才算限流」；6 路 848 指数约 30-40 秒
RETRY = 3
RETRY_SLEEP = 2.0


# ---------------------------------------------------------------- 抓取
def catalog(tag):
    """单个 tag 的全部指数 [{thscode, name}]（无分页，一次全量）"""
    d = _fq._req(CATALOG_API, {'tag': tag}, api_name='catalog:%s' % tag)
    return list(d.get('item') or [])


def constituents(thscode):
    """单只指数的成分股 [{thscode,ticker,name}]"""
    d = _fq._req(CONS_API, {'thscode': thscode}, api_name='cons:%s' % thscode)
    return list(d.get('item') or [])


def _cons_with_retry(code):
    """带限流退避的成分股抓取；彻底失败返回 (code, None, err)"""
    last = ''
    for i in range(RETRY):
        try:
            return code, constituents(code), ''
        except Exception as e:
            last = str(e)[:120]
            if i < RETRY - 1:
                time.sleep(RETRY_SLEEP * (i + 1))
    return code, None, last


def dump(workers=WORKERS, verbose=True):
    """拉全部目录 + 全部成分股，落 catalog.json / members.json / meta.json"""
    os.makedirs(OUT_DIR, exist_ok=True)
    t0 = time.time()
    cat = {}
    for tag in TAGS:
        it = catalog(tag)
        cat[tag] = it
        if verbose:
            print('  目录 %-10s %4d 个' % (tag, len(it)))
    allidx = [x for tag in TAGS for x in cat[tag]]
    if verbose:
        print('  合计指数 %d 个，开始抓成分股（%d 并发）…' % (len(allidx), workers))

    members, fails = {}, {}
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_cons_with_retry, x['thscode']): x['thscode'] for x in allidx}
        for fu in as_completed(futs):
            code, rows, err = fu.result()
            done += 1
            if rows is None:
                fails[code] = err
            else:
                members[code] = [str(r.get('ticker') or '').strip()
                                 for r in rows if str(r.get('ticker') or '').strip()]
            if verbose and done % 100 == 0:
                print('    %d/%d …失败 %d' % (done, len(allidx), len(fails)))

    meta = {
        'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'elapsed_s': round(time.time() - t0, 1),
        'counts': {t: len(cat[t]) for t in TAGS},
        'index_total': len(allidx),
        'members_ok': len(members),
        'members_fail': fails,
        'stocks_union': len({c for v in members.values() for c in v}),
    }
    _write_json(os.path.join(OUT_DIR, 'catalog.json'),
                {'counts': meta['counts'],
                 'items': {t: cat[t] for t in TAGS}})
    _write_json(os.path.join(OUT_DIR, 'members.json'), members)
    _write_json(os.path.join(OUT_DIR, 'meta.json'), meta)
    if verbose:
        print('  完成 %.1fs：指数 %d，成分股成功 %d，失败 %d，去重股票 %d'
              % (meta['elapsed_s'], meta['index_total'], meta['members_ok'],
                 len(fails), meta['stocks_union']))
    return meta


# ---------------------------------------------------------------- 派生 csv
def _load_json(p, default=None):
    try:
        with open(p, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(p, obj):
    tmp = p + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, p)


def _write_csv(p, header, rows):
    """项目铁律：纯 LF + utf-8-sig（读者都用 utf-8-sig 打开）"""
    tmp = p + '.tmp'
    with open(tmp, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f, lineterminator='\n')
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, p)
    return len(rows)


def build(verbose=True):
    """members.json → ths_l1.csv（上级）/ ths_l2.csv（细分）/ ths_concept.csv"""
    cat = _load_json(os.path.join(OUT_DIR, 'catalog.json'), {}) or {}
    members = _load_json(os.path.join(OUT_DIR, 'members.json'), {}) or {}
    items = cat.get('items') or {}
    if not items or not members:
        raise SystemExit('缺 catalog.json / members.json，先跑 --dump')

    # 指数代码 → 名称
    name_of = {}
    for tag in TAGS:
        for x in items.get(tag) or []:
            name_of[str(x.get('thscode') or '')] = str(x.get('name') or '').strip()

    l1, l2 = {}, {}
    con = {}
    multi_l2 = []
    for code, codes in members.items():
        nm = name_of.get(code) or ''
        if not nm:
            continue
        pre = code[:3]
        if pre == P_L1:
            for c in codes:
                if c in l1 and l1[c] != nm:
                    print('  ⚠ 上级行业冲突 %s: %s / %s' % (c, l1[c], nm))
                l1[c] = nm
        elif pre == P_L2:
            for c in codes:
                if c in l2 and l2[c] != nm:
                    multi_l2.append((c, l2[c], nm))
                    l2[c] = l2[c] + '|' + nm if nm not in l2[c].split('|') else l2[c]
                else:
                    l2[c] = nm
        elif pre in P_CON:
            for c in codes:
                con.setdefault(c, set()).add(nm)

    rows1 = sorted((c, v) for c, v in l1.items())
    rows2 = sorted((c, v) for c, v in l2.items())
    rows3 = sorted((c, '|'.join(sorted(v))) for c, v in con.items())

    n1 = _write_csv(os.path.join(OUT_DIR, 'ths_l1.csv'), ['代码', '上级行业'], rows1)
    n2 = _write_csv(os.path.join(OUT_DIR, 'ths_l2.csv'), ['代码', '细分行业'], rows2)
    n3 = _write_csv(os.path.join(OUT_DIR, 'ths_concept.csv'), ['代码', '概念'], rows3)

    # 校验：上级行业必须互斥
    conflict = len(l1) - len({c for c in l1})
    if verbose:
        print('  上级行业 %d 只（%d 个行业）' % (n1, len({v for _, v in rows1})))
        print('  细分行业 %d 只（%d 个行业）%s'
              % (n2, len({v.split("|")[0] for _, v in rows2}),
                 '，%d 只跨细分' % len(multi_l2) if multi_l2 else ''))
        print('  概念     %d 只（%d 个概念）' % (n3, len({x for v in con.values() for x in v})))
        if conflict:
            print('  ⚠ 上级行业有覆盖冲突')
    return {'l1': n1, 'l2': n2, 'con': n3, 'multi_l2': len(multi_l2)}


# ---------------------------------------------------------------- 对照报告
def _read_map_csv(p, key_col, val_col):
    out = {}
    if not os.path.exists(p):
        return out
    with open(p, encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            k = (row.get(key_col) or '').strip()
            v = (row.get(val_col) or '').strip()
            if k and v:
                out[k] = v
    return out


def _bare(c):
    """统一成 6 位裸码：BZ920000 / SH600519 / 600519.SH → 600519 / 920000
    （旧 industry_map.csv 里北交所写成 BZ920000，fuyao 是裸码 920000，不归一化会误判成 353 只差异）"""
    c = str(c or '').strip().upper()
    if c.endswith('.TI'):
        return c
    for sep in ('.',):
        if sep in c:
            c = c.split(sep)[0]
    if len(c) > 6 and c[-6:].isdigit():
        return c[-6:]
    if c[:3] in ('BJS',):
        c = c[3:]
    if c[:2].isalpha():
        c = c[2:]
    return c


def report(verbose=True):
    """新数据 vs 旧 map 的差异（只读旧文件，一个字都不改）"""
    L = []
    def P(s=''):
        L.append(s)

    base = os.path.join(BASE_DIR, 'kline_data')
    old_ind = {_bare(k): v for k, v in
               _read_map_csv(os.path.join(base, 'industry_map.csv'), '代码', '行业').items()}
    old_par = _read_map_csv(os.path.join(base, 'industry_parent.csv'), '细分行业', '上级行业')
    old_con_raw = {_bare(k): v for k, v in
                   _read_map_csv(os.path.join(base, 'concept_map.csv'), '代码', '概念').items()}

    new_l1 = {_bare(k): v for k, v in
              _read_map_csv(os.path.join(OUT_DIR, 'ths_l1.csv'), '代码', '上级行业').items()}
    new_l2 = {_bare(k): v for k, v in
              _read_map_csv(os.path.join(OUT_DIR, 'ths_l2.csv'), '代码', '细分行业').items()}
    new_con_raw = {_bare(k): v for k, v in
                   _read_map_csv(os.path.join(OUT_DIR, 'ths_concept.csv'), '代码', '概念').items()}

    old_con = {}
    for c, s in old_con_raw.items():
        old_con[c] = {x.strip() for x in s.split('|') if x.strip()}
    new_con = {}
    for c, s in new_con_raw.items():
        new_con[c] = {x.strip() for x in s.split('|') if x.strip()}

    P('# 终端 8799 · 同花顺 fuyao 板块数据 对照报告')
    P('生成时间：%s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    P()

    P('## 1. 规模')
    P()
    P('| 项目 | 旧（本地 csv） | 新（fuyao） |')
    P('|---|---|---|')
    P('| 上级行业个数 | %d | %d |' % (len(set(old_par.values())), len(set(new_l1.values()))))
    P('| 细分行业个数 | %d | %d |' % (len(set(old_ind.values())),
                                     len({v.split("|")[0] for v in new_l2.values()})))
    P('| 概念个数 | %d | %d |' % (len({x for s in old_con.values() for x in s}),
                                  len({x for s in new_con.values() for x in s})))
    P('| 有行业归属的股票 | %d | %d |' % (len(old_ind), len(new_l1)))
    P('| 有概念归属的股票 | %d | %d |' % (len(old_con), len(new_con)))
    P()

    # ---- 上级行业名单差异
    old_l1_names = set(old_par.values())
    new_l1_names = set(new_l1.values())
    P('## 2. 上级行业名单差异（90 类口径）')
    P()
    add = sorted(new_l1_names - old_l1_names)
    gone = sorted(old_l1_names - new_l1_names)
    P('- 相同：%d 个' % len(old_l1_names & new_l1_names))
    P('- **fuyao 新增**：%d 个 %s' % (len(add), '、'.join(add) if add else '（无）'))
    P('- **fuyao 没有**：%d 个 %s' % (len(gone), '、'.join(gone) if gone else '（无）'))
    P()

    # ---- 个股上级行业归属差异
    old_l1_of = {c: old_par.get(s) for c, s in old_ind.items() if old_par.get(s)}
    diff = [(c, old_l1_of.get(c), new_l1.get(c)) for c in sorted(set(old_l1_of) | set(new_l1))
            if old_l1_of.get(c) != new_l1.get(c)]
    P('## 3. 个股「上级行业」归属差异')
    P()
    P('- 旧表可比对 %d 只，新表 %d 只' % (len(old_l1_of), len(new_l1)))
    P('- **归属不同/缺失：%d 只**' % len(diff))
    if diff:
        P()
        P('| 代码 | 旧上级 | 新上级 |')
        P('|---|---|---|')
        for c, a, b in diff[:40]:
            P('| %s | %s | %s |' % (c, a or '—', b or '—'))
        if len(diff) > 40:
            P('| … | 共 %d 只，只列前 40 | |' % len(diff))
    P()

    # ---- 细分行业名单差异
    old_sub_names = set(old_ind.values())
    new_sub_names = {v.split('|')[0] for v in new_l2.values()}
    add2 = sorted(new_sub_names - old_sub_names)
    gone2 = sorted(old_sub_names - new_sub_names)
    P('## 4. 细分行业名单差异')
    P()
    P('- 相同：%d 个' % len(old_sub_names & new_sub_names))
    P('- fuyao 新增：%d 个 %s' % (len(add2), '、'.join(add2[:30]) + ('…' if len(add2) > 30 else '')))
    P('- fuyao 没有：%d 个 %s' % (len(gone2), '、'.join(gone2[:30]) + ('…' if len(gone2) > 30 else '')))
    P()
    # 「fuyao 没有的细分名」几乎都是同花顺不分细分的 881 上级行业（本地把它们平铺成细分）
    l1_names = set(new_l1.values())
    as_l1 = [n for n in gone2 if n in l1_names]
    really_gone = [n for n in gone2 if n not in l1_names]
    if as_l1:
        P('> **这 %d 个不是「fuyao 缺了」，而是同花顺不给它们分细分** ——'
          '它们在 fuyao 里本来就是 881 上级行业，本级即末级（本地 industry_map.csv 把它们平铺成了细分）。'
          '所以本地「细分」字段是混合层级：%d 个真细分 + %d 个末级上级。'
          % (len(as_l1), len(new_sub_names), len(as_l1)))
        P()
        P('| 本地当细分用的名字 | 本地票数 |')
        P('|---|---|')
        _oc = {}
        for _v in old_ind.values():
            _oc[_v] = _oc.get(_v, 0) + 1
        for n in sorted(as_l1, key=lambda _n: -_oc.get(_n, 0)):
            P('| %s | %d |' % (n, _oc.get(n, 0)))
        P()
    if really_gone:
        P('- 真正 fuyao 没有的细分名：%s' % '、'.join(really_gone))
        P()

    # ---- 概念差异
    old_cn = {x for s in old_con.values() for x in s}
    new_cn = {x for s in new_con.values() for x in s}
    P('## 5. 概念差异')
    P()
    P('- 相同：%d 个' % len(old_cn & new_cn))
    P('- **fuyao 新增**：%d 个' % len(new_cn - old_cn))
    P('  %s' % ('、'.join(sorted(new_cn - old_cn)[:60]) + ('…' if len(new_cn - old_cn) > 60 else '')
                if new_cn - old_cn else '（无）'))
    P('- **fuyao 没有**：%d 个' % len(old_cn - new_cn))
    P('  %s' % ('、'.join(sorted(old_cn - new_cn)[:60]) + ('…' if len(old_cn - new_cn) > 60 else '')
                if old_cn - new_cn else '（无）'))
    P()
    # 每只票概念数对比
    P('- 每只票平均概念数：旧 %.1f / 新 %.1f'
      % (sum(len(s) for s in old_con.values()) / max(1, len(old_con)),
         sum(len(s) for s in new_con.values()) / max(1, len(new_con))))
    P()

    # ---- 全量名单（供人工核口径）
    P('## 6. 全量上级行业名单（fuyao，按成分股数排序）')
    P()
    cnt = {}
    for v in new_l1.values():
        cnt[v] = cnt.get(v, 0) + 1
    P('| # | 上级行业 | 成分股 |')
    P('|---|---|---|')
    for i, (n, k) in enumerate(sorted(cnt.items(), key=lambda kv: -kv[1]), 1):
        P('| %d | %s | %d |' % (i, n, k))
    P()
    P('## 7. 全量概念名单（fuyao，按成分股数排序，前 60）')
    P()
    ccnt = {}
    for s in new_con.values():
        for x in s:
            ccnt[x] = ccnt.get(x, 0) + 1
    P('| # | 概念 | 成分股 |')
    P('|---|---|---|')
    for i, (n, k) in enumerate(sorted(ccnt.items(), key=lambda kv: -kv[1])[:60], 1):
        P('| %d | %s | %d |' % (i, n, k))

    txt = '\n'.join(L) + '\n'
    p = os.path.join(OUT_DIR, 'report.txt')
    with open(p, 'w', encoding='utf-8') as f:
        f.write(txt)
    if verbose:
        print('  报告 → %s（%d 行）' % (p, len(L)))
    return p


# ---------------------------------------------------------------- 生成 map 候选（不覆盖现用文件）
MAP_DIR = os.path.join(OUT_DIR, 'map_new')


def _ext(c):
    """6 位裸码 → 项目现用写法：北交所加 BZ 前缀，其余裸码（与 industry_map.csv 一致）"""
    c = _bare(c)
    if not c:
        return c
    if c[:3] == '920' or c[0] in '48':
        return 'BZ' + c
    return c


def _write_map_csv(p, header, rows, bom=False):
    """map 文件沿用现用格式：**CRLF 行尾、无 BOM**（读它的代码用 utf-8-sig 打开，两种都能读）"""
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + '.tmp'
    with open(tmp, 'w', encoding='utf-8-sig' if bom else 'utf-8', newline='') as f:
        w = csv.writer(f, lineterminator='\r\n')
        w.writerow(header)
        w.writerows(rows)
    os.replace(tmp, p)
    return len(rows)


def _norm_con(s):
    """概念名归一化：去所有空白 + 大写（AI PC / AIPC、WiFi 6 / WiFi6 视为同名）"""
    return ''.join(str(s or '').split()).upper()


def build_maps(verbose=True):
    """由 members.json 生成 map 候选，落 MAP_DIR；**一个字节都不动现用文件**"""
    cat = _load_json(os.path.join(OUT_DIR, 'catalog.json'), {}) or {}
    members = _load_json(os.path.join(OUT_DIR, 'members.json'), {}) or {}
    items = cat.get('items') or {}
    if not items or not members:
        raise SystemExit('缺 catalog.json / members.json，先跑 --dump')

    name_of, tag_of = {}, {}
    for tag in TAGS:
        for x in items.get(tag) or []:
            _tc = str(x.get('thscode') or '')
            name_of[_tc] = str(x.get('name') or '').strip()
            tag_of[_tc] = tag

    l1_of, l2_of, con_of = {}, {}, {}
    l2_parent_votes = {}      # 884 名 → {上级名: 票数}
    for thscode, codes in members.items():
        nm = name_of.get(thscode) or ''
        if not nm:
            continue
        pre = str(thscode)[:3]
        if pre == P_L1:
            for c0 in codes:
                c = _bare(c0)
                if c:
                    l1_of[c] = nm
        elif pre == P_L2:
            for c0 in codes:
                c = _bare(c0)
                if c:
                    l2_of[c] = nm
                    v = l2_parent_votes.setdefault(nm, {})
                    # 上级名稍后由 l1_of 反查（此处先占位，下面统一算）
        elif pre in P_CON:
            for c0 in codes:
                c = _bare(c0)
                if c:
                    con_of.setdefault(c, set()).add(nm)

    # 884 名 → 上级：用「该 884 的成分股各自所属 881」投票（正常全票一致）
    for thscode, codes in members.items():
        pre = str(thscode)[:3]
        nm = name_of.get(thscode) or ''
        if pre != P_L2 or not nm:
            continue
        votes = {}
        for c0 in codes:
            up = l1_of.get(_bare(c0))
            if up:
                votes[up] = votes.get(up, 0) + 1
        l2_parent_votes[nm] = votes

    sub_parent = {}
    trouble = []
    for sub, votes in l2_parent_votes.items():
        if not votes:
            trouble.append((sub, '无上级票'))
            continue
        top = max(votes.items(), key=lambda kv: kv[1])
        if len(votes) > 1:
            trouble.append((sub, '跨上级:%s' % votes))
        sub_parent[sub] = top[0]

    # ---- 现用文件（只读）----
    base = os.path.join(BASE_DIR, 'kline_data')
    old_ind = {_bare(k): v for k, v in
               _read_map_csv(os.path.join(base, 'industry_map.csv'), '代码', '行业').items()}
    old_con_raw = {_bare(k): v for k, v in
                   _read_map_csv(os.path.join(base, 'concept_map.csv'), '代码', '概念').items()}
    old_con = {c: {x.strip() for x in s.split('|') if x.strip()} for c, s in old_con_raw.items()}
    old_con_names = {x for s in old_con.values() for x in s}
    old_by_norm = {}
    for n in old_con_names:
        old_by_norm.setdefault(_norm_con(n), n)

    # ---- 行业：有 884 用 884，没有就用 881（上级即末级，与终端现状一致）----
    new_ind = {}
    for c, up in l1_of.items():
        new_ind[c] = l2_of.get(c) or up
    inherited = []
    for c, v in old_ind.items():                       # fuyao 里没有的（停牌/退市）沿用旧值
        if c not in new_ind:
            new_ind[c] = v
            inherited.append(c)

    # ---- 概念 ----
    new_con = {}
    for c, s in con_of.items():
        new_con[c] = set(s)
    inherit_con = []
    for c, s in old_con.items():
        if c not in new_con:
            new_con[c] = set(s)
            inherit_con.append(c)
    # 写法归一化：去空格后与旧名相同的，沿用旧写法（AI PC / WiFi 6 / 中国AI 50）
    renamed = {}
    for c, s in new_con.items():
        fixed = set()
        for n in s:
            o = old_by_norm.get(_norm_con(n))
            if o and o != n:
                renamed[n] = o
                fixed.add(o)
            else:
                fixed.add(n)
        new_con[c] = fixed

    # ---- 写候选 ----
    rows_ind = sorted((_ext(c), v) for c, v in new_ind.items())
    n1 = _write_map_csv(os.path.join(MAP_DIR, 'industry_map.csv'), ['代码', '行业'], rows_ind)

    subs = {v.split('|')[0] for v in new_ind.values()}
    rows_par = sorted((s, sub_parent.get(s, s)) for s in subs)
    n2 = _write_map_csv(os.path.join(MAP_DIR, 'industry_parent.csv'), ['细分行业', '上级行业'], rows_par)

    rows_con = sorted((_ext(c), '|'.join(sorted(v))) for c, v in new_con.items() if v)
    n3 = _write_map_csv(os.path.join(MAP_DIR, 'concept_map.csv'), ['代码', '概念'], rows_con)

    info = {
        'industry_map': n1, 'industry_parent': n2, 'concept_map': n3,
        'l1_codes': len(l1_of), 'l2_codes': len(l2_of), 'con_codes': len(con_of),
        'sub_names': len(subs),
        'inherited_ind': inherited, 'inherit_con': inherit_con,
        'renamed_con': renamed, 'trouble': trouble,
    }
    if verbose:
        print('  候选 → %s' % MAP_DIR)
        print('    industry_map.csv   %d 行（沿用旧值 %d 只）' % (n1, len(inherited)))
        print('    industry_parent.csv %d 行' % n2)
        print('    concept_map.csv    %d 行（沿用旧值 %d 只）' % (n3, len(inherit_con)))
        if renamed:
            print('    概念写法沿用旧名：%s' % ', '.join('%s→%s' % (k, v) for k, v in renamed.items()))
        if trouble:
            print('    ⚠ 细分→上级 异常 %d 个：%s' % (len(trouble), trouble[:5]))
    return info


def diff_maps(verbose=True):
    """候选 vs 现用，逐行差异（只读两边，不写任何 map 文件）"""
    base = os.path.join(BASE_DIR, 'kline_data')
    oi = {_bare(k): v for k, v in _read_map_csv(os.path.join(base, 'industry_map.csv'), '代码', '行业').items()}
    op = _read_map_csv(os.path.join(base, 'industry_parent.csv'), '细分行业', '上级行业')
    oc_raw = {_bare(k): v for k, v in _read_map_csv(os.path.join(base, 'concept_map.csv'), '代码', '概念').items()}
    ni = {_bare(k): v for k, v in _read_map_csv(os.path.join(MAP_DIR, 'industry_map.csv'), '代码', '行业').items()}
    np_ = _read_map_csv(os.path.join(MAP_DIR, 'industry_parent.csv'), '细分行业', '上级行业')
    nc_raw = {_bare(k): v for k, v in _read_map_csv(os.path.join(MAP_DIR, 'concept_map.csv'), '代码', '概念').items()}

    oc = {c: {x.strip() for x in s.split('|') if x.strip()} for c, s in oc_raw.items()}
    nc = {c: {x.strip() for x in s.split('|') if x.strip()} for c, s in nc_raw.items()}

    L = []
    def P(s=''):
        L.append(s)
    P('# 板块 map 候选 vs 现用 · 逐行差异')
    P('生成时间：%s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    P('候选目录：`%s`　现用目录：`kline_data`' % MAP_DIR)
    P()
    P('## 0. 文件规模')
    P()
    P('| 文件 | 现用 | 候选 |')
    P('|---|---|---|')
    P('| industry_map.csv（代码,行业） | %d 行 | %d 行 |' % (len(oi), len(ni)))
    P('| industry_parent.csv（细分,上级） | %d 行 | %d 行 |' % (len(op), len(np_)))
    P('| concept_map.csv（代码,概念） | %d 行 | %d 行 |' % (len(oc), len(nc)))
    P()

    # 行业归属
    changed = [(c, oi.get(c), ni.get(c)) for c in sorted(set(oi) | set(ni)) if oi.get(c) != ni.get(c)]
    P('## 1. industry_map.csv · 行业归属变化')
    P()
    P('- 两边都有：%d 只　仅现用：%d 只　仅候选：%d 只' %
      (len(set(oi) & set(ni)), len(set(oi) - set(ni)), len(set(ni) - set(oi))))
    P('- **归属字段变化：%d 只**' % len(changed))
    if changed:
        P()
        P('| 代码 | 现用 | 候选 |')
        P('|---|---|---|')
        for c, a, b in changed:
            P('| %s | %s | %s |' % (c, a or '—', b or '—'))
    P()

    # 细分名集合
    osubs, nsubs = set(op), set(np_)
    P('## 2. industry_parent.csv · 细分行业名单变化')
    P()
    P('- 相同 %d 个　新增 %d 个　消失 %d 个' % (len(osubs & nsubs), len(nsubs - osubs), len(osubs - nsubs)))
    if nsubs - osubs:
        P('- 新增：%s' % '、'.join(sorted(nsubs - osubs)))
    if osubs - nsubs:
        P('- 消失：%s' % '、'.join(sorted(osubs - nsubs)))
    pmap_diff = [(s, op.get(s), np_.get(s)) for s in sorted(osubs & nsubs) if op.get(s) != np_.get(s)]
    P('- 细分→上级 映射变化：%d 个%s' % (len(pmap_diff),
                                  ('' if not pmap_diff else '（%s）' % '、'.join('%s:%s→%s' % t for t in pmap_diff[:10]))))
    selfmap = sum(1 for s, u in np_.items() if s == u)
    P('- 末级行业（细分=上级，终端现状保留双份）：**%d 个**' % selfmap)
    P()

    # 概念
    same_set = sum(1 for c in set(oc) & set(nc) if oc[c] == nc[c])
    diff_set = [c for c in sorted(set(oc) & set(nc)) if oc[c] != nc[c]]
    P('## 3. concept_map.csv · 概念变化')
    P()
    P('- 现用 %d 只 / 候选 %d 只；逐票概念集合完全相同 **%d / %d**' %
      (len(oc), len(nc), same_set, len(set(oc) & set(nc))))
    P('- 概念名字典：现用 %d 个 / 候选 %d 个' %
      (len({x for s in oc.values() for x in s}), len({x for s in nc.values() for x in s})))
    all_o = {x for s in oc.values() for x in s}
    all_n = {x for s in nc.values() for x in s}
    P('- 新增概念 %d 个：%s' % (len(all_n - all_o), '、'.join(sorted(all_n - all_o)) or '（无）'))
    P('- 消失概念 %d 个：%s' % (len(all_o - all_n), '、'.join(sorted(all_o - all_n)) or '（无）'))
    if diff_set:
        P()
        P('- 概念集合有变化的票：%d 只，前 15 只：' % len(diff_set))
        for c in diff_set[:15]:
            add = nc[c] - oc[c]
            rm = oc[c] - nc[c]
            P('  - %s　+%s　-%s' % (c, sorted(add) or '无', sorted(rm) or '无'))
    P()

    P('## 4. 结论')
    P()
    ok = (not (set(oi) - set(ni))) and (not (set(ni) - set(oi))) and len(changed) <= 3
    P('- 覆盖率：现用 %d 只 / 候选 %d 只，%s' %
      (len(oi), len(ni), '无增减' if set(oi) == set(ni) else '有增减（见上）'))
    P('- 行业归属实质变化 %d 只，细分名单变化 %d 个，概念实质变化 %d 只' %
      (len(changed), len(nsubs - osubs) + len(osubs - nsubs), len(diff_set)))
    P('- 判定：**%s**' % ('可以直接替换（差异仅为真实数据变更）' if ok else '先看上面逐条差异再决定'))
    P()

    txt = '\n'.join(L) + '\n'
    p = os.path.join(OUT_DIR, 'map_diff.txt')
    with open(p, 'w', encoding='utf-8') as f:
        f.write(txt)
    if verbose:
        print('  差异报告 → %s（%d 行）' % (p, len(L)))
    return p


# ---------------------------------------------------------------- 每日增量修正 (v2.172)
# 用户 m01560②：「成分股这个可以做到终端里面，每天更新板块之前先修正一下成分股。
#   我的想法是：拉取一下成分股数据到缓存，然后和本地文件的成分股进行对比，
#   将差异部分改写，没有变化的不动。」
SYNC_STATE = os.path.join(OUT_DIR, 'map_sync_state.json')
SYNC_LOG = os.path.join(OUT_DIR, 'map_sync.log')


def _sync_log(level, msg):
    """增量修正日志（>2MB 截半），只在有变化时写"""
    try:
        with open(SYNC_LOG, 'a', encoding='utf-8') as f:
            f.write('%s [%s] %s\n' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), level, msg))
        try:
            if os.path.getsize(SYNC_LOG) > 2 * 1024 * 1024:
                with open(SYNC_LOG, encoding='utf-8', errors='replace') as f:
                    tail = f.read()[-1024 * 1024:]
                with open(SYNC_LOG, 'w', encoding='utf-8') as f:
                    f.write(tail)
        except Exception:
            pass
    except Exception:
        pass


def _merge_map_file(path, header, cand, tag, verbose=True, same_fn=None):
    """按行增量合并 —— 值没变的行**原样保留**，只替换变化行 + 追加新行。

    · 无任何变化时**根本不落盘**（文件 mtime 不变 ⇒ engine 的 mtime 缓存不会失效）
    · 旧有新无的代码**保留**（停牌/退市不删，避免丢历史）
    """
    same_fn = same_fn or (lambda a, b: a == b)
    cmap = dict(cand)
    old = []
    if os.path.exists(path):
        with open(path, encoding='utf-8-sig', newline='') as f:
            rd = csv.reader(f)
            next(rd, None)
            for r in rd:
                if len(r) >= 2 and str(r[0]).strip():
                    old.append((str(r[0]).strip(), r[1]))
    out, changed = [], []
    for k, v in old:
        nv = cmap.get(k)
        if nv is not None and not same_fn(nv, v):
            out.append((k, nv))
            changed.append({'code': k, 'old': v, 'new': nv})
        else:
            out.append((k, v))
    have = {k for k, _ in old}
    added = [{'code': k, 'new': v} for k, v in cmap.items() if k not in have]
    out.extend((a['code'], a['new']) for a in added)
    if changed or added:
        _write_map_csv(path, header, out)
    same = len(out) - len(changed) - len(added)
    if verbose:
        print('    %-20s 行 %-5d 不变 %-5d 改 %-3d 新增 %-3d%s'
              % (tag, len(out), same, len(changed), len(added),
                 '' if (changed or added) else '  (未落盘)'))
    for c in changed[:40]:
        _sync_log('改', '%s %s: %s → %s' % (tag, c['code'], c['old'], c['new']))
    for a in added[:40]:
        _sync_log('增', '%s %s: %s' % (tag, a['code'], a['new']))
    return {'path': os.path.basename(path), 'rows': len(out), 'same': same,
            'changed': len(changed), 'added': len(added),
            'details': changed[:40] + added[:40]}


def sync_maps(force=False, verbose=True):
    """每日一次：拉成分股到缓存 → 与现用 map 逐行对比 → 只改写差异行。

    「每天一次」用 map_sync_state.json 的日期闸实现 —— 11:40 / 15:40 两次
    「更新板块」里只有第一次真拉（848 个请求 ≈ 30-40 秒），第二次直接跳过。
    force=True 可强制重拉（CLI `--sync`）。
    """
    st = _load_json(SYNC_STATE, {}) or {}
    today = datetime.now().strftime('%Y-%m-%d')
    if not force and st.get('date') == today:
        if verbose:
            print('  今日已同步过（%s），跳过' % (st.get('at') or ''))
        return {'ok': True, 'skipped': True, 'date': today, 'at': st.get('at') or ''}
    if not _fq.configured():
        return {'ok': False, 'skipped': True, 'error': '未配置同花顺 API Key'}
    t0 = time.time()
    if verbose:
        print('  [1/3] 拉取目录 + 全部成分股到缓存')
    dump(verbose=verbose)
    if verbose:
        print('  [2/3] 生成候选（不动现用文件）')
    build_maps(verbose=False)
    if verbose:
        print('  [3/3] 增量改写现用 map（无变化的行一个字节都不动）')
    base = os.path.join(BASE_DIR, 'kline_data')
    r_ind = _merge_map_file(os.path.join(base, 'industry_map.csv'), ['代码', '行业'],
                            _read_map_csv(os.path.join(MAP_DIR, 'industry_map.csv'), '代码', '行业'),
                            'industry_map', verbose)
    r_par = _merge_map_file(os.path.join(base, 'industry_parent.csv'), ['细分行业', '上级行业'],
                            _read_map_csv(os.path.join(MAP_DIR, 'industry_parent.csv'),
                                          '细分行业', '上级行业'),
                            'industry_parent', verbose)
    r_con = _merge_map_file(os.path.join(base, 'concept_map.csv'), ['代码', '概念'],
                            _read_map_csv(os.path.join(MAP_DIR, 'concept_map.csv'), '代码', '概念'),
                            'concept_map', verbose,
                            same_fn=lambda a, b: set(str(a).split('|')) == set(str(b).split('|')))
    res = {
        'ok': True, 'skipped': False,
        'date': today,
        'at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'elapsed': round(time.time() - t0, 1),
        'ind': r_ind, 'par': r_par, 'con': r_con,
        'total_changed': r_ind['changed'] + r_par['changed'] + r_con['changed'],
        'total_added': r_ind['added'] + r_par['added'] + r_con['added'],
    }
    _write_json(SYNC_STATE, res)
    _sync_log('本次', '耗时 %.1fs 行业改%d/增%d 上级改%d/增%d 概念改%d/增%d'
              % (res['elapsed'], r_ind['changed'], r_ind['added'],
                 r_par['changed'], r_par['added'], r_con['changed'], r_con['added']))
    if verbose:
        print('  完成 %.1fs：改动 %d 行 / 新增 %d 行 / 无变化 %d 行'
              % (res['elapsed'], res['total_changed'], res['total_added'],
                 r_ind['same'] + r_par['same'] + r_con['same']))
    return res


# ---------------------------------------------------------------- CLI
def _cli():
    acts = set(a.lstrip('-') for a in sys.argv[1:])
    if not acts or 'all' in acts:
        acts = {'dump', 'build', 'report'}
    if not _fq.configured():
        raise SystemExit('未配置同花顺 API Key（数据源设置 → 同花顺 fuyao）')
    print('输出目录: %s' % OUT_DIR)
    if 'dump' in acts:
        print('[1/3] 拉取目录 + 成分股')
        dump()
    if 'build' in acts:
        print('[2/3] 派生 csv')
        build()
    if 'report' in acts:
        print('[3/3] 出对照报告')
        report()
    if 'map' in acts:
        print('[map] 生成 map 候选（不动现用文件）')
        build_maps()
    if 'diff' in acts:
        print('[diff] 候选 vs 现用')
        diff_maps()
    if 'sync' in acts:
        print('[sync] 每日增量修正（拉成分股 → 比对 → 只改写差异行）')
        sync_maps(force=True)


if __name__ == '__main__':
    _cli()
