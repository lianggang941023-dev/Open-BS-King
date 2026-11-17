# -*- coding: utf-8 -*-
"""行业/概念趋势 升降徽标基线回填 (v5.0, 2026-09-12)
=====================================================
背景: 全量弹窗每行的「名次 n↑」「多头成分股 ↑n」徽标 = 本次结果 vs ma_baseline.json 的
      cur 槽 (最近一次"更新板块"force 的快照)。当板块名对不上时 _ma_deltas 静默跳过 →
      徽标大面积缺失。触发场景:
        ① 口径变更 — v5.0 行业 90 类二级 → 257 类细分, 旧快照名字只剩 27/257 能配上
        ② 基线丢失 / 新机器首次使用 / 长时间没点"更新板块"
        ③ cur 槽被同日重复 force 覆盖成"自己"(差值恒 0, 徽标全空)

解法: 用 analyze_*_ma 的 as_of 回填口径重算「上一交易日」作为基准槽, 再滚动出当日 deltas。
      as_of 参数 v2.61 就实现了(当时就是为补 n-1 日基线预留), 但一直没有调用方。

用法:
    python -m quant_terminal.backfill_ma_baseline                # as_of 自动 = K线末日前一交易日
    python -m quant_terminal.backfill_ma_baseline --date 2026-09-10
    python -m quant_terminal.backfill_ma_baseline --kind sector  # both(默认)|sector|concept
    python -m quant_terminal.backfill_ma_baseline --dry-run      # 只算不写盘

⚠️ 只动 ma_baseline.json 一个文件, 不碰 trend_data.json / K线 / map。
⚠️ 服务进程内有 _MA_BASELINE_CACHE 内存缓存 → 写完必须重启终端 (kill server, watchdog
   约 16s 自动拉起) 才读得到新基线。
"""
import os
import sys
import time
import argparse
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from quant_terminal import engine as E  # noqa: E402

_PROBE_CODES = ('SH600519', 'SZ000001', 'SH600036')


def _csv_last_date() -> str:
    """本地K线最新日期 (探针活跃票CSV末行, 同 breakout._kline_date 惯例)"""
    for c in _PROBE_CODES:
        try:
            kl = E.load_csv_kline(c)
            if kl:
                return kl[-1]['date']
        except Exception:
            continue
    return ''


def _prev_trade_date(last: str) -> str:
    """交易日历 (index_daily sh000001) 中 < last 的最后一个日期"""
    try:
        ds = [b['date'] for b in E.read_index_daily('sh000001')]
    except Exception:
        return ''
    prev = [d for d in ds if d and d < last]
    return prev[-1] if prev else ''


def backfill(as_of: str = None, kind: str = 'both', dry: bool = False,
             lines_sel=None) -> dict:
    """返回统计 dict; error 键存在表示失败未写盘"""
    last = _csv_last_date()
    if not last:
        return {'error': 'K线末日期读取失败 (探针票 %s 全空)' % (_PROBE_CODES,)}
    as_of = as_of or _prev_trade_date(last)
    if not as_of:
        return {'error': 'as_of 推导失败 (index_daily 无早于 %s 的日期)' % last}
    if as_of >= last:
        return {'error': 'as_of(%s) 必须早于 K线末日(%s) — 回填的是"上一交易日"基线' % (as_of, last)}

    ind_map, con_map = E.load_industry_concept_maps()
    bl = E._load_ma_baseline()
    lines_list = [l for l in E.PERSIST_LINES if (not lines_sel or l in lines_sel)]

    print('K线末日=%s  回填基准 as_of=%s  kind=%s  %s' % (
        last, as_of, kind, '(dry-run 不写盘)' if dry else ''))
    stats = {'as_of': as_of, 'kline_last': last, 'slots': {}}

    t_all = time.time()
    for lines in lines_list:
        mp = E.LINES_MAP.get(lines)
        if not mp:
            continue
        for k, src_map, fn in (('sector', ind_map, E.analyze_sector_ma),
                               ('concept', con_map, E.analyze_concept_ma)):
            if kind not in ('both', k):
                continue
            t0 = time.time()
            base = fn(src_map, mp, as_of=as_of)   # 基准 = 上一交易日 (绕缓存)
            t1 = time.time()
            cur = fn(src_map, mp)                 # 当日 (常规口径)
            dt_b, dt_c = t1 - t0, time.time() - t1

            bn = {r['name'] for r in base}
            cn = {r['name'] for r in cur}
            inter = len(bn & cn)
            trial = E._ma_deltas({'rows': E._sanitize_ma_rows(base)}, cur)

            slot = bl.setdefault(k, {}).setdefault(lines, {'prev': None, 'cur': None})
            old_rows = len((slot.get('cur') or {}).get('rows') or [])
            old_deltas = len((slot.get('cur') or {}).get('deltas') or {})

            print('  %-8s[%s] 基准%d行/当日%d行 交集%d  → 新deltas %d条  (旧: %d行/%d条, %s/%.1fs)'
                  % (k, lines, len(base), len(cur), inter, len(trial),
                     old_rows, old_deltas,
                     '命中缓存' if dt_c < 0.2 else '现算', dt_b))
            stats['slots']['%s[%s]' % (k, lines)] = {
                'base_rows': len(base), 'cur_rows': len(cur), 'overlap': inter,
                'deltas': len(trial), 'old_rows': old_rows, 'old_deltas': old_deltas}

            if dry:
                continue
            # 造基准槽 → 滚动 ( _ma_roll_slot 内: 跨日 ⟹ prev=本基准, deltas=vs 本基准 )
            slot['prev'] = None
            slot['cur'] = {'date': as_of, 'ts': '回填基线 %s' % as_of,
                           'rows': E._sanitize_ma_rows(base), 'deltas': {}}
            E._ma_roll_slot(k, lines, cur)

    if not dry:
        E._save_ma_baseline()
        print('已写盘 %s  (耗时 %.1fs)' % (os.path.basename(E.MA_BASELINE_FILE), time.time() - t_all))
        print('注意: 服务进程有 ma_baseline 内存缓存 → 需重启终端才生效 (kill server 后 watchdog 约16s 自动拉起)')
    return stats


def main():
    ap = argparse.ArgumentParser(description='行业/概念趋势 升降徽标基线回填')
    ap.add_argument('--date', default=None, help="基准日 YYYY-MM-DD (默认 = K线末日前一交易日)")
    ap.add_argument('--kind', default='both', choices=('both', 'sector', 'concept'))
    ap.add_argument('--lines', default=None, help="仅指定档位, 逗号分隔 (默认 4,5,6)")
    ap.add_argument('--dry-run', action='store_true', help='只算不写盘')
    a = ap.parse_args()
    sel = tuple(x.strip() for x in a.lines.split(',')) if a.lines else None
    r = backfill(as_of=a.date, kind=a.kind, dry=a.dry_run, lines_sel=sel)
    if r.get('error'):
        print('ERR:', r['error'])
        sys.exit(1)


if __name__ == '__main__':
    main()
