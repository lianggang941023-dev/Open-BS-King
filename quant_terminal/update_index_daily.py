"""指数日K本地文件 一次性回补 / 手动同步 CLI (v2.57 2026-09-05)

用法: python -m quant_terminal.update_index_daily
流程: 7 码逐一 缺失→全量回补(ensure) → 与远端比对补齐(sync, force=1)
      (force 下盘中不写 forming bar — 由 engine 的 _drop_forming_today 保证)
输出: 根数/首末日期/动作 表; 任一模 fail → 退出码 1
"""
import sys

from quant_terminal import engine

NAME = {'sh000001': '上证指数', 'sz399001': '深证成指', 'sz399006': '创业板指',
        'sh000300': '沪深300', 'sz399905': '中证500', 'sh000688': '科创50',
        'sh000680': '科创综指'}


def main() -> int:
    engine.refresh_dirs()  # 与 server 同口径解析本地路径
    fails = 0
    print(f"{'代码':<10}{'指数':<8}{'根数':>7}  {'首日':<12}{'末日':<12}  动作")
    for code in engine.INDEX_DAILY_CODES:
        name = NAME.get(code, '')
        if engine.read_index_daily(code) is None:
            # 缺失/损坏 → 全量回补
            ok = engine.ensure_index_daily(code)
            act = 'ensure-rebuilt' if ok else 'FAIL(ensure)'
            if not ok:
                fails += 1
        else:
            st = engine.sync_index_daily(code, force=True)
            act = st
            if st == 'fail':
                fails += 1
            elif st == 'missing-rebuilt':
                act = 'missing-rebuilt'
        rows = engine.read_index_daily(code)
        if rows:
            print(f"{code:<10}{name:<8}{len(rows):>7}  {rows[0]['date']:<12}"
                  f"{rows[-1]['date']:<12}  {act}")
        else:
            print(f"{code:<10}{name:<8}{0:>7}  {'-':<12}{'-':<12}  {act}")
    if fails:
        print(f'\n{FAILS_MSG}', file=sys.stderr)
    return 1 if fails else 0


FAILS_MSG = '有指数回补失败 — 检查网络后重跑, 或稍候用 /api/index/sync 端点/guard 自动重试'


if __name__ == '__main__':
    sys.exit(main())
