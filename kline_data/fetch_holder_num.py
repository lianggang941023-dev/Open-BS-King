# -*- coding: utf-8 -*-
"""fetch_holder_num.py — 全市场股东户数拉取 (东财F10, 每股票最近10期)

用法:
  全量: python kline_data/fetch_holder_num.py [--limit N]
  增量: python kline_data/fetch_holder_num.py --incremental [--limit N]
数据源: emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageAjax?code={SH/SZ前缀}{code}
输出: kline_data/holder_num.csv
  全量 = 重写全表; 增量 = 保留旧行, 只追加 CSV 中没有的 report_date (财报密集期每天有新数据)
  增量失败清单落盘 kline_data/holder_num_fail.csv (每次覆盖)
格式: code,report_date,holder_num,total_ratio,avg_free_shares,hold_focus,price
   code=SZ300750  report_date=2026-06-30  holder_num=股东总户数
   total_ratio=较上期% (正=户数增/筹码分散, 负=户数减/筹码集中)
   avg_free_shares=户均持股(股)  hold_focus=筹码评级  price=期末收盘价
说明: 季度低频数据, 每季度收盘后全量重写 (~10分钟, 10线程); 财报密集期每日增量 (~10分钟)
"""
import os, sys, csv, time, json, gzip, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# v2.25 打包版: 冻结后 __file__ 指向 _MEIPASS 临时目录(只读), 必须用 exe 旁 kline_data (可写数据区)
if getattr(sys, 'frozen', False):
    BASE = os.path.join(os.path.dirname(sys.executable), 'kline_data')
else:
    BASE = os.path.dirname(os.path.abspath(__file__))
DAILY = os.path.join(BASE, 'daily')
OUT = os.path.join(BASE, 'holder_num.csv')
URL = 'https://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/PageAjax?code={code}'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Referer': 'https://emweb.securities.eastmoney.com/'}


def fetch_one(code):
    """单股: 返回 (code, rows) 或 (code, None)"""
    for attempt in range(2):  # 失败重试1次
        try:
            req = urllib.request.Request(URL.format(code=code), headers=HEADERS)
            raw = urllib.request.urlopen(req, timeout=15).read()
            # v2.34 (2026-09-01): 部分请求返回 gzip (0x1f8b), urllib 不自动解压 → 解码崩
            if raw[:2] == b'\x1f\x8b':
                raw = gzip.decompress(raw)
            d = json.loads(raw.decode('utf-8', 'replace'))
            g = d.get('gdrs') or []
            rows = []
            for x in g:
                rd = str(x.get('END_DATE') or '')[:10]
                if not rd:
                    continue
                rows.append(','.join([
                    code, rd,
                    str(x.get('HOLDER_TOTAL_NUM') or ''),
                    str(round(x.get('TOTAL_NUM_RATIO') or 0, 2)),
                    str(x.get('AVG_FREE_SHARES') or ''),
                    str(x.get('HOLD_FOCUS') or ''),
                    str(x.get('PRICE') or ''),
                ]))
            return code, rows
        except Exception:
            time.sleep(0.5)
    return code, None


def load_existing():
    """读现有 CSV → {code: set(report_date)} (增量去重用)"""
    exist = {}
    if not os.path.exists(OUT):
        return exist
    try:
        with open(OUT, 'r', encoding='utf-8', newline='') as f:
            for row in csv.reader(f):
                if len(row) >= 2 and row[0] != 'code' and row[0].strip():
                    exist.setdefault(row[0].strip(), set()).add(row[1].strip())
    except Exception:
        exist = {}
    return exist


def main():
    limit = 0
    if '--limit' in sys.argv:
        try:
            limit = int(sys.argv[sys.argv.index('--limit') + 1])
        except (ValueError, IndexError):
            pass
    incremental = '--incremental' in sys.argv

    codes = sorted(fn[:-4] for fn in os.listdir(DAILY) if fn.endswith('.csv'))
    if limit > 0:
        codes = codes[:limit]
    total = len(codes)
    mode = '增量' if incremental else '全量'
    print(f'股票总数: {total} (模式: {mode})', flush=True)

    exist = load_existing() if incremental else {}
    if incremental:
        print(f'已有存档: {len(exist)} 只股票 (新增期数将追加)', flush=True)

    ok, fail, nrows, added = 0, 0, 0, 0
    t0 = time.time()
    rows_out, fail_list = [], []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fetch_one, c): c for c in codes}
        for i, fut in enumerate(as_completed(futs), 1):
            code, rows = fut.result()
            if rows is None:
                fail += 1
                fail_list.append(code)
                if fail <= 10:
                    print(f'  失败: {code}', flush=True)
            else:
                ok += 1
                if incremental:
                    known = exist.get(code, set())
                    rows = [r for r in rows if r.split(',')[1] not in known]
                nrows += len(rows)
                added += len(rows)
                if rows:
                    rows_out.append('\n'.join(rows))
            if i % 500 == 0:
                eta = (time.time() - t0) / i * (total - i) / 60
                print(f'  进度 {i}/{total}  成功{ok} 失败{fail}  新增行{added}  预计剩余{eta:.0f}分钟', flush=True)

    header = 'code,report_date,holder_num,total_ratio,avg_free_shares,hold_focus,price\n'
    if incremental and os.path.exists(OUT):
        # 保留旧行, 追加新行
        old = []
        with open(OUT, 'r', encoding='utf-8', newline='') as f:
            for i, line in enumerate(f):
                if i > 0 and line.strip():
                    old.append(line.rstrip('\n'))
        body = old + rows_out
    else:
        body = rows_out
    tmp = OUT + '.tmp'
    with open(tmp, 'w', encoding='utf-8', newline='') as f:
        f.write(header)
        if body:
            f.write('\n'.join(body) + '\n')
    os.replace(tmp, OUT)

    # 失败清单落盘 (补漏用: 下次增量/全量会重试)
    try:
        with open(os.path.join(BASE, 'holder_num_fail.csv'), 'w', encoding='utf-8', newline='') as f:
            f.write('code\n' + '\n'.join(fail_list) + ('\n' if fail_list else ''))
    except Exception:
        pass

    print(f'\n完成({mode}): 成功{ok} 失败{fail} 新增行{added} 总行数{nrows} 耗时{(time.time()-t0)/60:.1f}分钟 -> {OUT}', flush=True)
    if fail:
        print(f'失败 {fail} 只已写入 holder_num_fail.csv (下次更新重试)', flush=True)


if __name__ == '__main__':
    main()
