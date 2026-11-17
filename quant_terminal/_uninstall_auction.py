# -*- coding: utf-8 -*-
"""竞价打板 · 一键无损摘除 (v2.249, 2026-11-06)

把 server.py / terminal.html 里所有竞价打板的痕迹还原干净, 还原完比对 md5:
  - 标记块 (# >>> AUCTION BEGIN ... # <<< AUCTION END / <!-- >>> AUCTION BEGIN ... -->)
    按行切除
  - 非标记块的零星改动, 用「反向替换」逐条还原 (new -> old)
  - 反向替换的每条都断言命中次数, 少一处就整体放弃

用法:
  python _uninstall_auction.py              # dry-run, 只报会改什么
  python _uninstall_auction.py --apply      # 真写 (先自动备份)
  python _uninstall_auction.py --check      # 只检查当前是否装着

摘除后 auction_scan.py / auction_ann.py / _t_auction_*.py 这些独立新文件不会自动删,
需要手动删 (它们不影响终端, 留着也行)。
"""
import hashlib
import io
import re
import os
import shutil
import sys
import time

QT = r'D:\AI Working\BS King\quant_terminal'
SERVER = os.path.join(QT, 'server.py')
HTML = os.path.join(QT, 'terminal.html')
BAT = os.path.join(QT, '启动竞价选股.bat')
FLAG = os.path.join(QT, 'auction_off.flag')


def md5(path):
    return hashlib.md5(open(path, 'rb').read()).hexdigest().upper()


# ============================================================
# 一、标记块切除 (整块, 含 BEGIN/END 那两行)
# ============================================================
def cut_blocks(text):
    """切掉所有 # >>> AUCTION BEGIN ... # <<< AUCTION END 与
    <!-- >>> AUCTION BEGIN ... <!-- <<< AUCTION END 块。

    返回 (新文本, 切掉的块数)。END 行本身以及紧随其后的一个换行一起去掉。
    """
    out = []
    n = 0
    lines = text.split('\n')
    i = 0
    prefixes = (('#', '>>>', 'AUCTION', 'BEGIN'), ('<!--', '>>>', 'AUCTION', 'BEGIN'))
    while i < len(lines):
        ln = lines[i]
        stripped = ln.strip()
        # 2026-11-06 修: 标记贴着代码同一行末尾时(server.py SCAN_MODES 那种),
        # 必须连整行一起切, 否则会把 SCAN_MODES = (...) 的定义删掉
        is_begin = ('>>> AUCTION BEGIN' in stripped)
        if is_begin:
            # 找 END
            j = i + 1
            while j < len(lines) and ('<<< AUCTION END' not in lines[j]):
                j += 1
            if j >= len(lines):
                # 找不到 END, 不自作主张
                raise RuntimeError('第 %d 行有 AUCTION BEGIN 但找不到 AUCTION END, '
                                   '不摘除 (文件可能被手工改坏了)' % (i + 1))
            n += 1
            i = j + 1
            continue
        out.append(ln)
        i += 1
    return '\n'.join(out), n


# ============================================================
# 二、非标记块改动的反向还原 (new -> old)
# 顺序敏感: 长串先还原, 否则短串会误匹配
# ============================================================
def rev(name, new, old, times):
    return (name, new, old, times)


HTML_REVS = [
    # isScanMode
    rev('isScanMode',
        "function isScanMode(m){ return ['100','55','20','ai','smart','dragon','glue',"
        "'auction'].indexOf(m)>=0; }",
        "function isScanMode(m){ return ['100','55','20','ai','smart','dragon','glue'"
        "].indexOf(m)>=0; }", 1),
    # labels (2 处)
    rev('labels',
        "var labels={'100':'\u8d8b\u52bf\u4e3b\u5347','55':'\u8d8b\u52bf\u5ef6\u7eed',"
        "'20':'\u8d8b\u52bf\u53cd\u8f6c','ai':'\u6da8\u505c\u7a81\u7834',"
        "'smart':'\u6da8\u505c\u56de\u8c03','dragon':'\u9f99\u5934\u6218\u6cd5',"
        "'glue':'\u5747\u7ebf\u7c98\u5408','auction':'\u7ade\u4ef7\u6253\u677f'};",
        "var labels={'100':'\u8d8b\u52bf\u4e3b\u5347','55':'\u8d8b\u52bf\u5ef6\u7eed',"
        "'20':'\u8d8b\u52bf\u53cd\u8f6c','ai':'\u6da8\u505c\u7a81\u7834',"
        "'smart':'\u6da8\u505c\u56de\u8c03','dragon':'\u9f99\u5934\u6218\u6cd5',"
        "'glue':'\u5747\u7ebf\u7c98\u5408'};", 2),
    # pool-ternary (4 处)
    rev('pool-ternary',
        "(mode==='ai'||mode==='smart'||mode==='dragon'||mode==='glue'"
        "||mode==='auction')?'auto'",
        "(mode==='ai'||mode==='smart'||mode==='dragon'||mode==='glue')?'auto'", 4),
    # btnBright
    rev('btnBright',
        "           'glue':'btn-scan-glue','auction':'btn-scan-auction'};",
        "           'glue':'btn-scan-glue'};", 1),
    # setStMode
    rev('setStMode',
        "  var modes={'100':'\u8d8b\u52bf\u4e3b\u5347','55':'\u8d8b\u52bf\u5ef6\u7eed',"
        "'20':'\u8d8b\u52bf\u53cd\u8f6c','ai':'\u6da8\u505c\u7a81\u7834',"
        "'smart':'\u6da8\u505c\u56de\u8c03','dragon':'\u9f99\u5934\u6218\u6cd5',"
        "'glue':'\u5747\u7ebf\u7c98\u5408','auction':'\u7ade\u4ef7\u6253\u677f'};",
        "  var modes={'100':'\u8d8b\u52bf\u4e3b\u5347','55':'\u8d8b\u52bf\u5ef6\u7eed',"
        "'20':'\u8d8b\u52bf\u53cd\u8f6c','ai':'\u6da8\u505c\u7a81\u7834',"
        "'smart':'\u6da8\u505c\u56de\u8c03','dragon':'\u9f99\u5934\u6218\u6cd5',"
        "'glue':'\u5747\u7ebf\u7c98\u5408'};", 1),
    # BATCH_LABEL
    rev('BATCH_LABEL',
        "                 'ai':'\u6da8\u505c\u7a81\u7834','smart':'\u6da8\u505c\u56de\u8c03',"
        "'dragon':'\u9f99\u5934\u6218\u6cd5','glue':'\u5747\u7ebf\u7c98\u5408',"
        "'auction':'\u7ade\u4ef7\u6253\u677f'};",
        "                 'ai':'\u6da8\u505c\u7a81\u7834','smart':'\u6da8\u505c\u56de\u8c03',"
        "'dragon':'\u9f99\u5934\u6218\u6cd5','glue':'\u5747\u7ebf\u7c98\u5408'};", 1),
    # peakMark
    rev('peakMark',
        "function peakMark(m){return m==='ai'?'1':(m==='smart'?'s':(m==='glue'?'g':"
        "(m==='auction'?'a':'0')));}",
        "function peakMark(m){return m==='ai'?'1':(m==='smart'?'s':(m==='glue'?'g':'0'));}", 1),
    # sortReset
    rev('sortReset',
        "  sortCol=(mode==='glue')?'sync_pct':((mode==='auction')?'auct_pct':'cons_score');",
        "  sortCol=(mode==='glue')?'sync_pct':'cons_score';", 1),
]

# resetTableHead 的两段 (auction 分支 + 还原 else)
# resetTableHead 的 auction 分支不在这里硬编码匹配。
#   v2.250 (2026-11-06) 起改为 main() 里的统一正则扫尾 —— 因为表头列会随
#   需求变化(比如「公告」列换「昨额」), 硬编码整段字符串每次都要跟着改。
RESET_FIRST_NEW = ""
RESET_MID_NEW = ""
RESET_FIRST_OLD = ""
RESET_MID_OLD = ""


def main():
    mode = 'dry'
    if '--apply' in sys.argv:
        mode = 'apply'
    if '--check' in sys.argv:
        mode = 'check'

    srv = io.open(SERVER, 'r', encoding='utf-8', newline='').read()
    html = io.open(HTML, 'r', encoding='utf-8', newline='').read()

    srv_blocks = srv.count('>>> AUCTION BEGIN')
    html_blocks = html.count('>>> AUCTION BEGIN')
    print('=== 当前状态 ===')
    print('server.py   AUCTION 标记块 %d 个' % srv_blocks)
    print('terminal.html AUCTION 标记块 %d 个' % html_blocks)
    print('压舱开关 %s : %s' % (FLAG, '\u5b58\u5728' if os.path.exists(FLAG) else '\u4e0d\u5b58\u5728'))
    print()
    if mode == 'check':
        print('\u88c5\u7740' if (srv_blocks or html_blocks) else '\u5df2\u7ecf\u662f\u5e72\u51c0\u7684')
        return 0
    if not srv_blocks and not html_blocks:
        print('\u6ca1\u6709\u627e\u5230\u4efb\u4f55 AUCTION \u6807\u8bb0\u5757, \u65e0\u9700\u6458\u9664')
        return 0

    # ---- server.py: 只切标记块 ----
    srv2, n1 = cut_blocks(srv)
    print('=== server.py ===')
    print('  切掉 %d 个标记块, %d \u5b57\u8282 \u2192 %d \u5b57\u8282'
          % (n1, len(srv.encode('utf-8')), len(srv2.encode('utf-8'))))

    # ---- terminal.html: 先切标记块, 再反向还原零散改动 ----
    html2, n2 = cut_blocks(html)
    print('=== terminal.html ===')
    print('  切掉 %d 个标记块' % n2)

    ok = True
    for name, new, old, want in HTML_REVS:
        got = html2.count(new)
        flag = 'OK ' if got == want else 'BAD'
        if got != want:
            ok = False
        print('  [%s] %-16s \u56de\u6b63 %d/%d' % (flag, name, got, want))
        if got == want and got > 0:
            html2 = html2.replace(new, old)

    # v2.249 补底: resetTableHead 里所有 auction 分支一律正则删除
    #   (原文件里根本没有 else if(headMode==='auction') 这个分支,
    #    硬编码整段匹配容易失效 -> 改结构化扫尾)
    _before = len(html2)
    # 删除 }else if(headMode==='auction'){ ... } 整个分支 (到下一个 }else{ 为止)
    html2, _n_auct = re.subn(
        r"  \}else if\(headMode==='auction'\)\{\n"
        r"(?:(?!  \}else\{)[^\n]*\n)*",
        '', html2)
    if _n_auct:
        print('  [OK ] %-16s \u5220\u9664 %d \u4e2a auction \u8868\u5934\u5206\u652f (-%d \u5b57\u8282)'
              % ('resetTableHead-auct', _n_auct, _before - len(html2)))
    else:
        print('  [-- ] %-16s \u65e0\u6b8b\u7559' % 'resetTableHead-auct')

    if not ok:
        print('\n\u6709\u53cd\u5411\u66ff\u6362\u9519\u4f4d \u2192 \u6574\u4f53\u653e\u5f03, \u672a\u5199\u5165')
        print('\u63d0\u793a: \u4f60\u53ef\u4ee5\u76f4\u63a5\u7528 _bak_auction_* \u5907\u4efd\u76ee\u5f55\u91cc\u7684'
              'terminal.html \u8986\u76d6\u56de\u53bb (\u90a3\u662f\u6700\u5e72\u51c0\u7684\u8fd8\u539f)')
        return 1

    print('  %d \u5b57\u8282 \u2192 %d \u5b57\u8282'
          % (len(html.encode('utf-8')), len(html2.encode('utf-8'))))
    print('  CRLF=%d (\u5fc5\u987b\u4e3a 0)' % html2.count('\r\n'))
    print()

    if mode == 'dry':
        print('dry-run \u2014 \u52a0 --apply \u624d\u771f\u5199')
        return 0

    # ---- 真写: 先备份 ----
    ts = time.strftime('%Y%m%d_%H%M%S')
    bakdir = os.path.join(QT, '_bak_uninstall_auction_' + ts)
    os.makedirs(bakdir, exist_ok=True)
    shutil.copy2(SERVER, os.path.join(bakdir, 'server.py'))
    shutil.copy2(HTML, os.path.join(bakdir, 'terminal.html'))
    print('\u5907\u4efd: %s' % bakdir)

    io.open(SERVER, 'w', encoding='utf-8', newline='').write(srv2)
    io.open(HTML, 'w', encoding='utf-8', newline='').write(html2)
    print('\u5df2\u5199\u5165 server.py / terminal.html')
    print()
    print('\u6458\u9664\u540e\u5b57\u8282: server.py %d, terminal.html %d'
          % (len(srv2.encode('utf-8')), len(html2.encode('utf-8'))))
    print()
    print('\u4e0b\u4e00\u6b65:')
    print('  1) python -m py_compile server.py')
    print('  2) \u91cd\u542f 8799 (\u5148\u5efa quant_terminal\\.watchdog_off \u518d\u5168\u6740)')
    print('  3) curl -s -o NUL -w "%%{http_code} %%{size_download}" http://127.0.0.1:8799/'
          '  \u5e94\u4e3a 200 \u4e14\u5b57\u8282==\u78c1\u76d8')
    print('  4) \u9875\u9762\u4e0a\u5e94\u5c11\u4e86\u7b2c\u56db\u884c\u4e09\u4e2a\u6309\u94ae')
    return 0


if __name__ == '__main__':
    sys.exit(main())
