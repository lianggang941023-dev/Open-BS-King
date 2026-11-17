# -*- coding: utf-8 -*-
"""watchdog 守护进程 (v2.14, 2026-08-27)

每8秒探测 http://127.0.0.1:8799/api/status, 连续3次失败 (约24秒) 判定服务器死亡,
用 CREATE_NO_WINDOW 无窗口拉起 quant_terminal/server.py (不弹CMD黑窗口)。

停止标记: 存在 quant_terminal/.watchdog_off 文件时休眠不拉起 (删除文件恢复监视)。
日志: quant_terminal/watchdog.log (追加, 只写状态变化, 不刷屏)。
启动: python quant_terminal/watchdog.py  (建议用 Start-Process -WindowStyle Hidden)
"""
import os
import sys
import time
import subprocess
import datetime
import socket

# v2.248 (2026-11-04): 只走 IPv4 —— 本机 IPv6 出网不通, 而探测只给 4 秒超时,
#   v6 那个 6~8 秒的空等会让「活着的服务器」恒被判死, 每 36 秒拉起一个新进程
#   自我增殖 (2026-11-04 上午堆到 50 个 python 的直接原因)。
if os.environ.get('BSKING_SHOW_V6') != '1':
    _o = socket.getaddrinfo
    socket.getaddrinfo = lambda h, p, f=0, t=0, pr=0, fl=0: _o(h, p, socket.AF_INET, t, pr, fl)

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HERE = os.path.dirname(os.path.abspath(__file__))
OFF_FILE = os.path.join(HERE, '.watchdog_off')
LOG_FILE = os.path.join(HERE, 'watchdog.log')
URL = 'http://127.0.0.1:8799/api/status'
POLL_SEC = 8
FAIL_THRESHOLD = 3


def log(msg):
    line = f"[{datetime.datetime.now().strftime('%m-%d %H:%M:%S')}] {msg}"
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass
    print(line, flush=True)


def alive():
    # v2.239 (2026-10-29 用户 m13487): **强制直连本机**。
    #   本机 ProxyEnable=1 / ProxyServer=127.0.0.1:1080 且 ProxyOverride 为空 ——
    #   urllib 会连 http://127.0.0.1:8799 都先交给 SSR 代理转发, 实测 282ms vs 直连 3ms(94 倍)。
    #   更糟的是代理一挂/hang, alive() 恒返回 False ⇒ 连探 3 次就拉起一个新进程,
    #   把一个**健康**的服务器杀掉重启 —— 用户看到的就是「8799 莫名其妙被关闭」。
    #   代理对本机回环毫无意义, 所以这里绕开它, 也绝不回退到代理。
    try:
        import urllib.request
        req = urllib.request.Request(URL, headers={'User-Agent': 'watchdog'})
        _op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with _op.open(req, timeout=4) as r:
            return r.status == 200
    except Exception:
        return False


def start_server():
    """无窗口拉起 server.py; 端口被占用时新进程自行报错退出, 下轮再判"""
    flags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    subprocess.Popen([sys.executable, '-m', 'quant_terminal.server'],
                     cwd=BASE, creationflags=flags,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    log(f'watchdog 启动: 每{POLL_SEC}秒探测, 连续{FAIL_THRESHOLD}次失败自动拉起')
    fails = 0
    while True:
        if os.path.exists(OFF_FILE):
            time.sleep(POLL_SEC * 2)
            fails = 0
            continue
        if alive():
            if fails >= FAIL_THRESHOLD:
                log('✓ 服务器已恢复')
            fails = 0
        else:
            fails += 1
            if fails >= FAIL_THRESHOLD:
                log(f'✗ 连续{FAIL_THRESHOLD}次探测失败 → 无窗口拉起新进程')
                start_server()
                fails = 0  # 下轮探测确认, 若仍死则再次拉起
        time.sleep(POLL_SEC)


if __name__ == '__main__':
    main()
