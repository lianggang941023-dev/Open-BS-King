"""
BS King Quant Terminal — 版本信息与更新检查
=============================================
v2.340 (2026-11-18 用户 m25750): 建立版本机制。

为什么单独一个模块，而不是塞进 server.py：
  server.py 已经 484 KB、8,968 行。版本号会被三处读（/api/version、数据源窗口、
  命令行 `python -m quant_terminal.version`），塞进 server.py 就得 import 整个服务。

版本号规则
----------
本项目沿用自用版一路用下来的 `v2.NNN`：
  - v2.NNN  小版本，每修一个功能/一个 bug 加一
  - v2.NNNa / v2.NNNb  同一版本的补丁（先 a 后 b）
  - 当前 = 仓库根 `VERSION` 文件里那一行，**改版本只改那个文件**
    这里读文件、不写常量 —— 免得代码里和 VERSION 两处各写一个号、迟早对不上。

`APP_BUILD` 不是版本号，是「这份代码哪一天打的包」，用于排查
「我明明更了怎么还是老界面」这类问题。
"""

import json
import os
import subprocess
import threading
import time

# 仓库根：本文件在 <root>/quant_terminal/version.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERSION_FILE = os.path.join(BASE_DIR, 'VERSION')

# 更新检查的远端：GitHub 仓库 + 原始文件地址
GH_REPO = 'lianggang941023-dev/Open-BS-King'
GH_API = 'https://api.github.com/repos/%s' % GH_REPO
GH_RAW = 'https://raw.githubusercontent.com/%s/main/VERSION' % GH_REPO

# 检查更新的最短间隔（秒）。GitHub 对未登录请求限 60 次/小时，
# 每次开数据源窗口都查一次很容易打满，所以留 60 秒缓存。
CHECK_TTL = 60.0

_lock = threading.Lock()
_last = {'ts': 0.0, 'data': None}


def current():
    """读 <root>/VERSION 的第一行；文件缺失时返回 'v0.0.0' 而不是抛异常 ——
    版本号读不到不该让整个服务起不来。"""
    try:
        with open(VERSION_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith('#'):
                    return s
    except Exception:
        pass
    return 'v0.0.0'


def _vtuple(v):
    """'v2.340' → (2, 340, 0, 0)；'v2.340a' → (2, 340, 1, 0)；'v2.340b' → (2, 340, 2, 0)

    第四位留给以后可能出现的 c/d。解析失败返回 None（调用方据此判「不比较」，
    绝不把解析失败当成「已是最新」—— 那会让人以为检查成功了）。"""
    try:
        s = str(v).strip().lstrip('vV')
        patch = 0
        if s and s[-1].isalpha():
            patch = ord(s[-1].lower()) - ord('a') + 1
            s = s[:-1]
        parts = s.split('.')
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
        return (major, minor, patch, 0)
    except Exception:
        return None


def compare(local, remote):
    """本地 vs 远端：'newer' / 'same' / 'older' / 'unknown'"""
    a, b = _vtuple(local), _vtuple(remote)
    if a is None or b is None:
        return 'unknown'
    if b > a:
        return 'newer'
    if b == a:
        return 'same'
    return 'older'


def _opener():
    """境内源统一请求层：先直连，失败回退代理（与 net.py 同策略）。

    这里**不走 net.py** —— version 要能在 server.py 起不来的时候被单独 import，
    不能反过来依赖它。"""
    import urllib.request
    handlers = [urllib.request.ProxyHandler({})]
    op_direct = urllib.request.build_opener(*handlers)
    try:
        from quant_terminal import net as _net          # noqa: F401
        return op_direct, True
    except Exception:
        return op_direct, False


def _http_text(url, timeout=8.0):
    """取一个 URL 的正文。直连优先，失败再回退系统代理。返回 (text, err)"""
    import urllib.request
    import urllib.error
    errs = []
    # ① 直连
    try:
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req = urllib.request.Request(url, headers={
            'User-Agent': 'BSKing-Terminal/' + current(),
            'Accept': 'application/vnd.github+json',
        })
        with op.open(req, timeout=timeout) as r:
            return r.read().decode('utf-8', 'replace'), ''
    except Exception as e:
        errs.append('direct: %s' % e)
    # ② 回退：系统代理（用户梯子常开）
    try:
        op = urllib.request.build_opener()
        req = urllib.request.Request(url, headers={
            'User-Agent': 'BSKing-Terminal/' + current(),
            'Accept': 'application/vnd.github+json',
        })
        with op.open(req, timeout=timeout) as r:
            return r.read().decode('utf-8', 'replace'), ''
    except Exception as e:
        errs.append('proxy: %s' % e)
    return '', ' | '.join(errs)


def check_update(force=False):
    """检查有没有新版本。

    返回 dict：
      ok            True 表示**查到了远端版本**（不代表有更新）
      local         本地版本
      remote        远端版本（查不到就是 '')
      status        'newer' / 'same' / 'older' / 'unknown'
      url           仓库地址
      build         本份代码的构建标记
      err           失败原因（ok=False 时才有）
      ts            这次检查的时间戳（秒）
      cached        True 表示这是 60 秒内的缓存结果

    为什么要分 ok 和 status：
      「网络不通」和「已是最新」是两件事。合成一个布尔值的话，
      断网时会显示「已是最新」，人就再也不会去手动更新了。"""
    now = time.time()
    with _lock:
        if (not force) and _last['data'] is not None and (now - _last['ts']) < CHECK_TTL:
            d = dict(_last['data'])
            d['cached'] = True
            return d

    local = current()
    txt, err = _http_text(GH_RAW)
    remote = ''
    if txt:
        for line in txt.splitlines():
            s = line.strip()
            if s and not s.startswith('#'):
                remote = s
                break

    out = {
        'ok': bool(remote),
        'local': local,
        'remote': remote,
        'status': compare(local, remote) if remote else 'unknown',
        'url': 'https://github.com/' + GH_REPO,
        'build': APP_BUILD,
        'err': '' if remote else (err or '远端 VERSION 内容为空'),
        'ts': now,
        'cached': False,
    }
    with _lock:
        _last['ts'] = now
        _last['data'] = dict(out)
    return out


APP_BUILD = '2026-11-18'


def info():
    """给前端用的一小份快照（不联网）。"""
    return {
        'version': current(),
        'build': APP_BUILD,
        'repo': GH_REPO,
        'url': 'https://github.com/' + GH_REPO,
    }


def selftest():
    print('VERSION 文件 : %s' % VERSION_FILE)
    print('当前版本     : %s' % current())
    print('构建         : %s' % APP_BUILD)
    print('解析 v2.340  : %s' % (_vtuple('v2.340'),))
    print('解析 v2.340a : %s' % (_vtuple('v2.340a'),))
    print('解析 v2.340b : %s' % (_vtuple('v2.340b'),))
    print('解析 垃圾    : %s' % (_vtuple('abc'),))
    for a, b in [('v2.340', 'v2.341'), ('v2.340', 'v2.340'),
                 ('v2.340', 'v2.340a'), ('v2.340a', 'v2.340'),
                 ('v2.340', 'v2.339'), ('v2.340', 'x')]:
        print('  %-10s vs %-10s → %s' % (a, b, compare(a, b)))
    print()
    print('--- 联网检查 ---')
    r = check_update(force=True)
    for k in ('ok', 'local', 'remote', 'status', 'err'):
        print('  %-8s %s' % (k, r[k]))


if __name__ == '__main__':
    selftest()
