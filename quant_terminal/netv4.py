# -*- coding: utf-8 -*-
"""v2.248: 强制 IPv4 —— 本机 IPv6 通路是死的, 每个请求白等 8 秒

■ 事故 (2026-11-04 上午, 用户报「8799 很多数据加载不出来」)
  - 域名解析结果里 **IPv6 排在前面**, 但这台机器的 IPv6 出网**完全不通**:
    hq.sinajs.cn / qt.gtimg.cn / push2.eastmoney.com / quotes.sina.cn / www.cls.cn ...
    逐个实测: IPv6 全部 `TimeoutError`(6.00s), IPv4 全部 0.04~0.06s 就通。
  - `requests` / `urllib` 的 getaddrinfo 顺序是「v6 先、v4 后」, 于是**每次请求都先
    等 IPv6 超时**再回落 IPv4。实测 `fetch_sina(7 个指数)`:
      直连 8.4s / 8.8s (v6 白等)   vs   走系统代理 0.71s (代理只走 v4)  —— 完全反过来了。
  - 连锁反应: watchdog 探测 `timeout=4` < 8.1s ⇒ 永远判死 ⇒ 每 36 秒拉起一个新进程
    ⇒ 端口双绑/多绑 ⇒ 一半请求打到卡住的那个 ⇒ 前端大面积「加载中」; python 进程堆到 50 个。

■ 修法
  把 `socket.getaddrinfo` 的 family 锁死 `AF_INET`。一处生效、全进程受益:
  requests / urllib / http.client / 所有数据源模块都不必改。
  实测同一个请求 8.40s → **0.40s** (20 倍), 且内容逐字节相同 (1348B)。

■ 为什么不是「改成走代理」
  走代理确实快 (0.71s), 但用户的梯子随时可能关/换端口; 而 v2.234 之所以改「先直连」,
  是因为境内源走代理曾慢 4~11 倍 + SSL 中断率 37.8%。真正的病根是 IPv6, 不是代理。
  修好 IPv6 之后「先直连」的既定策略不用动。

■ 保留退路
  设环境变量 `BSKING_SHOW_V6=1` 可临时恢复系统默认 (排查问题时用)。
"""
import os
import socket

_orig_getaddrinfo = socket.getaddrinfo
_INSTALLED = False


def _v4_only(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def install():
    """幂等: 全进程只装一次"""
    global _INSTALLED
    if _INSTALLED:
        return False
    if os.environ.get('BSKING_SHOW_V6') == '1':
        return False
    socket.getaddrinfo = _v4_only
    _INSTALLED = True
    return True


def is_installed():
    return _INSTALLED


install()
