# -*- coding: utf-8 -*-
"""v2.234 (2026-10-27): 境内数据源统一请求层 —— 先直连, 失败再回退系统代理

背景 (用户 m10187 定案 2026-10-27):
  本机挂着全局 SSR 代理 (127.0.0.1:1080)。同花顺 / 腾讯 这类**境内源**走代理会慢
  4~11 倍, 而且代理链路更容易被掐断 —— fuyao_fetch.log 里 410 条失败中
  SSL 握手超时 155 条(37.8%) + 连接被服务端断开 86 条(21.0%), 真正的 429 只有 76 条(18.5%)。
  实测 (同一 Key / 同一接口, 2026-10-27):
    同花顺 3 只        走代理 1491ms  →  直连 149ms
    同花顺全市场 5579只 走代理 4021ms  →  直连 1202ms
    同花顺板块指数 200只 走代理 793~1675ms → 直连 201~308ms
    腾讯 qt.gtimg.cn 2只 中位 1042ms/最慢 5793ms → 直连 92ms/122ms
    新浪 hq.sinajs.cn    81ms       →  93ms   (不受影响, 故本次不动新浪那两处)

口径 (与 datasource.zt_pool_em 同思路, 但**保留退路**):
  **境内源先直连**, 直连失败才退回系统代理。不能只直连不兜底 ——
  万一哪天直连被挡 (出差/换网络/运营商封), 还得有条退路。

⚠ 两条不能改的细节:
  ① HTTPError 不参与回退 —— 服务器已经回了状态码 (429/403/5xx) 说明网络通,
     换代理毫无意义; 而且 fuyao._req 靠 `except urllib.error.HTTPError` 认 429,
     包装成普通异常会把限流识别打瞎。
  ② 两次尝试共用同一个 timeout, 不许累加成 2 倍。
"""
import urllib.error
import urllib.request

# v2.248 (2026-11-04): **先关掉 IPv6**, 再干别的。
#   本机 IPv6 出网不通, 但解析结果 v6 排在前 ⇒ 每个请求先白等 8 秒才回落 v4
#   (实测 fetch_sina 8.40s → 打补丁后 0.40s, 内容逐字节相同)。
#   放这里的原因: server.py 第一件事就是 import 本模块, 其后 engine / datasource /
#   fuyao / mcap 全被覆盖; socket.getaddrinfo 是全局的, 一处改全进程生效。
try:
    from quant_terminal import netv4 as _netv4
except Exception:
    try:
        import netv4 as _netv4
    except Exception:
        _netv4 = None
if _netv4 is not None:
    _netv4.install()

_OPENER = None


def _direct_opener():
    """全局直连 opener (ProxyHandler({}) = 不读环境变量/注册表里的代理)"""
    global _OPENER
    if _OPENER is None:
        _OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return _OPENER


def open_url(req, timeout=10):
    """先直连, 失败回退系统代理。返回 http.client.HTTPResponse 风格对象。"""
    try:
        return _direct_opener().open(req, timeout=timeout)
    except urllib.error.HTTPError:
        raise                                  # 已拿到状态码, 原样抛 (429/403/5xx 都走这里)
    except Exception as e_direct:
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except Exception as e_proxy:
            raise e_direct from e_proxy        # 报直连那个错: 更接近真实原因
