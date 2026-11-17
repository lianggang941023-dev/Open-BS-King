# 更新日志

版本号写在仓库根 `VERSION` 文件的第一行，形如 `v2.340`。
补丁版本用字母后缀：`v2.340a` → `v2.340b`（同一版本的连续修订）。

**发版流程**（三步，缺一不可）：

1. 改 `VERSION` 文件（只改这一处，代码里不写死版本号）
2. `git commit`，**提交标题必须以版本号开头**，例：`v2.341 修 xxx`
   （`quant_terminal/version.py` 在没有 tag 时会从提交标题里抠版本号作兜底）
3. `git tag -a v2.341 -m "..."` 然后 `git push origin main` + `git push origin refs/tags/v2.341`

终端里的「检查更新」会 `git ls-remote` 读远端最大的版本 tag，
比本地 `VERSION` 大就提示有新版本，「更新版本」按钮执行 `git pull --ff-only origin main`。

---

## v2.340 — 2026-11-18

首个带版本机制的版本。

### 新增
- **`VERSION` 文件**：版本号唯一真源，代码里不写死
- **`quant_terminal/version.py`**：版本读取 / 比较 / 检查更新 / 一键更新
- **数据源窗口底部版本区**：显示当前版本与构建日期，带「检查更新」「更新版本」两个按钮
- **`GET /api/version`**：本地版本 + 构建日期（不联网）
- **`GET /api/version/check`**：去 GitHub 查有没有新版本
- **`POST /api/version/pull`**：`git pull --ff-only` 拉取最新代码

### 设计说明
- **为什么检查更新走 git 而不是 HTTP API**：实测本机 `github.com` 直连 200 / 1.8 秒；
  而 `api.github.com` 与 `raw.githubusercontent.com` 直连、走代理**全部连不通**
  （`/raw/` 会 302 跳到 raw 域名，同样不通）。所以只能用 `git ls-remote`，
  它走 `github.com:443` 的 git 协议，是唯一稳定的通道。
- **`ok` 与 `status` 分开返回**：「网络不通」和「已是最新」是两件事。
  合成一个布尔值时，断网会显示「已是最新」，人就再也不会手动更新了。
- **一次 `ls-remote` 拿全量 refs**：直连会被 RST，单次调用最坏要走
  「直连→代理→重试→代理」四趟。原来查 head 和查 tag 各调一次 = 最坏 348 秒，
  合并成一次后 **1.1 秒**。
- **一键更新有三道护栏**：①不是 git 副本（zip 版）⇒ 提示去网页下载
  ②本地有未提交改动 ⇒ 拒绝（强拉会静默冲掉，本仓库无备份）
  ③网络失败 ⇒ 原样回传 git 的 stderr，不包装成「未知错误」

### 修复
- 数据源窗口底部按钮组后面接版本区时，缩进层级要跟着 `h+='...'` 串走，
  不能直接插函数（踩过：把 `renderDSWin` 的字符串拼接切开，整块 JS 语法错）