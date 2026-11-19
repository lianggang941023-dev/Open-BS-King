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

## v2.351 — 2026-11-20

**更新功能大修。** 起因是另一台测试机（Windows + Python 3.13 + 没装 Git for Windows）
的「检查更新 / 更新版本」两天都用不了。四个问题逐条查实，全部修掉。

### 修复
- **没有 git 也能查更新**（问题 1）—— 全工程只有两处调 git、都写死 `'git'`，没装 Git 就
  `FileNotFoundError`。新增 `find_git()` 三级回退（PATH → 安装目录 → 各盘 PortableGit），
  并且「检查更新」改走 **HTTP 通道**：`raw.githubusercontent.com` 读 VERSION、
  `api.github.com/repos/.../tags` 读 tag 列表，三路并发只要 **0.5 秒**；
  `git ls-remote` 退为兜底（实测最坏要 90 秒）
- **运行时文件不再阻塞更新**（问题 2）—— 原来 `git status` 里只要有内容就拒绝更新，
  而其中绝大多数是程序自己生成的（`kline_data/*.csv` 追加、各类 `*_cache.json`、`logs/`）。
  新增 `classify_dirty()` 四分类：`runtime` 自动 stash → pull → pop，
  `config`（`data_sources.json` 等）拒绝，`source` / `other` 拒绝并说明原因
- **pull 之前先做三方预演**（问题 3）—— 移植 TSP `scripts/upgrade_check.py` 的思路：
  `git merge-tree --write-tree --merge-base` 预演当前分支与远端的合并，提前告知
  「远端也改了你改过的 N 个文件，其中 M 个会文本冲突」。不再等 pull 报
  `Your local changes would be overwritten by merge` 才发现
- **没有 git 时用 zip 兜底**（问题 4）—— `_up_zip_update()` 下载
  `codeload.github.com/.../zip/refs/heads/main`，只覆盖 `.py/.html/.js/.vbs/.ico/.css`，
  **显式跳过 `data_sources.json`，只允许 `quant_terminal/` 前缀**，
  被覆盖的原文件先备份到 `_bak_zip_<时间戳>/`
- **🔴 「更新版本」按钮从 v2.340 起就是死的** —— `dsVerPull()` 里写的是
  `api('/api/version/pull',{method:'POST'})`，而 `api()` 的签名是 `function api(p)`，
  **第二个参数被静默丢掉** ⇒ 实际发出的是 `GET` ⇒ 路由是 POST-only ⇒ 404。
  更阴的是 404 的 body 是 `{"error":"Not found"}`，`r.json()` 照样解析成功，
  不进错误分支 ⇒ 界面永远只显示「更新失败」，一个字原因都没有。改成 `post()`

### 新增
- `version.py` 的 `merge_check()` / `dirty_breakdown()` / `find_git()` / `remote_info_http()`
  都可以单独调用，便于排查
- `/api/version` 多返回 `git: {ok, path, how}` 与 `repo_ok`；设置窗版本行多一颗
  `#ds-ver-git`，一眼看出本机是「git 副本」还是「无 git · 走 zip 覆盖」

### 安全
- **zip 兜底有版本护栏**：本机版本 ≥ 远端时**拒绝覆盖**。
  自用版没有 `.git`（不是 git 副本），而它通常比远端还新（远端是自己推上去的快照），
  不护栏就会把自己 1.28 MB 的 `terminal.html` 降级成远端旧版
- `.gitignore` 补 4 条：`quant_terminal/logs/`、`kline_data/*_cache.json`、
  `kline_data/.mcap_written.json`、`kline_data/ai_market_recaps.json`

### 说明
- `git merge-tree --write-tree` **有冲突时返回码是 1**（不是 0），
  而项目里 `_git()` 的契约是「rc==0 才算成功」—— 第一版复用它导致现代形态永远拿不到结果、
  `how` 永远是 `legacy`。新增 `_git_local()`（本地只读命令专用，单次、不走代理、
  returncode 原样返回）解决
- `version.compare(local, remote)` 返回的是**远端视角**：`newer` = 远端更新、
  `older` = 本机更新、`same` = 一样。zip 护栏第一版把这两个方向写反了，
  结果「本机更新」时界面显示「已经是最新的」，功能对但话说不清


---

## v2.349 — 2026-11-20

本版把自用版 **v2.343 ~ v2.349** 全部同步过来。原始小版本号保留在括注里，
方便对回开发日志；开源仓库只发汇总的里程碑 tag。

### 新增
- **多股同列（v2.346）** —— 一个窗口里并排看多只股票/指数/板块的日 K 与分时。
  支持 2×1 / 2×2 / 3×3 三种布局，每格可独立切换标的，右键菜单四项操作，
  双击单元格切换日 K ⇄ 分时
  - 任意股票列表右键「加入多股同列 ›」可把当前行加进指定布局的第一个空位（v2.347）
  - 格子可以**左键长按 350ms 拖动**与其他格子互换（v2.347）
  - 标题栏 ⟳ 与 ✕ 之间新增**最大化 / 还原**按钮（v2.347）
  - 窗口打开期间**每 10 秒**把全部布局的行情汇总落盘一次（覆盖式），关窗即停（v2.347）
  - 分时格新增：**右侧涨跌幅刻度**、**昨收 0 轴与现价线**两端固定显示涨跌幅（v2.347e~g）；
    鼠标移入时按 x 轴画**竖虚线 + 该时刻涨跌幅药丸**（v2.347h~k）
- **板块实时（v2.348）** —— 行业板块与概念板块**融合成一个窗口**，顶部两颗 tab 切换，
  当前档高亮、另一档变灰；两档各自缓存（含选中板块与成分股），切档不重算不闪；
  打开后**同时预拉另一档**、时间戳共用一颗；上次看的是行业还是概念记在
  `localStorage['qt_board_kind']`，刷新后还原
- **悬停分时批量拉取（v2.349）** —— 原来是「悬停一个目标就发一次请求」，
  连续看几个就频繁打接口。现在改成三层：
  - **L1 按 key 各自 20 秒冷却**（不是全局 20 秒一次，否则连看多只时后面的全卡住）
  - **L2 队列攒批**：光标停的 P0 目标单独一条快车道（攒 120ms 就发），
    其余预取攒 200ms 一批、单批上限 40
  - **L3 三级预取**：P0 光标那只 / P1 同列上下各 2 行 / P2 整页 + 其余 6 个指数
  - 后端新增 `POST /api/mini_minute/batch`（8 并发 + 失败项**串行重试一次**，避开同花顺偶发 502）
  - 两条保险：**预取结果不写 localStorage**（一只 7KB × 整页 30 只 = 210KB，会撑爆配额）；
    批量期间**不逐只写盘**，整批结束写一次（`_MINI_DEFER` 计数）

### 变更
- 左栏按钮：原「行业板块」改名 **「板块实时」**，原「概念板块」换成 **「多股同列」**；
  K 线工具栏的「田」按钮摘除（入口挪到左栏，功能不变）
- 悬停分时内存缓存上限 `_MINI_MAX` 300 → **1200**（预取一次整页 30 条，300 条不到十分钟就被挤光）

### 修复
- **`miniShow` 切走再切回会重复请求** —— `_MINI.ts` 被无条件清零，被当成「从没拉过」，
  既多发一次请求还闪一下「首次加载」
- **批量接口静默失败** —— `ThreadPoolExecutor` 只在别的函数内部局部 import，
  模块顶层没有 ⇒ `NameError` 被 `try/except` 兜成 HTTP 200 + `{ok:false}`。
  **教训：本模块顶层没有的符号，新函数里必须自己 import；被兜住的异常必须落日志，否则就是假成功**
- **P0 被预取堵住** —— 第一版把光标那只和 19 只预取挤在同一批，整批 5.9 秒才出图
  （改造前单只只要 0.44 秒）。拆出 P0 快车道后回到 **1.6 秒**
- **分时竖虚线在 150% DPI 下完全不可见** —— `border-left:1px dashed` 被算成 `0.666667px`，
  抗锯齿后一个实心像素都占不到（`getComputedStyle` 仍报 1px）。
  改成 `width:1px` + `repeating-linear-gradient` 才能画出来。
  **以后画 1px 细线一律用 width + 背景渐变，别用 border**
- **板块窗口切档后右栏空白** —— 状态条还挂着上一档数字、新档 `selected` 为空，且不报错

### 说明
- 本版是**代码同步**，不涉及数据格式变更；K 线 CSV 仍是 9 列纯 LF、`volume` 单位仍是手
- 自用版与开源版的差异仅剩 `quant_terminal/data_sources.json`（开源版不带任何 Key）

---

## v2.342 — 2026-11-18

### 修复
- **pythonw 下服务起子进程会弹黑窗口** —— 终端版本检查、快照导入、股东户数抓取
  等四处 \subprocess\ 调用在 \pythonw\ 环境下会闪出 cmd 窗口。
  统一给它们加 \creationflags=CREATE_NO_WINDOW\（非 Windows 平台不传该参数）

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