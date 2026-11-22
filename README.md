# BS King 量化终端（开源版）

一套本地运行的 A 股行情 + 选股 + 复盘终端，单进程 HTTP 服务 + 单页网页界面，
浏览器打开 `http://127.0.0.1:8799` 即可使用。**不使用数据库，行情全部落在本地 CSV 上**。

> 本仓库为**开源版**（自用版另有若干本地数据与个人配置）。
> 仅供学习与技术研究，不构成任何投资建议。使用前请阅读 [免责声明](#免责声明)。

---

## 一、它能做什么

| 模块 | 说明 |
|---|---|
| **实时行情** | 全市场快照（新浪主 / 腾讯备），七大指数、涨跌分布、成交额、涨跌停 |
| **选股模式** | 100 分制 / 55 / 20 / AI / 涨停回调 / 龙头战法 / 均线粘合 / 超跌反弹 / 波段低吸 / 竞价打板 / 行业突破 / 概念突破 |
| **行业 / 概念** | 257 个细分行业 + 351 个概念 + 90 个上级行业的当日涨幅排行、龙头分、成分股下钻 |
| **市场总览** | 六维情绪雷达、涨跌分布、站上均线比例、涨停梯队、板块热度、个股榜单、监控中心 |
| **市场环境** | 赚钱/投机/抗跌/趋势四维加权综合分，五态（强势/偏强/震荡/偏弱/弱势），历史时间轴 |
| **情绪周期** | 冰点/启动/发酵/高潮/退潮/修复 六阶段识别 + 阶段×主线交叉表 |
| **连板梯队** | 按板数分档，炸板/断板可点选展开 |
| **龙虎榜** | 榜单 / 机构席位 / 游资席位三级下钻，AI 复盘与历史复盘存档 |
| **模拟交易** | 虚拟资金撮合，9:25~9:30 竞价阶段按开盘价成交 |
| **K 线图** | 日 K / 分时，均线、成交量、趋势线（含波段低吸的趋势线延展） |
| **自动任务** | 交易日 09:27:20 竞价扫描、11:31/11:35、15:31/15:35 快照与板块更新 |
| **数据归档** | 每个窗口的结果自动落盘，下次打开先读存档秒显、再拉新数据覆盖 |

---

## 二、快速开始

### 1. 环境要求

- **Windows 10/11**（服务本身是跨平台的，但随附的启动脚本与部分模块按 Windows 编写）
- **Python 3.10+**（开发与验证环境为 3.12.10）
- 浏览器：Chrome / Edge（界面按 1707×1065 的视口设计，其他尺寸会自动拉伸）

### 2. 安装依赖

```bat
pip install -r requirements.txt
```

### 3. 启动

```bat
python -m quant_terminal.server
```

然后浏览器打开 <http://127.0.0.1:8799>。

> 想让服务在后台无窗口运行，把 `python` 换成 `pythonw`。
> 端口在 `quant_terminal/server.py` 顶部的 `PORT = 8799` 改。

### 4. 首次运行会发生什么

程序会**自动创建**这些运行时文件（都已在 `.gitignore` 里）：

```
quant_terminal/scan_cache.json          扫描结果缓存
quant_terminal/ma_baseline.json         均线基准（用于"名次上升"徽标）
quant_terminal/trend_data.json          行业/概念趋势数据
quant_terminal/*.log                    各模块日志
data_archive/window/*.json              各窗口的落盘存档
data_archive/scan/*.json                竞价打板等模式的历史存档
```

**首次打开各窗口会慢几秒**（要现算一遍全市场），之后就走存档与内存缓存。

---

## 三、数据来源

全部为**公开免费接口**，代码里没有任何内置凭据：

| 用途 | 接口 | 说明 |
|---|---|---|
| 个股实时 | 新浪 `hq.sinajs.cn` | 主源。**volume 单位 = 股** |
| 个股实时 | 腾讯 `qt.gtimg.cn` | 备源。**volume 单位 = 手**（两者差 100 倍，代码里已各自归一） |
| 日 K 线 | 腾讯 `ifzq.gtimg.cn/appstock/app/newfqkline/get` | **前复权（qfq）** |
| 分时 | 腾讯 `ifzq.gtimg.cn/appstock/app/minute/query` | 北交所只有 3 段（无成交额段） |
| 财报 | 东方财富 datacenter | 市盈率 / 市净率等 |
| 涨停池 / 炸板池 / 龙虎榜 | 同花顺（需自备 Key） | 见下节 |
| 自由流通股本 / 市值 | 通达信 tqs（需自备 Token） | 见下节 |

### 可选：接入同花顺与通达信

本仓库**不附带**这两个服务的凭据。要用相关功能，请自行申请后填进
`quant_terminal/data_sources.json`：

```json
{
  "online": {
    "fuyao": { "enabled": true, "key": "<你的同花顺 Key>" },
    "tdx":   { "enabled": true, "key": "<你的通达信 Token>" }
  },
  "ai": {
    "primary": { "name": "智谱",     "key": "<你的智谱 Key>" },
    "backup":  { "name": "DeepSeek", "key": "<你的 DeepSeek Key>" }
  }
}
```

- 同花顺 Key：<https://fuyao.aicubes.cn>（免费额度够个人用）
- 通达信 Token：通达信个人版商城 → 会员中心 → 积分和 Key 管理 → 创建数据服务 Key
  （填到 `tdx/lib/tdxaidata/lib/TdxAiData.ini` 的 `[Token]` 段，或上面的 `tdx.key`）
- AI 复盘 / AI 选股点评需要智谱或 DeepSeek 任一 Key，**没有 Key 时相关功能留空不可用，不会用其他数据顶替**

**没填 Key 时**：同花顺相关的涨停池、炸板池、龙虎榜、行业/概念指数会不可用；
通达信相关的手续费/自由流通股本列会留空。其余功能不受影响。

---

## 四、本地数据目录结构

```
Open BS King/
├── quant_terminal/              # 全部 Python 代码（51 个模块）
│   ├── server.py                # HTTP 服务 + 全部 API 路由（约 9000 行）
│   ├── terminal.html            # 单页前端（约 2 万行，含 ECharts）
│   ├── engine.py                # 选股引擎（100/55/20/AI 等模式）
│   ├── auction_scan.py          # 竞价打板扫描
│   ├── rebound_scan.py          # 超跌反弹
│   ├── wave_scan.py             # 波段低吸
│   ├── board_rank.py            # 行业/概念板块排行
│   ├── board_overview.py        # 市场总览
│   ├── board_rotation.py        # 板块轮动矩阵
│   ├── board_rps_ai.py          # RPS 轮动 AI 分析
│   ├── regime_scan.py           # 市场环境
│   ├── regime_phase.py          # 情绪周期与主线
│   ├── ladder_scan.py           # 连板梯队
│   ├── market_recap.py          # 龙虎榜 + AI 复盘
│   ├── archive.py              # 落盘存档
│   ├── auto_task.py             # 定时任务调度
│   ├── simulator.py             # 模拟交易
│   ├── datasource.py            # 数据源适配层
│   ├── mcap.py                  # 市值/自由流通股本读写
│   ├── newstock.py              # 新股建 CSV
│   ├── watchdog.py              # 守护进程
│   └── ...                      # 其余按功能分模块
│
├── kline_data/
│   ├── daily/                   # 5,590 只个股日 K（前复权，9 列，纯 LF）
│   ├── index_daily/             # 指数日 K
│   ├── sector_index/            # 718 个板块指数
│   ├── ths_index/               # 同花顺行业/概念指数
│   ├── industry_map.csv         # 个股 → 细分行业（257 类）
│   ├── concept_map.csv          # 个股 → 概念（351 类）
│   ├── sub_to_ind.csv           # 细分行业 → 上级行业
│   └── industry_parent.csv      # 上级行业定义（90 类）
│
├── tdx/                         # 通达信 SDK（第三方二进制，见下）
├── dragon_data/
│   └── limit_up_history.csv     # 涨停池历史（龙头战法用）
│
├── data_sources.json            # 数据源配置（在 quant_terminal/ 下）
├── requirements.txt
└── README.md
```

### K 线 CSV 格式（重要）

```
date,open,close,high,low,volume,总市值,流通市值,自由流通股本
2026-11-17,10.780,10.860,10.880,10.730,925247,3617.01,3617.01,1201127.01
```

- **注意列序是 `open,close,high,low`**，不是常规的 OHLC
- **前复权**（qfq），不是原始价
- `volume` 单位 = **手**（1 手 = 100 股）；科创板 688/689 也统一成手
- 后三列是**常量列**：只写在最新那一行，历史行留空以保持完整矩形
- 文件编码 **UTF-8 + 纯 LF**（不能是 CRLF，否则读取会错列）
- 自由流通股本单位 = **万股**

---

## 五、常见任务

| 想干什么 | 怎么做 |
|---|---|
| 改端口 | `quant_terminal/server.py` 顶部 `PORT = 8799` |
| 更新日 K | 界面「⚡ 快照更新」按钮，或等 15:31 自动任务 |
| 重建行业/概念映射 | `python kline_data\import_ths_snapshot.py` |
| 回填自由流通股本 | `python -m quant_terminal._backfill_free` |
| 重建均线基准 | `python -m quant_terminal.backfill_ma_baseline` |
| 关闭自动任务 | `data_sources.json` 里相应开关，或改 `auto_task.py` 的 `SLOTS` |
| 守护进程 | `pythonw quant_terminal\watchdog.py`（8 秒探活，连续 3 次失败自动拉起） |

---

## 六、第三方组件

- **ECharts 5.x** — Apache-2.0，`quant_terminal/echarts.min.js`（已内联，无需联网）
- **通达信 tqs SDK**（`tdx/lib/`）— **第三方闭源二进制，版权归通达信所有**，
  本仓库仅为便于调试而附带，**请在使用前自行确认你的授权**。
  没有它也能运行，只是自由流通股本等少数功能不可用。
  可删除整个 `tdx/` 目录。

---

## 七、免责声明

1. 本项目为**个人学习与技术研究**用途，**不构成任何投资建议**，不推荐任何股票。
2. 所有数据来自第三方公开接口，**准确性、完整性、时效性均不作保证**。
3. 数据接口（新浪 / 腾讯 / 东方财富 / 同花顺 / 通达信）均为其各自公司面向
   个人用户的免费或付费服务，**请遵守各自的服务条款**，不要用于商业用途或高频抓取。
4. 使用本软件进行任何交易决策，**风险自负**，作者不承担任何责任。
5. 界面中出现的 AI 分析内容由大模型生成，**可能存在错误与幻觉**，仅供参考。

---

## 八、版本与更新

版本号写在仓库根的 `VERSION` 文件里，当前版本会显示在终端里
（左下角 **⬡ 数据源设置** → 窗口最底部），旁边有「检查更新」和「更新版本」两个按钮。

- **检查更新**：`git ls-remote` 读 GitHub 上最大的版本 tag，与本地 `VERSION` 比
- **更新版本**：`git pull --ff-only origin main`，拉完需要重启终端
- **改了代码自己发版**：改 `VERSION` → 提交（标题以版本号开头）→ `git tag -a vX.YYY`
  → `git push origin main` + `git push origin refs/tags/vX.YYY`

历史变更见 [CHANGELOG.md](CHANGELOG.md)。

> ⚠️ **网络说明**：本项目实测 `api.github.com` 与 `raw.githubusercontent.com`
> 在部分地区**无法访问**，因此检查更新只依赖 `github.com`（git 协议）。
> 如果你那里 `github.com` 也连不上，检查更新会明确报「连不上」而不是假装「已是最新」。
> 走 `git pull` 时若直连被重置，程序会自动改用 `socks5://127.0.0.1:1080` 重试
> （本地有梯子时有效）。

## 九、支持这个项目

这个终端从行情抓取、指标计算到界面，都是一点点磨出来的。**它完全免费、完全开源，
功能不设任何限制，也不需要注册登录。**

如果它对你的看盘有帮助，欢迎请作者喝杯咖啡 ——

<p align="center">
  <img src="assets/support/wechat-donate.png" alt="微信赞赏码" width="240" />
</p>

- **微信扫一扫**上面的赞赏码，金额随意，几块钱也是心意。
- **打赏不解锁任何功能**，也不换取任何东西 —— 不包含答疑、定制、商业授权，
  前后功能一模一样。
- 它是**纯自愿的个人赠与**，不是购买、不是订阅、也没有任何对价。
- 不想花钱也一样能帮上忙：**Star 一下、提个 Issue、说说你用它的场景**，对我都同样有用。

> 本项目是个人业余作品，不提供商业服务、不开票、不签合同。
> 打赏与代码使用之间没有任何附加条件，MIT 协议下你该怎么用还怎么用。

---

## 十、许可

代码部分采用 **MIT License**（见 [LICENSE](LICENSE)）。
随附的第三方二进制（通达信 SDK）、Apache ECharts 与行情数据文件**不在此许可范围内**，版权归各自所有者，明细见 [THIRD-PARTY.md](THIRD-PARTY.md)。
