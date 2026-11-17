# 更新说明（Changelog）

## 1.2.2（2026-09-30）

- **新增集合排名行情接口**：
  - `get_domain_sorthq(domain=6, colname="ZAF", sorttype=1, wantcol=[], startxh=0, wantnum=100, data_source=0)` —— 按指定排序栏目获取某一市场、板块或指数成分域内证券的排名行情，可附加返回最多 5 个其他栏目；`data_source=1` 时取期货期权、港股美股等扩展行情。
- **新增扩展市场板块列表接口**：
  - `get_sector_list_ds(list_type=0)`
- **集合竞价接口新增时段参数**：
  - `get_call_auction(stock_code, type=1)` —— 单只证券当日集合竞价数据。
  - `get_call_auction_batch(stock_list, field_list, type=1, return_df=False)` —— 批量获取集合竞价数据。
  - `type=0` 返回早盘与尾盘两段，`type=1`（默认）只返回早盘。
- **集合竞价返回结果不再包含 `InOutFlag` 字段**。

## 1.1.1（2026-09-20）

- 修复了 1.1.0 版本动态库文件不匹配问题。
- 修复部分已知问题，优化使用体验。

## 1.1.0（2026-09-18）

- **新增集合竞价接口**：
  - `get_call_auction(stock_code)` —— 单只证券当日集合竞价逐笔虚拟撮合数据，含开盘（09:15-09:25）与收盘（14:57-15:00）两段。
- **新增批量取数接口（5 个）**：
  - `get_call_auction_batch(stock_list, field_list)` —— 批量获取集合竞价数据。
  - `get_market_snapshot_batch(stock_list, field_list)` —— 批量获取实时行情快照，实测 26 个字段。
  - `get_stock_info_batch(stock_list, field_list)` —— 批量获取股票基本财务数据，实测 62 个字段。
  - `get_more_info_batch(stock_list, field_list)` —— 批量获取股票扩展数据，实测 92 个字段。
  - `get_kzz_info_batch(stock_list, field_list)` —— 批量获取可转债基础信息，实测 27 个字段。

## 1.0.2（2026-09-04）

- **包内模块改名**：随包实现模块由 `tqserver.py` 更名为 `tdxaidata.py`，顶层用法不变（仍为 `from tdxaidata import tqs`）。
- **Python 版本支持**：声明支持 Python 3.7 及以上（`requires-python = ">=3.7"`，classifiers 增加 3.7）。

## 1.0.1（2026-09-04）

- **命名统一**：随包核心模块与使用文档由旧名 `tqServer` 统一更名为 `tdxaidata`。
- **接口字段更新**：`get_more_info`（扩展证券信息）新增 4 个期货/期权字段并同步到文档字段表：
  - `PreVolIn` —— 昨持仓量（期货期权有效）
  - `VolInStock` —— 持仓量（期货期权有效）
  - `ClearPrice` —— 结算价（期货期权有效）
  - `PreMinum` —— 基差/溢价

## 1.0.0（2026-09-03）

- 首次发布至 PyPI。基于 TdxAiData 原生动态库（Windows `TdxAiData.dll` / macOS `libTdxAiData.dylib` / Linux `libTdxAiData.so`）的跨平台行情 / 专业数据 Python 接口。
- 提供行情快照、K 线、证券基础信息、板块、交易日、专业数据、除权除息、股本、涨跌停、分笔、分时、实时订阅、文件下载等 30+ 接口，命名对齐 `tqcenter`。
- 随包分发三平台动态库与 `TdxAiData.ini` 配置模板；无硬依赖，pandas 为可选增强（`pip install tdxaidata[pandas]`）。
