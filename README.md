# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、派单、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 派救流程（可恢复）

派单（assignment）是一等实体，生命周期：`pending`（未执行）→ `active`（执行中）→ `completed` / `invalidated` / `superseded`。每条派单记录创建时的**依据**（事件/区域/资源版本、海况、距离、航程、能力），时间线与页面展示同一派单与待核原因。

- **创建/改派先冻结版本**：提交携带 `expected_incident_version`、`expected_area_version`（改派必填）与 `expected_asset_version`；再按航程、能力和海况确认有效占用。两人同时提交时，后到者收到 409，响应体 `conflict.current` 是最新版本与状态、`conflict.draft` 是其草稿，页面保留草稿并可一键采用最新版本重提。
- **触发重算**：事件结束、资源撤回或海况更新后，未执行派单自动失效（释放资源与区域，等待重算）；执行中派单保留原依据并追加待核原因。复核（`/api/assignments/review`）可按当前条件确认（刷新依据）或失效（释放占用重算）。
- **占用原子性**：资源置占用、区域置已派、生成派单在同一事务内完成，不会出现资源被占而区域未派单的状态。

## 离线批次

- 按 `client_event_id` 幂等合并线索、时间线和派单事件；同字段冲突不覆盖原值，以 `conflicts` 并列保留（kept/incoming）。
- 批次先落库完整载荷再逐事件处理（每事件一个 SAVEPOINT）；写入失败批次标记 `failed` 并保留完整载荷，用同一 `client_batch_id` 重提即可从完整批次恢复。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：冻结版本创建派单
- `POST /api/assignments/reassign`：冻结事件与区域版本后改派
- `POST /api/assignments/start`、`POST /api/assignments/review`
- `POST /api/incidents/sea-state`：更新海况并触发派单重算
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源（未执行失效、执行中待复核）
- `POST /api/areas/complete`、`POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：幂等合并离线记录，失败可恢复
- `GET /api/incidents/{id}/timeline`

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整派救流程、改派版本冻结与有效占用、并发冲突草稿保留、海况/撤回/结束触发重算与待复核、离线幂等、同字段冲突并列、派单事件原子占用、失败批次恢复、重复报警、错误位置和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
