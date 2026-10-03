# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、派单、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 派救流程

派单（`assignments`）是连接事件、区域和资源占用的核心实体，状态机为
`issued → executing → completed / invalidated / superseded`。

- **改派冻结版本**：签发与改派可携带 `expected_incident_version`、`expected_area_version`、
  `expected_asset_version`，按航程、能力和海况确认有效占用。两人同时提交时，后到者收到 409，
  请求自动保留为草稿（`assignment_drafts`），响应携带 `draft_id` 与当前版本，刷新后可提交或放弃。
- **原子占用**：资源占用、区域更新、派单签发在同一事务内完成，不会留下资源被占而区域未派单的状态。
- **失效重算**：事件结束、资源撤回或海况更新后，未执行派单自动失效并释放资源、区域回到待派；
  执行中的派单保留原依据（`basis` 快照）并标记待复核（`review_reasons`），由协调员确认继续或作废。
- **离线批次**：按客户端事件编号（`client_event_id`）幂等合并；同字段冲突写入 `clue_conflicts`
  并列保留，不覆盖已存记录。整批事务化，写入失败整体回滚，客户端重发完整批次即可恢复；
  离线派单事件重放不会重复占船。
- 页面与时间线使用同一派单编号展示，并显示待核原因。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/assets`：登记资源
- `POST /api/areas`：创建搜索区域
- `POST /api/assignments`：签发派单（版本冻结 + 能力/海况/航程校验）
- `POST /api/assignments/reassign`：改派（作废旧单、释放旧资源、签发新单，原子完成）
- `POST /api/assignments/start`：确认派单开始执行
- `POST /api/assignments/review`：复核执行中派单（`confirm` / `invalidate`）
- `POST /api/assignments/drafts/submit`、`POST /api/assignments/drafts/discard`：处理版本冲突草稿
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源，未执行派单失效、执行中的待复核
- `POST /api/incidents/sea_state`：更新海况并重算派单
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：幂等合并离线记录（clue / assignment / timeline）
- `GET /api/incidents/{id}/timeline`

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、改派版本冻结与草稿、
海况/撤回/结束触发的失效重算、离线冲突并列保留与批次恢复、权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
