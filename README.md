# 机器人作业数据回流服务

这是一个面向机器人作业数据团队的服务端应用，负责管理机型、场景、技能、作业记录、人工标注和数据集。服务使用 FastAPI 提供本地 HTTP 接口，以 SQLite 保存业务数据；质量评分、数据集审核、版本、订阅和统计分析均在同一进程内完成。

## 目录

- `main.py`：应用入口、健康检查和路由注册。
- `app/models`：业务实体及其关系。
- `app/routers`：基础资源、作业、数据集和分析接口。
- `app/services`：评分、统计、策略目录与时间窗口工具。
- `app/seed_data.py`：可重复执行的示例数据初始化逻辑。
- `scripts/init_sample_data.py`：初始化脚本的兼容入口。

## 配置与运行

默认数据库文件为项目根目录的 `robot_data.db`，可以通过 `DATABASE_URL` 指定 SQLite 文件。安装依赖后运行 `python3 main.py`，服务默认监听 `8000` 端口；`GET /health` 返回服务状态，接口文档位于 `/docs`。

初始化示例数据可执行 `python3 scripts/init_sample_data.py`。该命令会重建本地数据库并写入机型、场景、技能、作业、标注及数据集示例。

## 验证

运行 `python3 -m pytest -q` 执行服务和领域工具测试，运行 `python3 -m compileall -q app main.py scripts` 检查编译。测试只使用临时 SQLite 数据库，不需要额外服务。

## 订阅与通知语义

- 同一接收方（`subscriber_team`）对同一数据集在任一时刻只有一条有效订阅；重复订阅幂等返回已有记录，接口通过 `result` 区分 `created`（首次订阅）、`duplicate`（重复请求）与 `restored`（恢复订阅）。
- 取消订阅为软取消，保留订阅与通知历史；重新订阅在同一记录上恢复并递增订阅周期（`epoch`），取消前的旧通知不会随恢复复活。
- 版本发布（审核通过）与通知落库在同一事务中提交，共同成功或共同失败；并发发布只有一个请求能成功，其余返回 409/400。
- 通知按（版本， 订阅）唯一落库，可通过 `GET /api/v1/datasets/{id}/notifications` 追溯每条通知来自哪次订阅（`subscription_id` + `subscription_epoch`）；`GET /api/v1/notifications/unread?subscriber_team=...` 查询当前有效订阅的未读通知，`POST /api/v1/notifications/{id}/read` 标记已读。
