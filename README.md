# xunfei-retry-auto

一个轻量 Go 反向代理：客户端先请求本服务，本服务再转发到讯飞 MaaS Anthropic 接口；当上游响应能判断为可重试错误（例如 `503` 且 `error.code=10310`、message 包含 `busy` / `try again later`、响应体包含 `engine timeout`，`429` 且响应体包含 `authorization failed`，或 `400` 且响应体包含 `Invalid Argument`）时，会按配置自动重试。

初始请求始终只发 1 份；如果触发可重试错误，后续按重试组并发发起请求。第 1 轮重试组默认发 5 份，第 2 轮发 6 份，之后逐轮递增。任意一份先返回 `200` 就立即返回给客户端，并取消同组其他请求；如果同组出现非 `200` 且非可重试错误，则直接把该错误返回给客户端。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `LISTEN_ADDR` | `:8080` | 代理监听地址 |
| `UPSTREAM_URL` | `https://maas-coding-api.cn-huabei-1.xf-yun.com/anthropic` | 上游接口地址 |
| `MAX_RETRIES` | `1` | 命中可重试上游响应后最多再发起的重试组轮数；`1` 表示初始 1 次后最多再发 1 组 |
| `RETRY_GROUP_BASE` | `5` | 第 1 轮重试组的并发请求数；后续每轮加 1 |
| `RETRY_BACKOFF` | `500ms` | 第 2 轮及之后的重试组启动前等待时间，支持 Go duration，例如 `200ms`、`1s`；第 1 轮重试组会立即发起 |
| `REQUEST_TIMEOUT` | `0` | 单次请求超时；`0` 表示不设置总超时，适合流式响应 |
| `PROXY_PORT` | `8080` | docker compose 暴露到宿主机的端口 |

## 本地运行

```bash
go run .
```

请求示例：

```bash
curl -N http://127.0.0.1:8080/anthropic \
  -H 'Authorization: Bearer <token>' \
  -H 'Content-Type: application/json' \
  -d '{...}'
```

也可以请求根路径 `/`，默认会转发到 `UPSTREAM_URL`；请求 `/v1/messages` 会转发到 `UPSTREAM_URL` 下的 `/v1/messages`。如果请求路径已经以 `/anthropic` 开头，不会重复拼接。

## Docker Compose 部署

```bash
docker compose up -d --build
```

自定义最大重试轮数和首轮并发数：

```bash
MAX_RETRIES=3 RETRY_GROUP_BASE=5 RETRY_BACKOFF=1s docker compose up -d --build
```

## 管理页面

访问：

```text
http://127.0.0.1:8080/admin
```

页面提供：

- 成功率：按 client 请求计算，最终返回 `200` 就算成功；中间重试多少次不影响本次成功判定
- 重试率：触发过至少一轮重试组的 client 请求占比
- 平均首字时间：代理向 client 写出首个响应 body 字节的平均时间
- 重试成功率：只统计重试组里实际完成的上游请求；被同组 `200` 胜出后取消的请求不计入分母
- 5h / 周 / 月请求数：只统计最终成功的 client 请求；5h 按 1 小时桶展示，周按 1 天桶展示，月为本月自然月
- 请求表格：请求时间、首字时间、返回状态码、重试轮数、上游请求数、请求 ID
- 非 `200` 请求支持查看最终返回给 client 的原始错误响应 headers + body

## 日志

服务使用 JSON 日志输出到 stdout，会记录：

- 服务启动配置
- 每个代理请求的 `request_id`、方法、路径、目标地址
- 每次上游请求 attempt
- 命中可重试上游响应时的 retry group、group size 和 `retry_reason`
- 请求完成状态、已发起上游请求数、重试组轮数、耗时、响应字节数

注意：如果上游的 `engine timeout` 本身需要等待很久才返回，例如约 120 秒，重试会在收到这个错误后再发起下一轮请求组。并发组内请求同时进行，所以墙钟耗时大致按轮数增长，但上游请求量会按 `1 + RETRY_GROUP_BASE + (RETRY_GROUP_BASE + 1) + ...` 增长。

## 验证

```bash
go test ./...
```
