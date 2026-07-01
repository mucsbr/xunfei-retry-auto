# xunfei-retry-auto

一个轻量 Go 反向代理：客户端先请求本服务，本服务再转发到讯飞 MaaS Anthropic 接口；当上游响应能判断为可重试错误（例如 `503` 且 `error.code=10310`、message 包含 `busy` / `try again later`、响应体包含 `engine timeout`，或 `429` 且响应体包含 `authorization failed`）时，会按配置自动重试。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `LISTEN_ADDR` | `:8080` | 代理监听地址 |
| `UPSTREAM_URL` | `https://maas-coding-api.cn-huabei-1.xf-yun.com/anthropic` | 上游接口地址 |
| `MAX_RETRIES` | `1` | 命中可重试上游响应后最多再重试的次数；`1` 表示最多请求上游 2 次 |
| `RETRY_BACKOFF` | `500ms` | 每次重试前等待时间，支持 Go duration，例如 `200ms`、`1s` |
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

自定义最大重试次数：

```bash
MAX_RETRIES=3 RETRY_BACKOFF=1s docker compose up -d --build
```

## 日志

服务使用 JSON 日志输出到 stdout，会记录：

- 服务启动配置
- 每个代理请求的 `request_id`、方法、路径、目标地址
- 每次上游请求 attempt
- 命中可重试上游响应时的 retry 和 `retry_reason`
- 请求完成状态、重试次数、耗时、响应字节数

注意：如果上游的 `engine timeout` 本身需要等待很久才返回，例如约 120 秒，重试会在收到这个 503 后再发起下一次请求，因此总耗时可能接近 `上游超时时间 * (MAX_RETRIES + 1)`。

## 验证

```bash
go test ./...
```
