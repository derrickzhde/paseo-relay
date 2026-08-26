# paseo-relay

[Paseo](https://github.com/getpaseo/paseo) 的 WebSocket 中继，Rust 单节点实现。

Paseo 的 daemon 跑在你自己的机器上，App 跑在手机或另一台电脑上。两者不在同一个网络时，双方都主动连到这个中继，由它把 WebSocket 帧对转。载荷是端到端加密的，中继不持有密钥、不解析内容、不落盘。

协议与官方的 `relay.paseo.sh` 一致，daemon 改一行配置就能接上。

## 部署

```sh
cargo build --release
```

产物是单个静态二进制 `target/release/paseo-relay`，无运行时依赖。

它只监听明文 HTTP，TLS 交给前面的 nginx / Caddy。反代必须转发 WebSocket 升级头：

```nginx
location / {
    proxy_pass http://127.0.0.1:4000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 3600s;   # 中继不设空闲超时，反代也不要断
}
```

`proxy_read_timeout` 必须调大。中继本身不会主动断开空闲连接，但 nginx 默认 60 秒就会掐断，导致客户端反复重连。

## 配置

全部通过环境变量，都有默认值。

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PASEO_RELAY_HOST` | `127.0.0.1` | 监听地址 |
| `PASEO_RELAY_PORT` | `4000` | 监听端口 |
| `PASEO_RELAY_ALLOWED_SERVER_IDS` | 空 | 逗号分隔的 serverId 白名单。**留空等于对所有人开放** |
| `PASEO_RELAY_MAX_SOCKETS` | `20000` | 活跃连接上限，达到后 `/ready` 转 503 且拒绝新连接 |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | 每连接控制通知队列上限 |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | 单次写入超时，超时即以 1013 剔除慢连接 |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | 客户端等待 daemon 开数据通道的上限 |
| `PASEO_RELAY_DRAIN` | `false` | 启动即进入排空状态，`/ready` 返回 503 |

### 白名单

不设白名单的话，任何知道你域名的人都能把这个中继当免费内网穿透用，流量费你出。建议填上自己的 serverId：

```sh
PASEO_RELAY_ALLOWED_SERVER_IDS=$(cat ~/.paseo/server-id)
```

多台机器用逗号分隔。白名单只挡陌生 serverId，挡不住已知 serverId 的顶替——这一点与官方中继的风险面相同。

## 让 daemon 用上它

改 daemon 配置里的 `relayEndpoint`（格式是 `host:port`，不是 URL），走 TLS 时同时打开 `relayUseTls`。

## 运维端点

| 路径 | 说明 |
| --- | --- |
| `GET /health` | 存活探针，恒返回 `{"status":"ok"}` |
| `GET /ready` | 就绪探针。排空中或连接数满时返回 503 |
| `GET /metrics` | Prometheus 文本 |

收到 `SIGTERM` 会先进入排空状态（`/ready` 转 503）再退出，方便滚动重启。

## 开发

```sh
cargo test                    # 单元测试
cargo run                     # 起服务
node tests/contract.mjs       # 黑盒契约测试，需要服务已在 4000 端口运行
```

`tests/contract.mjs` 用 Node 内置的 WebSocket，无依赖。它覆盖查询参数校验、v1/v2 路由、广播、关闭码、握手校验、关闭握手、控制通道心跳等 61 项对外行为。

协议契约的完整依据、与官方两份实现的行为差异、以及并发设计的取舍都记在
[`docs/design.md`](docs/design.md)。改动前先读它。

## 与官方实现的关系

官方有两份实现：Elixir/OTP 版（`getpaseo/paseo-relay`，为多节点集群设计）和 Cloudflare Durable Objects 版。两者在若干行为上并不一致；本实现一律取 Elixir 语义，差异逐条记在设计文档 3.10 节。

本实现刻意不做的：跨节点归属仲裁、连接重放、集群就绪门槛、全局字节预算与内存水位线、状态持久化。这些是官方为约 2.3 万并发连接的集群准备的，单节点自建用不上。

## 许可证

Apache-2.0，见 [LICENSE](LICENSE)。

协议契约来自上游 [`getpaseo/paseo-relay`](https://github.com/getpaseo/paseo-relay)（同为 Apache-2.0）与 Paseo 的 Cloudflare Durable Objects 版，代码为独立编写；设计文档中按 `文件:行号` 标注了每条契约的具体依据。
