# Paseo Relay · Rust 单节点实现设计

日期：2026-08-26

## 1. 背景

Paseo 的 daemon 跑在自己的机器上，App 跑在手机或另一台电脑上。两者不在同一网络时，需要一个公网中继把双方的 WebSocket 连起来。官方提供 `relay.paseo.sh:443`，也开源了两份实现：

- `getpaseo/paseo-relay` — Elixir/OTP，2824 行，为约 2.3 万并发连接的多节点集群设计
- `packages/relay/src/cloudflare-adapter.ts` — Cloudflare Durable Objects，614 行

本项目是第三份实现：Rust，单节点，自用。

**关键前提**：relay 是哑管道。daemon 与 App 之间的内容用 X25519 端到端加密，relay 不持有密钥、不解析载荷、不落盘。

## 2. 范围决策

已确认的边界：

| 项 | 决策 |
|---|---|
| 部署规模 | 单节点。砍掉跨节点归属仲裁、409 重放、集群就绪门槛 |
| 背压 | 简化版：每连接有界发送队列 + 写超时，超限以 1013 关闭慢端 |
| 访问控制 | `serverId` 白名单；白名单为空时退回全开放（便于本地调试） |
| 定位 | 自用工具。不做发布级文档与 CI 矩阵 |
| 协议版本 | v1 与 v2 都实现。客户端当前只用 v2，v1 成本极低（一个槽位对转） |

TLS 由前置的 nginx/Caddy 终止，relay 只监听明文 HTTP。

## 3. 对外协议契约

本节是实现的验收基准。每条都标注了 Elixir 版的出处，实现时逐条对照。

### 3.1 端点与查询参数

WebSocket 端点固定为 `/ws`。客户端 URL 由 `packages/protocol/src/daemon-endpoints.ts:176` 的 `buildRelayWebSocketUrl` 构造，形如：

```
ws(s)://<host>:<port>/ws?serverId=<id>&role=<server|client>&v=2[&connectionId=<id>]
```

**该函数不支持任何额外参数或自定义路径**，因此 relay 侧不能引入基于 URL 的鉴权字段。

参数校验（`lib/paseo_relay/connection.ex:33-77`）：

| 参数 | 规则 | 违规响应 |
|---|---|---|
| `role` | 必须**原文**等于 `server` 或 `client`，不 trim | 400 `Missing or invalid role parameter` |
| `serverId` | 1–256 字节，**不 trim** | 缺失/空 400 `Missing serverId parameter`；超长 400 `serverId is too long` |
| `v` | **trim 后**：缺失/空串/`1` → 1；`2` → 2 | 其他 400 `Invalid v parameter (expected 1 or 2)` |
| `connectionId` | v1 忽略；v2 **trim 后** ≤256 字节 | 超长 400 `connectionId is too long` |

trim 与否逐个参数不同：`role` 和 `serverId` 取原文，`v` 和 `connectionId` 先 trim（`connection.ex:39,50,64`）。统一处理会引入行为差异。

v2 且 `role=client` 且 `connectionId` 为空时，relay 生成 `conn_` + 8 随机字节的小写十六进制（`connection.ex:74`）。

对 `/ws` 的非 upgrade 请求返回 426 `Expected WebSocket upgrade`（`socket.ex:52`）。

### 3.2 连接角色（v2）

| 角色 | 判定 | 数量 |
|---|---|---|
| 控制通道 | `role=server` 且 `connectionId` 为空 | 每个 serverId 一条 |
| 数据通道 | `role=server` 且 `connectionId` 非空 | 每个 connectionId 一条 |
| 客户端 | `role=client` | 每个 connectionId 可多条 |

同槽位被新连接占用时，旧连接以 **1008 `Replaced by new connection`** 关闭（`ownership.ex:307-330`）。这是 daemon 断线重连能接管自己位置的前提，不可改为拒绝新连接。

### 3.3 转发规则

- 客户端帧 → 同 `connectionId` 的数据通道，**单播**（`ownership.ex:196-201`）
- 数据通道帧 → 同 `connectionId` 的所有客户端，**广播**（`ownership.ex:203-207`）
- 控制通道的入站帧**不转发给任何人**，只处理 JSON ping（`socket.ex:88-93`）
- v1：server ↔ client 对转，单播；**无对端时直接丢弃，不等待**（`ownership.ex:189-192`）

载荷原样透传，不改动 opcode，不重新分片。

**同一源连接的帧必须按到达顺序投递。** Elixir 版在投递进行中把后续帧排进源连接自己的 `pending` 队列，完成一条再取下一条（`socket.ex:200-245`）。Rust 侧的等价约束是：投递必须在读循环内串行 `await`，**不得 `spawn` 出去并发投递**——那样会打乱源顺序。这条约束在压测下才会暴露，实现时要明确。

### 3.4 数据通道就绪等待

客户端连上时数据通道通常还不存在。这里有两条**互相独立**的计时线，实现时不要合并：

**A · 催促控制通道**（`ownership.ex:257-268, 332-345`），从客户端 attach 起算：

1. 客户端 attach → 立即向控制通道发 `{"type":"connected","connectionId":"<id>"}`，并排一个 10 秒定时器
2. 10 秒到点仍无数据通道 → 向控制通道补发 `{"type":"sync","connectionIds":[...]}`，再排一个 5 秒定时器
3. 又 5 秒仍无数据通道 → **控制通道**以 1011 `Control unresponsive` 关闭

**B · 挂起客户端帧**（`ownership.ex:196-201, 397-410`），从**该客户端发出第一帧**起算：

- 帧被挂起等待数据通道就绪，上限 `data_attach_timeout_ms`（默认 15 秒）
- 超时 → **该客户端**以 1013 `Data route unavailable` 关闭

两条线起点不同（attach 与首帧），终点不同（关控制通道与关客户端）。默认参数下总时长恰好都是 15 秒，容易误合并成一个定时器。

### 3.5 控制通道消息

relay → daemon：

| 消息 | 时机 |
|---|---|
| `{"type":"sync","connectionIds":[...]}` | 控制通道 attach 后**立即**发一次；nudge 时补发 |
| `{"type":"connected","connectionId":"<id>"}` | 客户端 attach |
| `{"type":"disconnected","connectionId":"<id>"}` | 某 connectionId 的最后一个客户端断开 |

daemon → relay：`{"type":"ping"}` → relay 回 `{"type":"pong","ts":<毫秒时间戳>}`（`socket.ex:376-380`）。其他类型静默忽略。

**attach 后立即发 sync 这条不能省。** daemon 侧 `relay-transport.ts:58` 的 `CONTROL_READY_TIMEOUT_MS = 8000`：控制通道打开后 8 秒内收不到任何可解析的控制消息就 terminate 重连。省掉这一发会让 daemon 陷入无限重连。

**必须响应 WebSocket 协议层 ping。** daemon 每 10 秒发一次协议 ping（`relay-transport.ts:56, 214-248`），30 秒收不到任何东西就判定连接僵死并重连（`relay-transport.ts:57`）。

已用最小实验验证：axum 0.8 会自动回复协议 ping，且 `Message::Ping` 同时透给应用层。但**自动回复发生在读循环调用 `recv()` 的时刻**——读循环一旦挂起就不再回 pong。

由此得到一条硬约束：**控制通道的读循环不得被任何投递阻塞。** 当前设计天然满足（控制通道入站帧只用于 ping/pong，不转发，不会 await 任何发送队列）。daemon 也只在控制通道上发 keepalive，数据通道不发（`relay-transport.ts:345-409`）。若日后让控制通道参与转发，这条会失效并导致 daemon 每 30 秒重连一次。

### 3.6 握手校验

唯一不透明转发的例外。仅对 `role=client` 的连接生效（`socket.ex:170-186`）。

对每个 text/binary 帧尝试 JSON 解析，若 `type` 为 `hello` 或 `e2ee_hello`，则校验 `key` 字段（`handshake_validation.ex:44-66`）：

1. 标准 Base64（带 padding）可解码
2. 解码后恰好 32 字节
3. 重新编码后与原串逐字相等（拒绝非规范编码）
4. 按小端解释为 256 位整数，必须小于 2²⁵⁵−19
5. 不在 7 个已知弱 X25519 公钥黑名单内（`handshake_validation.ex:14-22`）

任一不满足 → **1008 `Invalid handshake key`**，该帧不转发。非握手帧原样转发。

### 3.7 字节上限

| 项 | 值 | 出处 |
|---|---|---|
| 线上帧上限 | 32 MiB | `protocol.ex:5` |
| 载荷上限 | 32 MiB − 14 = 33554418 | `protocol.ex:7` |
| 控制通道载荷上限 | 64 KiB | `protocol.ex:14` |
| serverId / connectionId | 256 字节 | `connection.ex:3` |

超限以 1009 关闭。256 字节的路由键上限是 Elixir 版独有的，Cloudflare 版不设此限（README 已声明）。

### 3.8 关闭码

| 码 | reason | 触发 |
|---|---|---|
| 1001 | `Client disconnected` | 最后一个客户端断开 → 关闭对应数据通道 |
| 1008 | `Invalid handshake key` | 握手 key 非法 |
| 1008 | `Replaced by new connection` | 同槽位被新连接顶替 |
| 1009 | 依据不足 | 帧或重组消息超限。Elixir 测试有意忽略 reason 文本，未固化 |
| 1011 | `Control unresponsive` | **关闭的是控制通道**：客户端接入后 10 秒发 sync 催促、再 5 秒仍无数据通道（3.4 的 A 线） |
| 1012 | `Server disconnected` | 数据通道断开 → 关闭其下所有客户端 |
| 1013 | `Slow consumer` | 控制队列溢出或写回执超时 |
| 1013 | `Data route unavailable` | 客户端挂起的帧等数据通道超时（3.4 的 B 线） |
| 1013 | `Delivery unavailable` | 投递路径失效：目标 writer 已死、控制通道发送失败、或投递返回非超时错误（`socket.ex:126-153`） |

单节点不实现的关闭码，及各自不再可能发生的理由：

| 码与 reason | Elixir 触发条件 | 为何本实现不会发生 |
|---|---|---|
| 1012 `Session owner moved` | Owner 进程消亡（分区收敛后败方被 Syn 淘汰） | 无跨节点归属，无败方 |
| 1012 `Session expired` | 预约 5 秒内未完成 attach，或 Owner 已死（`socket.ex:80,164`） | 无预约机制：upgrade 与插表在同一临界区完成，不存在中间态 |
| 1013 `Relay ingress capacity` | 节点级加权字节账本耗尽 | 无节点级账本，改为每连接队列 |
| 1013 `Relay memory pressure` | BEAM 总内存越过水位线 | 不实现水位线 |
| 1013 `Relay capacity unavailable` | Capacity 账本进程失联 | 无独立账本进程 |

砍掉这些不影响客户端：它们在客户端侧都归入「非正常关闭 → 退避重连」的同一条路径（`relay-transport.ts:262-278`），没有针对具体码的分支逻辑。

### 3.9 HTTP 端点

| 路径 | 响应 |
|---|---|
| `GET /health` | 200 `application/json` `{"status":"ok"}` |
| `GET /ready` | 200 `{"status":"ready"}`；draining 或连接数满时 503 `{"status":"unready"}` |
| `GET /metrics` | 200 `text/plain; version=0.0.4`，Prometheus 文本 |
| 其他 | 404 `text/plain` `not found\n` |

### 3.10 两份官方实现的行为分歧

Elixir 版与 Cloudflare 版并非逐字一致。客户端能同时接受两者，说明这些点上存在容差：

| 行为 | Elixir | Cloudflare | 本实现 |
|---|---|---|---|
| 路由键 256 字节上限 | 有 | 无（`README.md:9-14`） | 跟 Elixir |
| 数据通道未就绪时 | 挂起，逐帧 15 秒超时后关闭来源 | 缓冲最近 200 帧、静默丢弃最旧、不关闭（`cloudflare-adapter.ts:252-274`） | 跟 Elixir |
| hello / e2ee_hello 校验 | 校验，非法即 1008 | 不校验（`cloudflare-adapter.ts:297-309`） | 跟 Elixir |
| 控制通道发送失败 | 1013 `Delivery unavailable` | 1011 `Control send failed`（`cloudflare-adapter.ts:448-505`） | 跟 Elixir |
| `/ready`、`/metrics` | 有 | 404，只有 `/health`（`cloudflare-adapter.ts:583-612`） | 跟 Elixir |
| v1 与 v2 的房间 | 共用房间，槽位独立 | 按 `version + serverId` 分成两个房间（`cloudflare-adapter.ts:602-609`） | 跟 Elixir |

一律取 Elixir 语义：它更严格（静默丢帧和不校验握手都是可观测的弱化），且它自带的黑盒压测工具是本项目的主要验收手段，行为对齐才能直接复用。

**与 Elixir 的一处有意偏差：最后一个客户端离场时删除数据通道条目。**

Elixir 在这里只关闭数据通道、不把它从 `state.data` 移除——其 `close/4` 辅助函数仅调用 `Writer.close`，不改表（`ownership.ex:376-388`）；条目要等数据通道自身 detach 才消失。本实现在同一处直接 `data.remove(cid)`（`room.rs:198`）。

这样做是为了避开一个误杀。设 cid 上原有客户端 K1 与数据通道 D：K1 离场触发关闭 D，此时新客户端 K2 接入同一 cid，随后 D 的 detach 才落地。Elixir 那边 `state.data[cid] == D` 仍然成立，级联分支会把刚接进来的 K2 用 1012 `Server disconnected` 关掉（`ownership.ex:393-408`）。本实现因条目已移除、`owned` 判定为假，级联不触发，K2 转入等待数据通道、由 daemon 依 `connected` 通知重开——代价只是多等一次重开，而不是被关闭。

### 4.1 技术栈

`tokio` + `axum`（WebSocket 走 `axum::extract::ws`，底层 tokio-tungstenite）。选它是因为与 HaloGate 同栈，交叉编译与部署方式已有现成经验。

### 4.2 状态结构

```rust
struct AppState {
    // 单把同步锁保护整张路由表，含所有房间内部状态
    rooms: std::sync::Mutex<HashMap<ServerId, Room>>,
    config: Config,
    metrics: Metrics,
    draining: AtomicBool,
    active_sockets: AtomicUsize,
}

struct Room {
    v1_server: Option<Peer>,
    v1_client: Option<Peer>,
    control: Option<Peer>,
    data: HashMap<ConnectionId, Peer>,
    clients: HashMap<ConnectionId, HashMap<SocketId, Peer>>,
    waiters: HashMap<ConnectionId, Vec<Waiter>>,
}

struct Waiter {
    source: SocketId,                    // 谁在等，源断开时清理
    deadline: Instant,
    tx: oneshot::Sender<Peer>,
}

#[derive(Clone)]
struct Peer {
    id: SocketId,
    inflight: Arc<Semaphore>,              // 容量 1，写完才释放
    data_tx: mpsc::Sender<Outbound>,
    control_tx: mpsc::Sender<String>,
    control_bytes: Arc<AtomicUsize>,       // 控制队列累计字节
    close_tx: mpsc::Sender<(u16, String)>, // 独立，永不被数据挤占
}

enum Outbound {
    Frame {
        msg: Message,
        permit: OwnedSemaphorePermit,   // 随消息转移，writer 写完后 drop
        ack: oneshot::Sender<bool>,     // true = 已写入 socket
    },
}
```

**用一把锁，不用两级锁。** 初版设计是 `Mutex<HashMap<ServerId, Arc<Mutex<Room>>>>`，存在竞态：任务 A 取得 `Arc<Room>` 并释放外层锁后、尚在等待房间锁时，任务 B 可能已把该房间从表中删除；A 随后 attach 到一个孤儿房间上，后续连接从表里查不到它，同一 `serverId` 就分裂成两份。加 generation 校验能补，但更简单的做法是取消这一层：临界区内只有 HashMap 查改与 `Peer` 克隆，纳秒级，单节点规模下一把锁绰绰有余。

**锁类型用 `std::sync::Mutex` 而非 `tokio::sync::Mutex`。** 前者的 Guard 不是 `Send`，编译器会直接拒绝跨 `await` 持锁——把「锁内不得有 IO」从纪律变成编译期保证。这条纪律初版文档里自相矛盾过（4.2 声称锁内无 IO，4.3 却写着持锁 `send().await`），用类型系统钉死更可靠。

### 4.3 每连接任务

一读一写两个 task：

**写 task** 用 `select!` 同时监听三条队列，关闭优先。**每次 sink 写入本身也必须与关闭请求、超时并发**——否则一旦卡在对端零窗口上，writer 就已离开 select 循环，关闭队列里的消息永远无人消费：

```rust
loop {
    tokio::select! {
        biased;
        Some((code, reason)) = close_rx.recv() => { graceful(&mut sink, code, reason).await; break }
        Some(text)           = control_rx.recv() => {
            control_bytes.fetch_sub(text.len(), Relaxed);
            if !write_guarded(&mut sink, Text(text), &mut close_rx).await { break }
        }
        // permit 随 Frame 转移到此处，本轮结束时 drop，在途名额才归还
        Some(Outbound::Frame { msg, permit, ack }) = data_rx.recv() => {
            let ok = write_guarded(&mut sink, msg, &mut close_rx).await;
            drop(permit);
            let _ = ack.send(ok);
            if !ok { break }
        }
    }
}

async fn write_guarded(sink, msg, close_rx) -> bool {
    // 写之前：还没动 sink，可以正常关闭
    if let Ok((c, r)) = close_rx.try_recv() { graceful(sink, c, r).await; return false }

    // 写开始之后：关闭或超时胜出一律硬断，不再复用 sink
    tokio::select! {
        r = sink.send(msg)             => r.is_ok(),
        Some(_) = close_rx.recv()      => false,
        _ = sleep(WRITE_TIMEOUT)       => false,
    }
}
```

**permit 必须随消息转移，不能留在源侧。** 若由源持有、待写完后释放，源任务一旦被取消（连接断开、task drop），permit 会随之提前释放，而 writer 可能仍在写——「两条在途」重新出现。把 `OwnedSemaphorePermit` 放进 `Outbound::Frame`，让 writer 在本轮结束时 drop，名额的生命周期才真正等于「占用 sink 的时长」。

**写入一旦开始，取消后不得复用 sink。** `SinkExt::send` 在 `tokio-tungstenite` 上没有公开的取消安全保证；虽然实测中未写完的字节会被保留、通常不至于破坏帧边界，但这不是可依赖的契约。因此分两段处理：写之前 `try_recv` 到关闭请求，走正常关闭握手（自带 1 秒超时）；写开始之后，无论关闭还是超时胜出，一律丢弃 socket 硬断。代价是这种情况下客户端收不到明确的 1013，只看到连接消失——但两种情况它都会重连，行为不受影响。

**writer 先结束时，读循环必须继续把流读完。** `serve()` 用 `select!` 同时等读循环与 writer，是为了让 writer 的死亡（慢消费者被剔除、写失败）能终止连接、及时释放房间槽位。但 `select!` 落败的一侧会被直接丢弃：若此时对端已发来 `Close`，tungstenite 排队的关闭回声只有在流继续被 poll 时才冲得出去，读循环一被丢弃，回声就永远留在缓冲区，对端看到 1006 而不是我们发出的关闭码。

因此 writer 先结束时：先 detach（房间槽位不因排空而多占），再以 1 秒为上限只 poll `stream.next()` 至流末尾，期间不转发任何帧。压测取证：修复前 30 轮 reconnect 出现 3 次 1006，这 3 条连接全部落在「writer 先结束」的日志里；修复后 395 次 writer 先结束、1006 归零，且排空全部在上限内到达流末尾。

**读循环**（严格串行，见 3.3 的保序约束）：

```
读到一帧
  → 锁 rooms，查目标，克隆出 Peer 列表，释放锁      // 无 IO，无 await
  → 对每个目标：投递并等待写回执，带总超时
  → 处理结果（失败的目标各自关闭并摘除）
  → 读下一帧
```

### 4.4 背压

**入队不等于写出。** `mpsc::send` 返回只表示消息进了队列，此时 WebSocket 可能一个字节都没写。仅靠队列容量做背压会让源比目标超前一整条消息——在 32 MiB 帧下就是多出 32 MiB 驻留。Elixir 版为此设了显式写屏障：Writer 把帧交给目标后要等目标回 `{:written, ref}` 才处理下一条（`writer.ex:63-70,119-123`）。

Rust 侧等价物是每帧附一个 oneshot 回执，写 task 在 `sink.send().await` 返回后才回执，源等到回执才继续。

**但「队列容量 1 + 回执」并不足以保证严格一条在途。** 同一个 `connectionId` 下可以挂多条客户端连接（`clients` 是集合），它们会并发向同一条数据通道投递。当 writer 取走 A 的消息、正在 `sink.send` 时队列已腾空，B 立刻就能入队——于是变成「一条在写 + 一条排队」，最坏驻留 64 MiB 而非 32 MiB。

正确做法是给每个 `Peer` 配一个容量 1 的信号量：投递方**入队前**先取 permit，并把 permit 随消息一起交给 writer，由 writer 在写完（或失败）后 drop。permit 而非队列容量才是真正的在途闸门；而它必须由 writer 持有，理由见 4.3。

**不能用「N 条消息」作为队列上限。** 单帧上限 32 MiB，按条数限制（如 256 条）意味着单个慢连接最坏占用 8 GiB——保护措施本身成了内存放大器。

三条队列各司其职：

| 队列 | 闸门 | 满时行为 |
|---|---|---|
| 数据 | 容量 1 的信号量，写完才释放 | 投递方挂起（背压点） |
| 控制 | 1 MiB 累计字节（同 Elixir） | 目标以 1013 `Slow consumer` 关闭 |
| 关闭 | 独立队列，且参与每次 sink 写的 select | 永不被数据挤占，也不被卡住的写阻断 |

**关闭必须走独立队列，且必须能打断进行中的写。** 只做到「独立队列」不够：writer 一旦进入 `sink.send().await` 并卡在对端零窗口上，它就已经离开了 select 循环，关闭队列里的 1013 永远没人取。所以关闭接收端要参与每一次 sink 写入的 select（见 4.3 的 `write_guarded`）。

控制消息由 relay 自己生成（sync / connected / disconnected），每条几十字节，1 MiB 可排上万条；排不下说明目标已经僵死。

写超时用 `tokio::time::timeout` 包住「入队 + 等回执」，超时则向该目标的关闭队列投递 1013 `Slow consumer` 并从房间摘除。

**广播语义**（对齐 `delivery.ex:26-31`）：向所有目标并发投递，**等待全部完成或超时**，其中任一成功即视为整体成功、源连接继续；失败的目标各自关闭并摘除。注意是「等全部结束后看有没有成功的」，不是「有一个成功就不等其余」——后者会让慢目标的清理逻辑与源的下一帧竞争。

### 4.5 房间生命周期

房间在第一条连接 upgrade 成功时创建，最后一条连接离开且无等待者时删除。因为查表、attach、detach、删除都在同一把锁下完成，不存在「拿到房间句柄后房间被删」的窗口。

Elixir 版的 30 秒 idle 延迟是为了保护「预约但尚未 attach」的中间态；本设计 upgrade 与插表在同一临界区完成，不存在该中间态，因此不需要延迟。

**所有连接退出统一走一个 `detach(socket_id)` 入口**，无论是正常关闭、写超时被摘除，还是任务 panic。该入口按 `SocketId` 比对后再移除——若某槽位已被新连接顶替（3.2 的 1008 路径），旧连接的 detach 不得清除接替者。同一入口负责级联：数据通道退出时关闭其下所有客户端（1012 `Server disconnected`），最后一个客户端退出时关闭对应数据通道（1001 `Client disconnected`）并通知控制通道。

detach 与转发遵循同一条纪律：**锁内只改表并把待发的关闭/通知连同目标 `Peer` 收集成一个动作列表，出锁之后再逐条发送**。级联可能涉及多个目标，若在锁内发送，一个慢目标会阻塞整张路由表。

**等待者必须可清理。** `Waiter` 带源 `SocketId` 与 deadline：数据通道就绪时唤醒、超时后移除并关闭源（1013 `Data route unavailable`）、源连接先行断开时也要从等待表里摘掉，否则房间永远删不掉。

### 4.6 访问控制

配置项 `allowed_server_ids`：非空时，`serverId` 不在其中的 upgrade 请求返回 **403**；为空时不限制。校验在解析查询参数之后、创建房间之前。

这挡住陌生人把 relay 当免费中转站，但挡不住已知 `serverId` 的顶替——后者与官方 relay 的风险面相同。

### 4.7 底层库的三个默认行为

这三条都经查证，照默认值写会直接违反协议契约：

**协议 ping 自动回复成立，但有前提。** tungstenite 在读取时把收到的 Ping 排队成 Pong 并在下次写入时发出（`tungstenite-0.29/src/protocol/mod.rs:281-292,668-674`），因此**只在读循环持续轮询时有效**。这与 3.5 的约束一致：控制通道的读循环不得被投递阻塞。

**帧大小上限必须显式设置，两个都要。** axum 的 `max_frame_size` 与 `max_message_size` 是分开的两个值，默认分别是 16 MiB 和 64 MiB（`axum-0.8/src/extract/ws.rs:194-203`）。帧上限 16 MiB 会拒掉合法的 32 MiB 消息；消息上限 64 MiB 又比协议允许的宽一倍。两个都要显式设：数据通道设 33554418，控制通道设 65536。

**超限不会自动发 1009。** tungstenite 遇到超限只返回 `Capacity(MessageTooLong)` 然后结束流（`tungstenite-0.29/src/protocol/mod.rs:784-790`），客户端看到的是连接消失而不是关闭码。必须捕获该错误并显式发送 1009 关闭帧。这一条要用真实 close frame 的集成测试验证，不能只看代码。

## 5. 配置

环境变量，全部有默认值：

| 变量 | 默认 | 说明 |
|---|---|---|
| `PASEO_RELAY_HOST` | `127.0.0.1` | 监听地址 |
| `PASEO_RELAY_PORT` | `4000` | 监听端口 |
| `PASEO_RELAY_ALLOWED_SERVER_IDS` | 空 | 逗号分隔白名单，空则不限制 |
| `PASEO_RELAY_MAX_SOCKETS` | `20000` | 活跃连接上限。达到后 `/ready` 转 503，新的 upgrade 请求也返回 503 `Relay connection capacity`（对齐 `socket.ex:38-40`） |
| `PASEO_RELAY_CONTROL_QUEUE_BYTES` | `1048576` | 每连接控制通知队列上限（4.4），取值对齐 Elixir 版同名配置；数据侧固定为 1 条在途载荷，不可配 |
| `PASEO_RELAY_DELIVERY_TIMEOUT_MS` | `30000` | 即 4.3 伪代码中的 `WRITE_TIMEOUT`：单次 sink 写入的上限 |
| `PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS` | `15000` | 客户端挂起帧等待数据通道的上限（3.4 的 B 线） |
| `PASEO_RELAY_DRAIN` | `false` | 启动即进入 draining |

## 6. 验证策略

不靠自写的测试自证，四层证据由内向外：

**单元测试**（25 项）：查询参数校验（含各参数 trim 差异）、握手 key 校验（7 个弱密钥、非规范 Base64、非规范坐标）、路由表增删、关闭码选择。

**黑盒契约测试**（61 项，`tests/contract.mjs`）：只用 Node 内置 WebSocket，不触碰本实现的内部结构。覆盖 HTTP 端点、升级检查顺序、查询参数校验、v1/v2 路由、广播、服务端断开级联、连接顶替、握手校验、超限载荷、关闭握手、控制通道心跳与队列限流。Elixir 的 `test/*.exs` 是关闭码与时序契约最权威的来源，其行为断言逐条翻译到了这里。

**官方压测脚本**（上游 `scripts/relay-load.mjs`）：它用真实 WebSocket 和实际部署的 v2 契约，不依赖本实现的内部结构，是主要证据来源。四种场景的实测结果：

- `idle` 41 条连接全部正常关闭
- `sustained` 4059 帧零丢失、零乱序，p99 延迟 2 毫秒
- `reconnect` 30 轮，零异常关闭
- `ownership` 200 个 serverId，零异常
- 每轮结束后 `active_websockets` 与 `active_sessions` 均回零，无泄漏

**真实链路**：daemon 的 `relayEndpoint` 指向自建实例，用 App 连接，验证配对、断线重连与大载荷传输。

3.10 表中已列出的六处分歧属已知行为，不计入缺陷；出现表外差异即为缺陷。

## 7. 明确不做

- 跨节点归属仲裁、`x-reroute-target` 重放、`minimum_cluster_size` 就绪门槛
- 全局加权字节预算、内存水位线、单连接堆熔断、容量账本 epoch 失效
- 连接迁移与跨分区转发
- TLS 终止（交给反向代理）
- 持久化。所有状态在内存，进程重启后客户端按既有重连逻辑自行恢复
