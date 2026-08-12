# HTTP 集成（catga-axum）

Axum 适配器提供可组合原语；服务器生命周期、请求大小限制、认证仍由调用方拥有。

## 1. 推荐方式：标准 Axum + `MediatorState`

`MediatorState` 是标准 extractor，可与 `Path`/`Query`/`State` 等任意组合：

```rust,ignore
use axum::{Json, Router, extract::Path, routing::post};
use catga_axum::{CatgaHttpResult, MediatorState};

async fn place_order(
    MediatorState(mediator): MediatorState,
    Json(command): Json<PlaceOrder>,
) -> CatgaHttpResult<Json<OrderAck>> {
    mediator.send(command).await.map(Json).map_err(Into::into)
}

let app = Router::new()
    .route("/orders", post(place_order))
    .layer(catga_axum::CorrelationLayer)        // 关联 ID 传递（见下）
    .layer(catga_axum::TraceContextLayer)       // W3C traceparent/tracestate 传递
    .with_state(AppState { mediator });
```

## 2. 错误与响应映射

- `CatgaHttpResult<T> = Result<T, CatgaHttpError>` — handler 直接返回；`CatgaError` 按 `ErrorCode::http_status_u16()` 映射状态码，body 为紧凑 JSON `{ code, message }`。
- `IntoCatgaHttpResponse`（`CatgaResult<T: Serialize>` 的扩展）：
  - `.into_catga_response(StatusCode::OK)` — 自定义成功状态；`204 NO_CONTENT` 无 body。
  - `.into_catga_created("/orders/42")` — `201 Created` + `Location` 头。
- `axum::middleware::from_fn(endpoint_panic_middleware)` — 把 handler panic 转成稳定的 internal-error 响应（opt-in）。

## 3. 上下文传播

- `CorrelationLayer` / `TraceContextLayer` — tower layer 形式（新代码首选）。
- 出站 HTTP：`CorrelationHttpClient` 保留调用方提供的关联/trace 头，ambient Catga 上下文只补缺；也可手动 `propagate_correlation_header(&mut headers)` / `propagate_trace_context_headers(&mut headers)`。
- **信任边界**：入站关联头在应用中间件校验/替换前一律视为不可信。

## 4. 快捷宏（原型/小服务；非必需）

```rust,ignore
// handlers + mediator + typed 路由一次组合
let app = catga_axum::catga_application! {
    handlers {
        request GetOrder => GetOrderHandler;
        command PlaceOrder => PlaceOrderHandler;
        event OrderCreated => [ProjectionHandler];
    }
    routes {
        requests {
            @post "/orders/get" => GetOrder,
            "/orders" => PlaceOrder,          // 默认 POST
        }
        events {
            "/events/order-created" => OrderCreated,
        }
    }
}?;
// app.mediator() / app.router() 供应用自有服务器使用

// 仅路由：catga_routes! { mediator = m; requests { .. } events { .. } }
// 原生 axum 路由：axum_routes! { router; GET "/healthz" => health, .. }
// OpenAPI 元数据：catga_endpoint_metadata! { commands { .. } queries { .. } events { .. } }
```

单条路由函数：`mediator_route` / `mediator_route_with_method` / `event_route` / `event_route_with_method`。

## 5. 集群/Raft 路由

- `leader_forward_route` / `leader_forward_route_at` — follower 转发到 leader（配合 `HttpClusterForwarder`，见 [distributed.md](distributed.md)）。
- `raft_message_route` — Raft 协议消息 HTTP 入口（`RAFT_MESSAGE_PATH = "/api/catga/raft"`，帧上限 `MAX_RAFT_MESSAGE_BYTES = 1 MiB`）。**必须**前置 mTLS/签名帧认证 + `RaftPeerIdentity` + `StaticRaftInboundPolicy`。
- `HttpRaftTransport` — `RaftTransport` 的 HTTP 实现。

## 6. 端点校验

`EndpointValidation` 与校验函数（`validate_required` / `validate_not_empty` / `validate_min_length` / `validate_max_length` / `validate_min_count` / `validate_positive` / `validate_range`）从 catga-core 重导出，用于在 handler 入口做输入校验（失败 → `ErrorCode::Validation` → HTTP 422）。
