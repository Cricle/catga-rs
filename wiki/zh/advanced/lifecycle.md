# 生命周期管理

## TransportLifecycle

显式管理传输层生命周期：

```rust
use catga_core::{TransportLifecycle, TransportLifecycleOptions};

let lifecycle = TransportLifecycle::new(transport)
    .with_options(TransportLifecycleOptions {
        drain_timeout: Duration::from_secs(30),
        ..Default::default()
    });

// 启动
lifecycle.start().await?;

// 停止接受新消息
lifecycle.stop_accepting().await?;

// 排空中
lifecycle.drain().await?;

// 完全关闭
lifecycle.shutdown().await?;
```

## AcceptanceGate

无锁停止接受组件：

```rust
use catga_core::AcceptanceGate;

// 检查是否接受新请求
if gate.is_accepting() {
    // 处理请求
}

// 停止接受
gate.close();
```

## OperationTracker

RAII  drain slots：

```rust
use catga_core::{OperationTracker, OperationGuard};

let tracker = OperationTracker::new(10);  // 10 个槽位

// 获取槽位
let guard = tracker.acquire().await?;
// guard 自动释放槽位
drop(guard);

// 等待所有槽位释放
tracker.drain().await?;
```

## RecoveryManager

自动恢复管理：

```rust
use catga_core::{RecoveryManager, AutoRecoveryOptions};

let manager = RecoveryManager::new(transport)
    .with_options(AutoRecoveryOptions {
        retry_interval: Duration::from_secs(5),
        max_retries: 10,
        backoff: RetryJitter::exponential(Duration::from_secs(1)),
    });

// 启动恢复循环
manager.start().await?;

// 手动触发恢复
manager.recover().await?;
```

## ShutdownCoordinator

协调多组件关闭：

```rust
use catga_core::ShutdownCoordinator;

let coordinator = ShutdownCoordinator::new();

// 注册组件
coordinator.register("transport", transport_lifecycle);
coordinator.register("consumer", consumer_lifecycle);

// 触发关闭
coordinator.shutdown().await?;

// 等待完成
coordinator.await_termination().await?;
```
