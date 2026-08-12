# Event Sourcing

## 概念

事件溯源存储状态变更而非当前状态：

```
传统方式:                    事件溯源:
┌─────────────┐              ┌─────────────┐
│   Account   │              │   Account   │
│   Balance   │              │   Events    │
│   $100      │              │   Created   │
└─────────────┘              │   Deposited │
                             │   Withdrew  │
                             └─────────────┘
```

## Aggregate

聚合根处理命令并生成事件：

```rust
use catga_core::{Aggregate, Event, CatgaResult};

struct BankAccount {
    id: String,
    balance: i64,
    version: i64,
}

impl Aggregate for BankAccount {
    type Event = AccountEvent;

    fn id(&self) -> &str {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }
}

enum AccountEvent {
    Created { id: String, initial_balance: i64 },
    Deposited { amount: i64 },
    Withdrawn { amount: i64 },
}

impl AccountEvent {
    fn apply_to(&self, account: &mut Option<BankAccount>) {
        match self {
            AccountEvent::Created { id, initial_balance } => {
                *account = Some(BankAccount {
                    id: id.clone(),
                    balance: *initial_balance,
                    version: 1,
                });
            }
            AccountEvent::Deposited { amount } => {
                if let Some(acc) = account {
                    acc.balance += amount;
                    acc.version += 1;
                }
            }
            AccountEvent::Withdrawn { amount } => {
                if let Some(acc) = account {
                    acc.balance -= amount;
                    acc.version += 1;
                }
            }
        }
    }
}
```

## Repository

聚合仓储：

```rust
use catga_core::{AggregateRepository, EventStore};

struct BankAccountRepository<E: EventStore> {
    store: Arc<E>,
}

impl<E: EventStore> AggregateRepository<BankAccount> for BankAccountRepository<E> {
    async fn find(&self, id: &str) -> CatgaResult<BankAccount> {
        let events = self.store.read_stream(id).await?;
        let mut account: Option<BankAccount> = None;
        for evt in events {
            evt.apply_to(&mut account);
        }
        Ok(account.unwrap())
    }

    async fn save(&self, aggregate: &BankAccount) -> CatgaResult<()> {
        self.store.append_stream(aggregate.id(), &aggregate.uncommitted_events()).await
    }
}
```

## 命令处理

```rust
impl BankAccount {
    pub fn deposit(&mut self, amount: i64) -> CatgaResult<()> {
        if amount <= 0 {
            return Err(CatgaError::validation("amount must be positive"));
        }
        self.apply(AccountEvent::Deposited { amount });
        Ok(())
    }

    pub fn withdraw(&mut self, amount: i64) -> CatgaResult<()> {
        if amount <= 0 {
            return Err(CatgaError::validation("amount must be positive"));
        }
        if self.balance < amount {
            return Err(CatgaError::conflict("insufficient funds"));
        }
        self.apply(AccountEvent::Withdrawn { amount });
        Ok(())
    }

    fn apply(&mut self, event: AccountEvent) {
        event.apply_to(&mut Some(self));
    }
}
```

## 快照

定期快照加速重建：

```rust
use catga_core::{SnapshotStrategy, SnapshotStore};

struct BankAccountSnapshot {
    balance: i64,
    version: i64,
}

let strategy = CompositeSnapshotStrategy::new()
    .with_event_count(100)  // 每100个事件快照
    .with_time(Duration::from_secs(3600));  // 或每小时
```
