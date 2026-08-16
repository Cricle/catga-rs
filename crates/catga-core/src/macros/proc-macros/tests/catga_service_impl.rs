//! Behavioral contracts for `#[catga_service]`: request/command/event detection, dynamic
//! registry generation, and the optional typed-mediator variant.

#[path = "support/messages.rs"]
mod messages;

mod plain_service {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use catga_core::CatgaResult;

    use crate::messages::{Ask, Bell, Log};

    #[catga_core::catga_request(response = u64)]
    pub struct Fallible(pub u64);

    #[derive(Clone, Default)]
    pub struct Calculator {
        bells: Arc<AtomicU64>,
    }

    #[catga_core::catga_service]
    impl Calculator {
        async fn answer(&self, ask: Ask) -> CatgaResult<u64> {
            Ok(ask.value + 1)
        }

        async fn archive(&self, _log: Log) -> CatgaResult<()> {
            Ok(())
        }

        async fn on_bell(&self, _event: Bell) -> CatgaResult<()> {
            self.bells.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn fallible(&self, fallible: Fallible) -> Result<u64, catga_core::CatgaError> {
            Ok(fallible.0 * 3)
        }

        pub async fn ping_only(&self) -> CatgaResult<()> {
            Ok(())
        }

        pub const MAX_RETRIES: u32 = 3;

        pub fn sync_helper(&self) -> u32 {
            5
        }
    }

    pub fn bell_counter(calculator: &Calculator) -> Arc<AtomicU64> {
        Arc::clone(&calculator.bells)
    }
}

mod typed_service {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use catga_core::CatgaResult;

    use crate::messages::{Ask, Bell, Log};

    #[derive(Clone, Default)]
    pub struct Bank {
        balance: Arc<AtomicU64>,
    }

    #[catga_core::catga_service(BankMediator)]
    impl Bank {
        async fn balance(&self, _ask: Ask) -> CatgaResult<u64> {
            Ok(self.balance.load(Ordering::SeqCst))
        }

        async fn deposit(&self, _log: Log) -> CatgaResult<()> {
            self.balance.fetch_add(10, Ordering::SeqCst);
            Ok(())
        }

        async fn on_bell(&self, _event: Bell) -> CatgaResult<()> {
            Ok(())
        }
    }
}

mod event_only_service {
    use catga_core::CatgaResult;

    use crate::messages::{Bell, Log};

    #[derive(Clone, Default)]
    pub struct Notifier;

    #[catga_core::catga_service]
    impl Notifier {
        async fn archive(&self, _log: Log) -> CatgaResult<()> {
            Ok(())
        }

        async fn on_bell(&self, _event: Bell) -> CatgaResult<()> {
            Ok(())
        }
    }
}

use std::sync::atomic::Ordering;

use catga_core::CatgaResult;

use plain_service::{Calculator, Fallible};
use typed_service::Bank;

#[tokio::test]
async fn service_registry_dispatches_every_detected_kind() -> CatgaResult<()> {
    let calculator = Calculator::default();
    let bells = plain_service::bell_counter(&calculator);
    let mediator = catga_core::Mediator::new(calculator.registry()?);

    assert_eq!(mediator.send(messages::Ask { value: 41 }).await?, 42);
    assert_eq!(mediator.send(Fallible(5)).await?, 15);
    mediator.send_command(messages::Log).await?;
    mediator.publish(messages::Bell).await?;
    assert_eq!(bells.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn service_keeps_message_less_async_methods_callable() -> CatgaResult<()> {
    Calculator::default().ping_only().await
}

#[test]
fn service_expansion_preserves_non_async_members() {
    assert_eq!(Calculator::default().sync_helper(), 5);
    assert_eq!(Calculator::MAX_RETRIES, 3);
}

#[tokio::test]
async fn typed_mediator_dispatches_directly_to_service_methods() -> CatgaResult<()> {
    let mediator = typed_service::BankMediator::new(Bank::default());

    mediator.send_command(messages::Log).await?;
    assert_eq!(mediator.send(messages::Ask { value: 0 }).await?, 10);
    mediator.publish(messages::Bell).await?;

    let cloned = mediator.clone();
    assert_eq!(cloned.send(messages::Ask { value: 0 }).await?, 10);
    Ok(())
}

#[tokio::test]
async fn typed_mediator_services_also_build_dynamic_registries() -> CatgaResult<()> {
    let mediator = catga_core::Mediator::new(Bank::default().registry()?);

    mediator.send_command(messages::Log).await?;
    assert_eq!(mediator.send(messages::Ask { value: 0 }).await?, 10);
    Ok(())
}

#[tokio::test]
async fn services_without_request_methods_still_build_registries() -> CatgaResult<()> {
    let mediator = catga_core::Mediator::new(event_only_service::Notifier.registry()?);

    mediator.send_command(messages::Log).await?;
    mediator.publish(messages::Bell).await?;
    Ok(())
}
