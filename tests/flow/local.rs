use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler, Mediator, Registry, Request,
    RequestClient,
};
use catga_flow::{
    DslFlow, DslFlowLifecycleEvent, DslFlowLifecycleHooks, DslFlowLifecycleObserver, Flow,
    FlowTagPolicy, FlowThrottle, dsl_action, dsl_each_action,
};
use futures::{StreamExt, stream};
use metrics::{
    Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default)]
struct FlowMetricRecorder {
    counters: Arc<Mutex<HashMap<String, u64>>>,
    histograms: Arc<Mutex<HashMap<String, usize>>>,
}

impl FlowMetricRecorder {
    fn counter(&self, name: &str) -> u64 {
        self.counters
            .lock()
            .expect("metric recorder lock")
            .get(name)
            .copied()
            .unwrap_or_default()
    }

    fn histogram_samples(&self, name: &str) -> usize {
        self.histograms
            .lock()
            .expect("metric recorder lock")
            .get(name)
            .copied()
            .unwrap_or_default()
    }
}

struct FlowCounter {
    name: String,
    values: Arc<Mutex<HashMap<String, u64>>>,
}

impl CounterFn for FlowCounter {
    fn increment(&self, value: u64) {
        *self
            .values
            .lock()
            .expect("metric recorder lock")
            .entry(self.name.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.values
            .lock()
            .expect("metric recorder lock")
            .insert(self.name.clone(), value);
    }
}

struct FlowHistogram {
    name: String,
    samples: Arc<Mutex<HashMap<String, usize>>>,
}

impl HistogramFn for FlowHistogram {
    fn record(&self, _: f64) {
        *self
            .samples
            .lock()
            .expect("metric recorder lock")
            .entry(self.name.clone())
            .or_default() += 1;
    }
}

impl Recorder for FlowMetricRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(FlowCounter {
            name: key.name().to_owned(),
            values: Arc::clone(&self.counters),
        }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(FlowHistogram {
            name: key.name().to_owned(),
            samples: Arc::clone(&self.histograms),
        }))
    }
}

#[tokio::test]
async fn local_flow_compensates_completed_steps_in_reverse_order() {
    let trace = Arc::new(AtomicUsize::new(0));
    let reserve = Arc::clone(&trace);
    let release = Arc::clone(&trace);
    let charge = Arc::clone(&trace);

    let result = Flow::new("reserve")
        .step(
            move || {
                let trace = Arc::clone(&reserve);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 0);
                    Ok(())
                }
            },
            move || {
                let trace = Arc::clone(&release);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 2);
                    Ok(())
                }
            },
        )
        .step(
            move || {
                let trace = Arc::clone(&charge);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 1);
                    Err(CatgaError::new(ErrorCode::Transient, "charge"))
                }
            },
            || async { Ok(()) },
        )
        .run()
        .await;

    assert!(!result.is_success());
    assert_eq!(result.error().unwrap().message(), "charge");
    assert_eq!(trace.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn local_flow_resumes_at_selected_step_and_bounds_compensation() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let result = Flow::new("restartable")
        .step(
            {
                let trace = Arc::clone(&trace);
                move || {
                    let trace = Arc::clone(&trace);
                    async move {
                        trace.lock().expect("trace lock").push("first");
                        Ok(())
                    }
                }
            },
            {
                let trace = Arc::clone(&trace);
                move || {
                    let trace = Arc::clone(&trace);
                    async move {
                        trace.lock().expect("trace lock").push("undo-first");
                        Ok(())
                    }
                }
            },
        )
        .step(
            {
                let trace = Arc::clone(&trace);
                move || {
                    let trace = Arc::clone(&trace);
                    async move {
                        trace.lock().expect("trace lock").push("second");
                        Ok(())
                    }
                }
            },
            {
                let trace = Arc::clone(&trace);
                move || {
                    let trace = Arc::clone(&trace);
                    async move {
                        trace.lock().expect("trace lock").push("undo-second");
                        Ok(())
                    }
                }
            },
        )
        .step(
            {
                let trace = Arc::clone(&trace);
                move || {
                    let trace = Arc::clone(&trace);
                    async move {
                        trace.lock().expect("trace lock").push("third");
                        Err(CatgaError::new(ErrorCode::Transient, "declined"))
                    }
                }
            },
            || async { Ok(()) },
        )
        .run_from(1, 1)
        .await;

    assert_eq!(result.completed_steps(), 2);
    assert_eq!(
        *trace.lock().expect("trace lock"),
        ["second", "third", "undo-second"]
    );
}

#[tokio::test]
async fn empty_local_flow_completes_successfully() {
    let result = Flow::new("empty").run().await;

    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 0);
}

#[derive(Default)]
struct RecordedDslLifecycle {
    events: Mutex<Vec<&'static str>>,
}

impl DslFlowLifecycleObserver for RecordedDslLifecycle {
    fn observe(&self, event: &DslFlowLifecycleEvent) {
        let name = match event {
            DslFlowLifecycleEvent::StepSucceeded { .. } => "step-succeeded",
            DslFlowLifecycleEvent::StepFailed { .. } => "step-failed",
            DslFlowLifecycleEvent::FlowSucceeded => "flow-succeeded",
            DslFlowLifecycleEvent::FlowFailed { .. } => "flow-failed",
        };
        self.events.lock().expect("lifecycle test lock").push(name);
    }
}

struct TracingDslLifecycleObserver {
    trace: Arc<Mutex<Vec<&'static str>>>,
}

impl DslFlowLifecycleObserver for TracingDslLifecycleObserver {
    fn observe(&self, event: &DslFlowLifecycleEvent) {
        let event = match event {
            DslFlowLifecycleEvent::StepSucceeded { .. } => "observer-step-succeeded",
            DslFlowLifecycleEvent::StepFailed { .. } => "observer-step-failed",
            DslFlowLifecycleEvent::FlowSucceeded => "observer-flow-succeeded",
            DslFlowLifecycleEvent::FlowFailed { .. } => "observer-flow-failed",
        };
        self.trace.lock().expect("lifecycle trace lock").push(event);
    }
}

#[tokio::test]
async fn dsl_flow_notifies_configured_lifecycle_observer_for_step_and_flow_outcomes() {
    let observer = Arc::new(RecordedDslLifecycle::default());
    let flow = DslFlow::new()
        .with_lifecycle_observer(Arc::clone(&observer))
        .action(dsl_action!(|state: &mut ()| async move {
            let _ = state;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut ()| async move {
            let _ = state;
            Err(CatgaError::new(ErrorCode::Validation, "declined"))
        }));

    let error = flow
        .run(&mut ())
        .await
        .expect_err("second step must fail the flow");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        *observer.events.lock().expect("lifecycle test lock"),
        ["step-succeeded", "step-failed", "flow-failed"]
    );
}

#[tokio::test]
async fn dsl_flow_awaits_step_succeeded_hook_after_synchronous_observer() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let observer = Arc::new(TracingDslLifecycleObserver {
        trace: Arc::clone(&trace),
    });
    let hook_trace = Arc::clone(&trace);
    let flow_hook_trace = Arc::clone(&trace);
    let action_trace = Arc::clone(&trace);
    let flow = DslFlow::new()
        .with_lifecycle_observer(observer)
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_succeeded(move |state: &u32, step_index| {
                    let hook_trace = Arc::clone(&hook_trace);
                    Box::pin(async move {
                        assert_eq!(step_index, 0);
                        assert_eq!(*state, 1);
                        hook_trace
                            .lock()
                            .expect("lifecycle trace lock")
                            .push("hook-step-succeeded");
                        Ok(())
                    })
                })
                .on_flow_succeeded(move |_: &u32| {
                    let flow_hook_trace = Arc::clone(&flow_hook_trace);
                    Box::pin(async move {
                        flow_hook_trace
                            .lock()
                            .expect("lifecycle trace lock")
                            .push("hook-flow-succeeded");
                        Ok(())
                    })
                }),
        )
        .action(move |state: &mut u32| {
            let action_trace = Arc::clone(&action_trace);
            Box::pin(async move {
                *state = 1;
                action_trace
                    .lock()
                    .expect("lifecycle trace lock")
                    .push("action");
                Ok(())
            })
        });

    flow.run(&mut 0).await.unwrap();

    assert_eq!(
        *trace.lock().expect("lifecycle trace lock"),
        [
            "action",
            "observer-step-succeeded",
            "hook-step-succeeded",
            "observer-flow-succeeded",
            "hook-flow-succeeded",
        ]
    );
}

#[tokio::test]
async fn dsl_flow_runs_failure_hooks_after_observers_in_order() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let observer = Arc::new(TracingDslLifecycleObserver {
        trace: Arc::clone(&trace),
    });
    let step_hook_trace = Arc::clone(&trace);
    let flow_hook_trace = Arc::clone(&trace);
    let flow_failed_hook_trace = Arc::clone(&trace);
    let first_action_trace = Arc::clone(&trace);
    let second_action_trace = Arc::clone(&trace);
    let flow = DslFlow::new()
        .with_lifecycle_observer(observer)
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_succeeded(move |_: &(), _| {
                    let trace = Arc::clone(&step_hook_trace);
                    Box::pin(async move {
                        trace
                            .lock()
                            .expect("lifecycle trace lock")
                            .push("hook-step-succeeded");
                        Ok(())
                    })
                })
                .on_step_failed(move |_: &(), _, _| {
                    let trace = Arc::clone(&flow_hook_trace);
                    Box::pin(async move {
                        trace
                            .lock()
                            .expect("lifecycle trace lock")
                            .push("hook-step-failed");
                        Ok(())
                    })
                })
                .on_flow_failed(move |_: &(), _| {
                    let trace = Arc::clone(&flow_failed_hook_trace);
                    Box::pin(async move {
                        trace
                            .lock()
                            .expect("lifecycle trace lock")
                            .push("hook-flow-failed");
                        Ok(())
                    })
                }),
        )
        .action(move |_: &mut ()| {
            let trace = Arc::clone(&first_action_trace);
            Box::pin(async move {
                trace.lock().expect("lifecycle trace lock").push("first");
                Ok(())
            })
        })
        .action(move |_: &mut ()| {
            let trace = Arc::clone(&second_action_trace);
            Box::pin(async move {
                trace.lock().expect("lifecycle trace lock").push("second");
                Err(CatgaError::new(ErrorCode::Validation, "declined"))
            })
        });

    assert_eq!(
        flow.run(&mut ()).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(
        *trace.lock().expect("lifecycle trace lock"),
        [
            "first",
            "observer-step-succeeded",
            "hook-step-succeeded",
            "second",
            "observer-step-failed",
            "hook-step-failed",
            "observer-flow-failed",
            "hook-flow-failed",
        ]
    );
}

#[tokio::test]
async fn dsl_flow_hook_error_short_circuits_following_steps_and_failure_events() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let hook_trace = Arc::clone(&trace);
    let second_action_trace = Arc::clone(&trace);
    let flow = DslFlow::new()
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_succeeded(move |_: &(), _| {
                    let trace = Arc::clone(&hook_trace);
                    Box::pin(async move {
                        trace.lock().expect("lifecycle trace lock").push("hook");
                        Err(CatgaError::new(ErrorCode::Unavailable, "hook unavailable"))
                    })
                })
                .on_flow_failed(move |_: &(), _| {
                    Box::pin(async move {
                        panic!("a succeeded hook failure must not become a flow failure")
                    })
                }),
        )
        .action(|_: &mut ()| Box::pin(async move { Ok(()) }))
        .action(move |_: &mut ()| {
            let trace = Arc::clone(&second_action_trace);
            Box::pin(async move {
                trace.lock().expect("lifecycle trace lock").push("second");
                Ok(())
            })
        });

    let error = flow.run(&mut ()).await.unwrap_err();

    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.message(), "hook unavailable");
    assert_eq!(*trace.lock().expect("lifecycle trace lock"), ["hook"]);
}

#[tokio::test]
async fn dsl_flow_failed_step_hook_error_skips_flow_failed_hook() {
    let flow = DslFlow::new()
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_failed(|_: &(), _, _| {
                    Box::pin(async move {
                        Err(CatgaError::new(ErrorCode::Unavailable, "hook unavailable"))
                    })
                })
                .on_flow_failed(|_: &(), _| {
                    Box::pin(async move {
                        panic!("a failed-step hook error must skip the flow-failed hook")
                    })
                }),
        )
        .action(|_: &mut ()| {
            Box::pin(async move { Err(CatgaError::new(ErrorCode::Validation, "declined")) })
        });

    let error = flow.run(&mut ()).await.unwrap_err();

    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(error.message(), "hook unavailable");
}

#[tokio::test]
async fn dsl_flow_emits_hooks_only_for_its_top_level_steps() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let step_hook_trace = Arc::clone(&trace);
    let flow_hook_trace = Arc::clone(&trace);
    let flow = DslFlow::new()
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_succeeded(move |_: &(), step_index| {
                    let trace = Arc::clone(&step_hook_trace);
                    Box::pin(async move {
                        trace
                            .lock()
                            .expect("lifecycle trace lock")
                            .push(match step_index {
                                0 => "top-level-step",
                                _ => "unexpected-step",
                            });
                        Ok(())
                    })
                })
                .on_flow_succeeded(move |_: &()| {
                    let trace = Arc::clone(&flow_hook_trace);
                    Box::pin(async move {
                        trace.lock().expect("lifecycle trace lock").push("flow");
                        Ok(())
                    })
                }),
        )
        .if_else(
            |_| true,
            DslFlow::new().action(|_: &mut ()| Box::pin(async move { Ok(()) })),
            DslFlow::new(),
        );

    flow.run(&mut ()).await.unwrap();

    assert_eq!(
        *trace.lock().expect("lifecycle trace lock"),
        ["top-level-step", "flow"]
    );
}

#[tokio::test]
async fn dsl_flow_runs_only_the_selected_nested_branch_against_one_state() {
    let mut state = Vec::new();
    let then_branch = DslFlow::new().action(|state: &mut Vec<&'static str>| {
        Box::pin(async move {
            state.push("then");
            Ok(())
        })
    });
    let else_branch = DslFlow::new().action(|state: &mut Vec<&'static str>| {
        Box::pin(async move {
            state.push("else");
            Ok(())
        })
    });
    let flow = DslFlow::new()
        .action(|state: &mut Vec<&'static str>| {
            Box::pin(async move {
                state.push("start");
                Ok(())
            })
        })
        .if_else(|state| state.len() == 1, then_branch, else_branch);

    flow.run(&mut state).await.unwrap();
    assert_eq!(state, ["start", "then"]);
}

#[tokio::test]
async fn dsl_flow_match_on_runs_the_selected_case_or_default_branch() {
    #[derive(Default)]
    struct State {
        mode: u8,
        trace: Vec<&'static str>,
    }

    let flow = DslFlow::new().match_on(
        |state: &State| state.mode,
        [
            (
                1,
                DslFlow::new().action(dsl_action!(|state: &mut State| async move {
                    state.trace.push("one");
                    Ok(())
                })),
            ),
            (
                2,
                DslFlow::new().action(dsl_action!(|state: &mut State| async move {
                    state.trace.push("two");
                    Ok(())
                })),
            ),
        ],
        DslFlow::new().action(dsl_action!(|state: &mut State| async move {
            state.trace.push("default");
            Ok(())
        })),
    );

    let mut selected = State {
        mode: 2,
        ..State::default()
    };
    flow.run(&mut selected).await.unwrap();
    assert_eq!(selected.trace, ["two"]);

    let mut unmatched = State {
        mode: 9,
        ..State::default()
    };
    flow.run(&mut unmatched).await.unwrap();
    assert_eq!(unmatched.trace, ["default"]);
}

#[derive(catga_core::Message, Deserialize, Serialize)]
struct Double(u32);

impl Request for Double {
    type Response = u32;
}

struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, message: Double) -> CatgaResult<u32> {
        Ok(message.0.saturating_mul(2))
    }
}

struct RemoteDoubleClient;

#[async_trait]
impl RequestClient<Double> for RemoteDoubleClient {
    async fn request(&self, request: &Double) -> CatgaResult<u32> {
        Ok(request.0.saturating_mul(2))
    }
}

#[derive(Clone, catga_core::Message)]
struct FlowPublished(u32);

impl Event for FlowPublished {}

struct PublishedValue(Arc<AtomicU32>);

#[async_trait]
impl EventHandler<FlowPublished> for PublishedValue {
    async fn handle(&self, event: FlowPublished) -> CatgaResult<()> {
        self.0.store(event.0, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn dsl_flow_sends_a_request_stores_its_response_and_publishes_an_event() {
    struct State {
        value: u32,
    }

    let published = Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Double, _>(DoubleHandler)
        .unwrap();
    registry.register_event::<FlowPublished, _>(PublishedValue(Arc::clone(&published)));
    let mediator = Arc::new(Mediator::new(registry));

    let flow = DslFlow::new()
        .send_into(
            Arc::clone(&mediator),
            |state: &State| Double(state.value),
            |state, response| state.value = response,
        )
        .publish(Arc::clone(&mediator), |state: &State| {
            FlowPublished(state.value)
        });
    let mut state = State { value: 21 };

    flow.run(&mut state).await.unwrap();

    assert_eq!(state.value, 42);
    assert_eq!(published.load(Ordering::Relaxed), 42);
}

#[tokio::test]
async fn dsl_flow_remote_send_stores_a_typed_remote_response() {
    struct State {
        value: u32,
    }

    let flow = DslFlow::new().remote_send_into(
        Arc::new(RemoteDoubleClient),
        |state: &State| Double(state.value),
        |state, response| state.value = response,
    );
    let mut state = State { value: 21 };

    flow.run(&mut state).await.unwrap();

    assert_eq!(state.value, 42);
}

#[tokio::test]
async fn dsl_flow_remote_send_discards_its_response_and_advances() {
    let mut advanced = false;

    DslFlow::new()
        .remote_send(Arc::new(RemoteDoubleClient), |_| Double(21))
        .action(dsl_action!(|advanced: &mut bool| async move {
            *advanced = true;
            Ok(())
        }))
        .run(&mut advanced)
        .await
        .unwrap();

    assert!(advanced);
}

#[tokio::test]
async fn dsl_flow_send_discards_its_response_and_advances_to_later_steps() {
    let mut registry = Registry::new();
    registry
        .register_request::<Double, _>(DoubleHandler)
        .unwrap();
    let mediator = Arc::new(Mediator::new(registry));
    let mut advanced = false;

    DslFlow::new()
        .send(Arc::clone(&mediator), |_| Double(21))
        .action(dsl_action!(|advanced: &mut bool| async move {
            *advanced = true;
            Ok(())
        }))
        .run(&mut advanced)
        .await
        .unwrap();

    assert!(advanced);
}

#[tokio::test]
async fn dsl_flow_stops_before_later_steps_after_a_branch_error() {
    let mut state = Vec::new();
    let failed = DslFlow::new().action(|_: &mut Vec<&'static str>| {
        Box::pin(async { Err(CatgaError::new(ErrorCode::Validation, "branch failed")) })
    });
    let flow = DslFlow::new()
        .if_else(|_| true, failed, DslFlow::new())
        .action(|state: &mut Vec<&'static str>| {
            Box::pin(async move {
                state.push("after");
                Ok(())
            })
        });

    assert_eq!(
        flow.run(&mut state).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert!(state.is_empty());
}

#[tokio::test]
async fn dsl_action_macro_hides_the_borrowed_future_boxing() {
    let mut value = 0_u32;
    DslFlow::new()
        .action(dsl_action!(|value: &mut u32| async move {
            *value += 1;
            Ok(())
        }))
        .run(&mut value)
        .await
        .unwrap();
    assert_eq!(value, 1);
}

#[derive(Clone, Debug)]
struct ParallelState {
    value: u32,
}

#[tokio::test]
async fn dsl_flow_parallel_runs_isolated_branches_concurrently_and_merges_in_definition_order() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let first = DslFlow::new().action(move |state: &mut ParallelState| {
        let barrier = Arc::clone(&first_barrier);
        Box::pin(async move {
            state.value = 10;
            barrier.wait().await;
            tokio::task::yield_now().await;
            Ok(())
        })
    });
    let second = DslFlow::new().action(move |state: &mut ParallelState| {
        let barrier = Arc::clone(&second_barrier);
        Box::pin(async move {
            state.value = 20;
            barrier.wait().await;
            Ok(())
        })
    });
    let flow = DslFlow::new().parallel([first, second], |state, branch_states| {
        state.value = branch_states
            .iter()
            .fold(0, |value, branch| value * 100 + branch.value);
        Ok(())
    });
    let mut state = ParallelState { value: 5 };

    tokio::time::timeout(std::time::Duration::from_secs(1), flow.run(&mut state))
        .await
        .expect("parallel branches must reach each other")
        .unwrap();

    assert_eq!(state.value, 1020);
}

#[tokio::test]
async fn dsl_flow_when_all_merges_every_completed_branch() {
    let flow = DslFlow::new().when_all(
        [
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                state.value = 4;
                Ok(())
            })),
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                state.value = 8;
                Ok(())
            })),
        ],
        |state, branches| {
            state.value = branches.iter().map(|branch| branch.value).sum();
            Ok(())
        },
    );
    let mut state = ParallelState { value: 0 };

    flow.run(&mut state).await.unwrap();

    assert_eq!(state.value, 12);
}

#[tokio::test]
async fn dsl_flow_parallel_keeps_the_original_state_when_a_branch_fails() {
    let merge_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                state.value = 10;
                Ok(())
            })),
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                let _ = state;
                Err(CatgaError::new(ErrorCode::Validation, "parallel failed"))
            })),
        ],
        {
            let merge_called = Arc::clone(&merge_called);
            move |_, _: Vec<ParallelState>| {
                merge_called.store(true, Ordering::Relaxed);
                Ok(())
            }
        },
    );
    let mut state = ParallelState { value: 5 };

    assert_eq!(
        flow.run(&mut state).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(state.value, 5);
    assert!(!merge_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn dsl_flow_when_any_commits_the_first_completed_branch_without_waiting_for_the_rest() {
    let flow = DslFlow::new().when_any(
        [
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                state.value = 42;
                Ok(())
            })),
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                let _ = state;
                std::future::pending::<()>().await;
                Ok(())
            })),
        ],
        |state, winner| {
            state.value = winner.value;
            Ok(())
        },
    );
    let mut state = ParallelState { value: 5 };

    tokio::time::timeout(std::time::Duration::from_secs(1), flow.run(&mut state))
        .await
        .expect("when_any must not wait for unfinished branches")
        .unwrap();

    assert_eq!(state.value, 42);
}

#[derive(Debug)]
struct BatchState {
    items: Vec<u32>,
    processed: Vec<u32>,
}

#[derive(Debug)]
struct BestEffortBatchState {
    items: Vec<u32>,
    processed: Vec<u32>,
    failures: Vec<(usize, ErrorCode)>,
}

#[tokio::test]
async fn dsl_flow_for_each_processes_selected_items_in_order_before_later_steps() {
    let flow = DslFlow::new()
        .for_each(
            |state: &BatchState| state.items.clone(),
            dsl_each_action!(|state: &mut BatchState, item: u32| async move {
                state.processed.push(item);
                Ok(())
            }),
        )
        .action(dsl_action!(|state: &mut BatchState| async move {
            state.processed.push(99);
            Ok(())
        }));
    let mut state = BatchState {
        items: vec![3, 5, 8],
        processed: Vec::new(),
    };

    flow.run(&mut state).await.unwrap();

    assert_eq!(state.processed, [3, 5, 8, 99]);
}

#[tokio::test]
async fn dsl_flow_for_each_stops_before_later_items_and_steps_after_an_error() {
    let flow = DslFlow::new()
        .for_each(
            |state: &BatchState| state.items.clone(),
            dsl_each_action!(|state: &mut BatchState, item: u32| async move {
                state.processed.push(item);
                if item == 5 {
                    return Err(CatgaError::new(ErrorCode::Validation, "bad item"));
                }
                Ok(())
            }),
        )
        .action(dsl_action!(|state: &mut BatchState| async move {
            state.processed.push(99);
            Ok(())
        }));
    let mut state = BatchState {
        items: vec![3, 5, 8],
        processed: Vec::new(),
    };

    assert_eq!(
        flow.run(&mut state).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(state.processed, [3, 5]);
}

#[tokio::test]
async fn dsl_flow_for_each_continue_on_error_records_failures_and_advances() {
    let flow = DslFlow::new()
        .for_each_continue_on_error(
            |state: &BestEffortBatchState| state.items.clone(),
            dsl_each_action!(|state: &mut BestEffortBatchState, item: u32| async move {
                state.processed.push(item);
                if item == 5 {
                    return Err(CatgaError::new(ErrorCode::Validation, "bad item"));
                }
                Ok(())
            }),
            |state, index, error| {
                Box::pin(async move {
                    state.failures.push((index, error.code()));
                    Ok(())
                })
            },
        )
        .action(dsl_action!(|state: &mut BestEffortBatchState| async move {
            state.processed.push(99);
            Ok(())
        }));
    let mut state = BestEffortBatchState {
        items: vec![3, 5, 8],
        processed: Vec::new(),
        failures: Vec::new(),
    };

    flow.run(&mut state).await.unwrap();

    assert_eq!(state.processed, [3, 5, 8, 99]);
    assert_eq!(state.failures, [(1, ErrorCode::Validation)]);
}

#[tokio::test]
async fn dsl_flow_for_each_stream_polls_items_lazily_and_stops_after_an_error() {
    let polls = Arc::new(AtomicUsize::new(0));
    let source_polls = Arc::clone(&polls);
    let flow = DslFlow::new().for_each_stream(
        move |_: &BatchState| {
            let polls = Arc::clone(&source_polls);
            stream::unfold(0_u32, move |item| {
                let polls = Arc::clone(&polls);
                async move {
                    if item == 3 {
                        None
                    } else {
                        polls.fetch_add(1, Ordering::Relaxed);
                        Some((item, item.saturating_add(1)))
                    }
                }
            })
            .boxed()
        },
        dsl_each_action!(|state: &mut BatchState, item: u32| async move {
            state.processed.push(item);
            if item == 1 {
                return Err(CatgaError::new(ErrorCode::Validation, "declined"));
            }
            Ok(())
        }),
    );
    let mut state = BatchState {
        items: Vec::new(),
        processed: Vec::new(),
    };

    assert_eq!(
        flow.run(&mut state)
            .await
            .expect_err("second item must fail")
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(state.processed, [0, 1]);
    assert_eq!(polls.load(Ordering::Relaxed), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn dsl_for_each_records_completed_items_and_durations_without_high_cardinality_labels() {
    let recorder = FlowMetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let flow = DslFlow::new().for_each(
        |_: &Vec<u8>| vec![2_u8, 4_u8],
        |state, item| {
            Box::pin(async move {
                state.push(item);
                Ok(())
            })
        },
    );
    let mut state = Vec::new();

    flow.run(&mut state).await.expect("each item succeeds");
    drop(guard);

    assert_eq!(state, [2, 4]);
    assert_eq!(recorder.counter("catga.flow.foreach.items.processed"), 2);
    assert_eq!(recorder.counter("catga.flow.foreach.items.failed"), 0);
    assert_eq!(
        recorder.histogram_samples("catga.flow.foreach.item.duration"),
        2
    );
    assert_eq!(recorder.histogram_samples("catga.flow.foreach.duration"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn dsl_for_each_records_a_failed_item_before_returning_its_original_error() {
    let recorder = FlowMetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let flow = DslFlow::new().for_each(
        |_: &Vec<u8>| vec![7_u8],
        |_: &mut Vec<u8>, _: u8| {
            Box::pin(async { Err(CatgaError::new(ErrorCode::Validation, "item is invalid")) })
        },
    );
    let mut state = Vec::new();

    let error = flow
        .run(&mut state)
        .await
        .expect_err("the item error must stop sequential foreach");
    drop(guard);

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "item is invalid");
    assert_eq!(recorder.counter("catga.flow.foreach.items.processed"), 0);
    assert_eq!(recorder.counter("catga.flow.foreach.items.failed"), 1);
    assert_eq!(
        recorder.histogram_samples("catga.flow.foreach.item.duration"),
        1
    );
    assert_eq!(recorder.histogram_samples("catga.flow.foreach.duration"), 1);
}

#[tokio::test]
async fn dsl_flow_for_each_concurrent_limits_work_and_merges_results_in_input_order() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let flow = DslFlow::new()
        .for_each_concurrent(
            2,
            |state: &BatchState| state.items.clone(),
            {
                let active = Arc::clone(&active);
                let maximum = Arc::clone(&maximum);
                move |_: &BatchState, item| {
                    let active = Arc::clone(&active);
                    let maximum = Arc::clone(&maximum);
                    Box::pin(async move {
                        let concurrent = active.fetch_add(1, Ordering::AcqRel) + 1;
                        maximum.fetch_max(concurrent, Ordering::AcqRel);
                        tokio::time::sleep(std::time::Duration::from_millis(u64::from(
                            9_u32.saturating_sub(item),
                        )))
                        .await;
                        active.fetch_sub(1, Ordering::AcqRel);
                        Ok(item * 10)
                    })
                }
            },
            |state, results| {
                state.processed = results;
                Ok(())
            },
        )
        .expect("positive concurrency is valid");
    let mut state = BatchState {
        items: vec![3, 5, 8],
        processed: Vec::new(),
    };

    flow.run(&mut state).await.unwrap();

    assert_eq!(maximum.load(Ordering::Acquire), 2);
    assert_eq!(state.processed, [30, 50, 80]);
}

#[tokio::test]
async fn dsl_flow_for_each_stream_concurrent_bounds_each_batch_and_reduces_in_input_order() {
    let started = Arc::new(AtomicUsize::new(0));
    let release_first_batch = Arc::new(AtomicBool::new(false));
    let first_batch_started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let flow = Arc::new(
        DslFlow::new()
            .for_each_stream_concurrent(
                4,
                |_: &Vec<u8>| stream::iter(0_u8..10).boxed(),
                {
                    let started = Arc::clone(&started);
                    let release_first_batch = Arc::clone(&release_first_batch);
                    let first_batch_started = Arc::clone(&first_batch_started);
                    let release = Arc::clone(&release);
                    move |_: &Vec<u8>, item| {
                        let started = Arc::clone(&started);
                        let release_first_batch = Arc::clone(&release_first_batch);
                        let first_batch_started = Arc::clone(&first_batch_started);
                        let release = Arc::clone(&release);
                        Box::pin(async move {
                            let position = started.fetch_add(1, Ordering::AcqRel);
                            if position < 4 {
                                if position == 3 {
                                    first_batch_started.notify_one();
                                }
                                while !release_first_batch.load(Ordering::Acquire) {
                                    release.notified().await;
                                }
                            }
                            Ok(item)
                        })
                    }
                },
                |state, item| {
                    state.push(item);
                    Ok(())
                },
            )
            .expect("positive concurrency is valid"),
    );

    let task = tokio::spawn({
        let flow = Arc::clone(&flow);
        async move {
            let mut state = Vec::new();
            flow.run(&mut state).await?;
            Ok::<_, CatgaError>(state)
        }
    });
    first_batch_started.notified().await;
    assert_eq!(started.load(Ordering::Acquire), 4);

    release_first_batch.store(true, Ordering::Release);
    release.notify_waiters();
    assert_eq!(task.await.unwrap().unwrap(), (0_u8..10).collect::<Vec<_>>());
}

#[tokio::test]
async fn dsl_flow_retry_replays_transient_actions_until_they_succeed() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let action_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new().retry(2, std::time::Duration::ZERO, move |state: &mut u32| {
        let attempts = Arc::clone(&action_attempts);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::Relaxed);
            if attempt < 2 {
                return Err(CatgaError::new(ErrorCode::Transient, "try again"));
            }
            *state = 42;
            Ok(())
        })
    });
    let mut state = 0;

    flow.run(&mut state).await.unwrap();

    assert_eq!(state, 42);
    assert_eq!(attempts.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn dsl_flow_timeout_cancels_an_overdue_action_with_a_structured_error() {
    let flow = DslFlow::new().timeout(std::time::Duration::from_millis(1), |state: &mut u32| {
        Box::pin(async move {
            let _ = state;
            std::future::pending::<()>().await;
            Ok(())
        })
    });
    let mut state = 0;

    assert_eq!(
        flow.run(&mut state).await.unwrap_err().code(),
        ErrorCode::Timeout
    );
}

#[tokio::test]
async fn flow_throttle_limits_concurrent_actions_across_reused_flows() {
    let throttle = FlowThrottle::new(1).unwrap();
    let entered = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let flow = Arc::new(DslFlow::new().throttle(throttle, {
        let entered = Arc::clone(&entered);
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        move |state: &mut u32| {
            let entered = Arc::clone(&entered);
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            Box::pin(async move {
                let position = entered.fetch_add(1, Ordering::Relaxed);
                if position == 0 {
                    started.notify_one();
                    release.notified().await;
                }
                *state = 1;
                Ok(())
            })
        }
    }));
    let first_flow = Arc::clone(&flow);
    let first = tokio::spawn(async move {
        let mut state = 0;
        first_flow.run(&mut state).await?;
        Ok::<_, CatgaError>(state)
    });
    started.notified().await;

    let second_flow = Arc::clone(&flow);
    let second = tokio::spawn(async move {
        let mut state = 0;
        second_flow.run(&mut state).await?;
        Ok::<_, CatgaError>(state)
    });
    tokio::task::yield_now().await;
    assert_eq!(entered.load(Ordering::Relaxed), 1);

    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), 1);
    assert_eq!(second.await.unwrap().unwrap(), 1);
    assert_eq!(entered.load(Ordering::Relaxed), 2);
}

#[test]
fn flow_tag_policy_uses_matching_rules_and_default_values_without_locks() {
    let policy = FlowTagPolicy::new(std::time::Duration::from_secs(30), 1)
        .with_timeout("payment", std::time::Duration::from_secs(5))
        .with_retries("payment", 3)
        .with_persist("payment");

    assert_eq!(
        policy.timeout_for("payment"),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(policy.retries_for("payment"), 3);
    assert!(policy.should_persist("payment"));
    assert_eq!(
        policy.timeout_for("other"),
        std::time::Duration::from_secs(30)
    );
    assert_eq!(policy.retries_for("other"), 1);
    assert!(!policy.should_persist("other"));
}
