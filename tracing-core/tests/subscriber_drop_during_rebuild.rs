#![cfg(feature = "std")]

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Barrier, Mutex,
    },
    thread,
};
use tracing_core::{callsite, metadata::LevelFilter, span, Dispatch, Event, Metadata, Subscriber};

struct ReentrantDrop {
    owner: Arc<Mutex<Option<Dispatch>>>,
    dropped: Arc<AtomicBool>,
}

// Unlike NoSubscriber, this supplies an explicit OFF hint rather than None
// (which conservatively keeps the global maximum at TRACE).
struct Quiet;

impl Subscriber for Quiet {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::OFF)
    }
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

impl Subscriber for ReentrantDrop {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        // The rebuilder has upgraded its weak registration. Drop the only
        // external owner now, making that temporary upgrade the last owner.
        drop(self.owner.lock().expect("owner lock").take());
        Some(LevelFilter::TRACE)
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        // This takes the same registry read lock as registering a new event
        // callsite in a subscriber's destructor. It must not deadlock.
        callsite::rebuild_interest_cache();
        // Also exercise write re-entry, which would deadlock even if the
        // outer rebuilder held only a read guard.
        drop(Dispatch::new(Quiet));
        self.dropped.store(true, Ordering::SeqCst);
    }
}

// Keep this in its own test binary: it deliberately changes the process-wide
// dispatcher registry. The CI test runner bounds deadlock regressions.
#[test]
fn subscriber_drop_during_interest_rebuild() {
    let other = Dispatch::new(Quiet);

    for register_new_dispatch in [true, false] {
        let dropped = exercise_rebuild(register_new_dispatch);
        assert!(
            dropped.load(Ordering::SeqCst),
            "subscriber was retained after rebuilding"
        );
        assert_eq!(LevelFilter::current(), LevelFilter::OFF);
        // A subsequent registration must safely discard the expired weak entry.
        drop(Dispatch::new(Quiet));
    }

    // Contend on registration and rebuilding without sleeps or detached tasks.
    // Other rebuilders can legitimately retain a subscriber until they finish;
    // assert destruction after joining every owner rather than inside a worker.
    let start = Arc::new(Barrier::new(4));
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                (0..100)
                    .map(|index| exercise_rebuild(index % 2 == 0))
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let dropped: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().expect("rebuild worker panicked"))
        .collect();
    assert!(dropped.iter().all(|flag| flag.load(Ordering::SeqCst)));
    callsite::rebuild_interest_cache();
    assert_eq!(LevelFilter::current(), LevelFilter::OFF);
    drop(other);
}

fn exercise_rebuild(register_new_dispatch: bool) -> Arc<AtomicBool> {
    let owner = Arc::new(Mutex::new(None));
    let dropped = Arc::new(AtomicBool::new(false));
    let dispatch = Dispatch::new(ReentrantDrop {
        owner: owner.clone(),
        dropped: dropped.clone(),
    });
    *owner.lock().expect("owner lock") = Some(dispatch);

    if register_new_dispatch {
        // Registration rebuilds while holding the registry write lock.
        drop(Dispatch::new(Quiet));
    } else {
        callsite::rebuild_interest_cache();
    }
    assert!(owner.lock().expect("owner lock").is_none());
    dropped
}
