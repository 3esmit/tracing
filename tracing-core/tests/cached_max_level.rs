use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing_core::{
    callsite::{self, DefaultCallsite, Identifier},
    dispatcher,
    field::FieldSet,
    metadata::LevelFilter,
    span, Dispatch, Event, Kind, Level, Metadata, Subscriber,
};

const LEVELS: [LevelFilter; 6] = [
    LevelFilter::OFF,
    LevelFilter::ERROR,
    LevelFilter::WARN,
    LevelFilter::INFO,
    LevelFilter::DEBUG,
    LevelFilter::TRACE,
];

struct Filter(Arc<AtomicUsize>);

impl Subscriber for Filter {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= LEVELS[self.0.load(Ordering::SeqCst)]
    }
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LEVELS[self.0.load(Ordering::SeqCst)])
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

static CALLSITE: DefaultCallsite = DefaultCallsite::new(&META);
static META: Metadata<'static> = Metadata::new(
    "cached level",
    "test",
    Level::WARN,
    None,
    None,
    None,
    FieldSet::new(&[], Identifier(&CALLSITE)),
    Kind::EVENT,
);

// Own process-wide max-level state so other tests cannot race these assertions.
#[test]
fn cached_max_level_round_trip() {
    let level = Arc::new(AtomicUsize::new(0));
    dispatcher::set_global_default(Dispatch::new(Filter(level.clone())))
        .expect("set global subscriber");
    CALLSITE.register();

    for index in (0..LEVELS.len()).chain((0..LEVELS.len()).rev()) {
        level.store(index, Ordering::SeqCst);
        callsite::rebuild_interest_cache();
        assert_eq!(LevelFilter::current(), LEVELS[index]);
        assert_eq!(
            CALLSITE.interest().is_always(),
            LEVELS[index] >= Level::WARN
        );
        assert_eq!(CALLSITE.interest().is_never(), LEVELS[index] < Level::WARN);
    }
}
