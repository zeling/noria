use criterion::{criterion_group, criterion_main, Criterion};
use dataflow::prelude::*;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut state_default = dataflow::bench::setup_bench_state(PersistenceParameters::default());
    let mut state_with_user_shard = dataflow::bench::setup_bench_state(PersistenceParameters {
        user_column: Some(1),
        ..PersistenceParameters::default()
    });
    let mut group = c.benchmark_group("with_shard");
    group.bench_function("with_user_shard", |b| {
        b.iter(|| {
            dataflow::bench::bench_state_process_record(
                &mut state_with_user_shard,
                vec![1.into(), "alice".into(), 3.into()],
                true,
            );
            dataflow::bench::bench_state_process_record(
                &mut state_with_user_shard,
                vec![1.into(), "alice".into(), 3.into()],
                false,
            );
        })
    });
    group.bench_function("without_shard", |b| {
        b.iter(|| {
            dataflow::bench::bench_state_process_record(
                &mut state_default,
                vec![1.into(), "alice".into(), 3.into()],
                true,
            );
            dataflow::bench::bench_state_process_record(
                &mut state_default,
                vec![1.into(), "alice".into(), 3.into()],
                false,
            );
        })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
