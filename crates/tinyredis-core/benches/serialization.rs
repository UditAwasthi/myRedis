use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tinyredis_core::Database;

fn bench_snapshot_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_serialize");
    for key_count in [100usize, 1_000, 5_000] {
        let mut db = Database::new();
        for i in 0..key_count {
            db.set(format!("key:{i}"), format!("value:{i}")).unwrap();
        }
        group.bench_with_input(BenchmarkId::from_parameter(key_count), &db, |b, db| {
            b.iter(|| {
                let data = db.snapshot_data();
                let json = serde_json::to_vec(&data).unwrap();
                black_box(json);
            });
        });
    }
    group.finish();
}

fn bench_snapshot_deserialize(c: &mut Criterion) {
    let mut db = Database::new();
    for i in 0..1_000 {
        db.set(format!("key:{i}"), format!("value:{i}")).unwrap();
    }
    let json = serde_json::to_vec(&db.snapshot_data()).unwrap();

    c.bench_function("snapshot_deserialize", |b| {
        b.iter(|| {
            let data: std::collections::HashMap<
                String,
                (tinyredis_core::Value, Option<tinyredis_core::ExpiryInfo>),
            > = serde_json::from_slice(&json).unwrap();
            black_box(data);
        });
    });
}

criterion_group!(
    benches,
    bench_snapshot_serialize,
    bench_snapshot_deserialize
);
criterion_main!(benches);
