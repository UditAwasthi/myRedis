use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tinyredis_core::{execute, Command, Database, ExpiryInfo};

fn prepare_db(keys: usize) -> Database {
    let mut db = Database::new();
    for i in 0..keys {
        db.set(format!("key:{i}"), format!("value:{i}")).unwrap();
    }
    db
}

fn bench_set(c: &mut Criterion) {
    c.bench_function("set", |b| {
        let mut db = Database::new();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("k:{i}");
            let val = format!("v:{i}");
            i += 1;
            black_box(db.set(key, val).unwrap());
        });
    });
}

fn bench_get(c: &mut Criterion) {
    let db = prepare_db(10_000);
    c.bench_function("get", |b| {
        b.iter(|| {
            black_box(db.get("key:5000").unwrap());
        });
    });
}

fn bench_del(c: &mut Criterion) {
    let mut group = c.benchmark_group("del");
    for size in [100usize, 1_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut db = prepare_db(size);
                let keys: Vec<String> = (0..size).map(|i| format!("key:{i}")).collect();
                black_box(db.del(&keys));
            });
        });
    }
    group.finish();
}

fn bench_incr(c: &mut Criterion) {
    let mut db = Database::new();
    db.set("counter".into(), "0".into()).unwrap();
    c.bench_function("incr", |b| {
        b.iter(|| {
            let result = execute(&mut db, &Command::Incr("counter".into()));
            black_box(result);
        });
    });
}

fn bench_lrange(c: &mut Criterion) {
    let mut db = Database::new();
    let values: Vec<String> = (0..1_000).map(|i| format!("item:{i}")).collect();
    execute(
        &mut db,
        &Command::Lpush {
            key: "list".into(),
            values,
        },
    );
    c.bench_function("lrange", |b| {
        b.iter(|| {
            let result = execute(
                &mut db,
                &Command::Lrange {
                    key: "list".into(),
                    start: 0,
                    stop: 99,
                },
            );
            black_box(result);
        });
    });
}

fn bench_hash(c: &mut Criterion) {
    let mut db = Database::new();
    c.bench_function("hset", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let result = execute(
                &mut db,
                &Command::Hset {
                    key: "hash".into(),
                    field: format!("f:{i}"),
                    value: format!("v:{i}"),
                },
            );
            black_box(result);
        });
    });

    c.bench_function("hget", |b| {
        b.iter(|| {
            let result = execute(
                &mut db,
                &Command::Hget {
                    key: "hash".into(),
                    field: "f:500".into(),
                },
            );
            black_box(result);
        });
    });
}

fn bench_expiration(c: &mut Criterion) {
    let mut db = Database::new();
    db.set_with_expiry(
        "temp".into(),
        "value".into(),
        Some(ExpiryInfo::from_ttl(Duration::from_secs(3600))),
    )
    .unwrap();
    c.bench_function("ttl", |b| {
        b.iter(|| black_box(db.ttl("temp").unwrap()));
    });
    c.bench_function("expire_active_sample", |b| {
        b.iter(|| black_box(db.expire_active_sample()));
    });
}

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_del,
    bench_incr,
    bench_lrange,
    bench_hash,
    bench_expiration
);
criterion_main!(benches);
