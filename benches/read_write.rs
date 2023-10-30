use std::collections::BTreeMap;

use storage::Storage;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

const KEYS_IN_STORE: u64 = 1_000_000;
const TRANSACTIONS: [u64; 3] = [1, 10, 100];

fn fill_btree() -> BTreeMap<u64, u64> {
    let mut btree = BTreeMap::<u64, u64>::new();
    let mut c = 0;

    for _ in 0..KEYS_IN_STORE {
        btree.insert(c, c);
        c += 1;
    }

    btree
}

fn fill_storage(transactions: u64) -> Storage<u64, u64> {
    let storage = Storage::<u64, u64>::new();
    let mut c = 0;
    let keys_per_transaction = KEYS_IN_STORE / transactions;

    for _ in 0..transactions {
        let mut transaction = storage.block(false);
        for _ in 0..keys_per_transaction {
            transaction.insert(c, c);
            c += 1;
        }
        transaction.commit();
    }

    storage
}

fn write_btree(c: &mut Criterion) {
    c.bench_function("write_btree", |b| b.iter_with_large_drop(|| fill_btree()));
}

fn read_btree(c: &mut Criterion) {
    let btree = fill_btree();
    c.bench_function("read_btree", |b| {
        b.iter(|| {
            let btree = black_box(&btree);
            for key in 0..KEYS_IN_STORE {
                assert_eq!(btree.get(&key).copied(), Some(key));
            }
        })
    });
}

fn iter_btree(c: &mut Criterion) {
    let btree = fill_btree();
    c.bench_function("iter_btree", |b| {
        b.iter(|| {
            let btree = black_box(&btree);
            for (key, value) in btree {
                assert_eq!(key, value);
            }
        })
    });
}

fn write_storage(c: &mut Criterion) {
    let mut storage_group = c.benchmark_group("write_storage");

    for transactions in TRANSACTIONS {
        storage_group.bench_with_input(
            BenchmarkId::from_parameter(transactions),
            &transactions,
            |b, &transactions| b.iter_with_large_drop(|| fill_storage(transactions)),
        );
    }

    storage_group.finish();
}

fn read_storage(c: &mut Criterion) {
    let mut storage_group = c.benchmark_group("read_storage");

    for transactions in TRANSACTIONS {
        let storage = fill_storage(transactions);
        storage_group.bench_function(BenchmarkId::from_parameter(transactions), |b| {
            b.iter(|| {
                let storage = black_box(&storage);
                let view = storage.view();
                for key in 0..KEYS_IN_STORE {
                    assert_eq!(view.get(&key).as_deref().copied(), Some(key));
                }
            })
        });
    }

    storage_group.finish();
}

fn iter_storage(c: &mut Criterion) {
    let mut storage_group = c.benchmark_group("iter_storage");

    for transactions in TRANSACTIONS {
        let storage = fill_storage(transactions);
        storage_group.bench_function(BenchmarkId::from_parameter(transactions), |b| {
            b.iter(|| {
                let storage = black_box(&storage);
                let view = storage.view();
                for (key, value) in view.iter() {
                    assert_eq!(key, value);
                }
            })
        });
    }

    storage_group.finish();
}

criterion_group!(
    benches,
    write_btree,
    read_btree,
    iter_btree,
    write_storage,
    read_storage,
    iter_storage,
);
criterion_main!(benches);
