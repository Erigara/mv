use std::collections::BTreeMap;

use storage::Storage;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

const KEYS_IN_STORE: [u64; 5] = [100, 1_000, 10_000, 100_000, 1_000_000];

fn rollback_btree(c: &mut Criterion) {
    let mut btree_group = c.benchmark_group("rollback_btree");

    for n in KEYS_IN_STORE {
        let mut btree = BTreeMap::<u64, u64>::new();

        // Load key values into btree represent past state
        for i in 0..n {
            btree.insert(i, i);
        }

        btree_group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                let mut btree_backup = black_box(&btree).clone();

                for i in 0..10 {
                    btree.insert(i, i);
                }

                core::mem::swap(&mut btree, &mut btree_backup)
            })
        });
    }

    btree_group.finish();
}

fn rollback_storage(c: &mut Criterion) {
    let mut storage_group = c.benchmark_group("rollback_storage");

    for n in KEYS_IN_STORE {
        let mut storage = Storage::<u64, u64>::new();

        // Load key values into and storage to represent past state
        {
            let mut transaction = storage.block(false);
            for i in 0..n {
                transaction.insert(i, i);
            }
            transaction.commit();
        }

        storage_group.bench_function(BenchmarkId::from_parameter(n), |b| {
            {
                let mut transaction = storage.block(true);
                for i in 0..10 {
                    transaction.insert(i, i);
                }
                transaction.commit();
            }
            b.iter(|| {
                let storage = black_box(&mut storage);

                {
                    let mut transaction = storage.block(true);
                    for i in 0..10 {
                        transaction.insert(i, i);
                    }
                    transaction.commit();
                }
            })
        });
    }

    storage_group.finish();
}

criterion_group!(benches, rollback_btree, rollback_storage);
criterion_main!(benches);
