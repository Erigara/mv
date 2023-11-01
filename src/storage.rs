use std::{borrow::Borrow, collections::BTreeMap, ops::RangeBounds};

use concread::{
    bptree::{BptreeMap, BptreeMapReadTxn, BptreeMapWriteTxn},
    ebrcell::{EbrCell, EbrCellWriteTxn},
};

use crate::{Key, Value};

/// Multi-version key value storage
pub struct Storage<K: Key, V: Value> {
    pub(crate) rollback: EbrCell<BTreeMap<K, Option<V>>>,
    pub(crate) blocks: BptreeMap<K, V>,
}

impl<K: Key, V: Value> Storage<K, V> {
    /// Construct new [`Self`]
    pub fn new() -> Self {
        Self {
            rollback: EbrCell::new(BTreeMap::new()),
            blocks: BptreeMap::new(),
        }
    }

    /// Create persistent view of storage at certain point in time
    pub fn view(&self) -> View<'_, K, V> {
        View {
            blocks: self.blocks.read(),
        }
    }

    /// Create block to aggregate updates
    pub fn block(&self, rollback_latest_block: bool) -> Block<'_, K, V> {
        let mut rollback = self.rollback.write();
        let mut blocks = self.blocks.write();

        {
            let rollback = core::mem::take(rollback.get_mut());
            // Revert changes in case of rollback
            if rollback_latest_block {
                for (key, value) in rollback {
                    match value {
                        None => blocks.remove(&key),
                        Some(value) => blocks.insert(key, value),
                    };
                }
            }
        }

        Block { rollback, blocks }
    }
}

impl<K: Key, V: Value> Default for Storage<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key, V: Value> FromIterator<(K, V)> for Storage<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self {
            rollback: EbrCell::new(BTreeMap::new()),
            blocks: iter.into_iter().collect(),
        }
    }
}

/// Module for [`View`] and it's related impls
mod view {
    use super::*;
    /// Consistent view of the storage at the certain version
    pub struct View<'storage, K: Key, V: Value> {
        pub(crate) blocks: BptreeMapReadTxn<'storage, K, V>,
    }

    impl<K: Key, V: Value> View<'_, K, V> {
        /// Read entry from the list up to certain version non-inclusive
        pub fn get<'slf, Q>(&'slf self, key: &'slf Q) -> Option<&'slf V>
        where
            K: Ord + Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.blocks.get(key)
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            self.blocks.iter()
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn range<Q>(&self, bounds: impl RangeBounds<Q>) -> impl Iterator<Item = (&K, &V)>
        where
            K: Borrow<Q>,
            Q: Ord,
        {
            self.blocks.range(bounds)
        }
    }
}
pub use view::View;

/// Module for [`Block`] and it's related impls
mod block {
    use super::*;

    /// Batched update to the storage that can be reverted later
    pub struct Block<'store, K: Key, V: Value> {
        pub(crate) rollback: EbrCellWriteTxn<'store, BTreeMap<K, Option<V>>>,
        pub(crate) blocks: BptreeMapWriteTxn<'store, K, V>,
    }

    impl<'store, K: Key, V: Value> Block<'store, K, V> {
        /// Create transaction for the block
        pub fn transaction<'block>(&'block mut self) -> Transaction<'block, 'store, K, V>
        where
            'store: 'block,
        {
            Transaction {
                block: self,
                rollback: BTreeMap::new(),
            }
        }

        /// Apply aggregated changes to the storage
        pub fn commit(self) {
            // Commit fields in the inverse order
            self.blocks.commit();
            self.rollback.commit();
        }

        /// Get mutable access to the value stored in
        pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
            self.blocks.get_mut(key).map(|value| {
                self.rollback
                    .entry(key.clone())
                    .or_insert_with(|| Some(value.clone()));
                value
            })
        }

        /// Insert key value into the storage
        pub fn insert(&mut self, key: K, value: V) {
            let prev_value = self.blocks.insert(key.clone(), value);
            self.rollback.entry(key).or_insert(prev_value);
        }

        /// Remove key value from storage
        pub fn remove(&mut self, key: K) {
            let prev_value = self.blocks.remove(&key);
            self.rollback.entry(key).or_insert(prev_value);
        }

        /// Read entry from the storage up to certain version non-inclusive
        pub fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.blocks.get(key)
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            self.blocks.iter()
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn range<Q, R>(&self, bounds: R) -> impl Iterator<Item = (&K, &V)>
        where
            K: Borrow<Q>,
            Q: Ord,
            R: RangeBounds<Q>,
        {
            self.blocks.range(bounds)
        }
    }

    /// Part of block's aggregated changes which applied or aborted at the same time
    pub struct Transaction<'block, 'store, K: Key, V: Value> {
        pub(crate) rollback: BTreeMap<K, Option<V>>,
        pub(crate) block: &'block mut Block<'store, K, V>,
    }

    impl<'block, 'store: 'block, K: Key, V: Value> Transaction<'block, 'store, K, V> {
        /// Apply aggregated changes of [`Transaction`] to the [`Block`]
        pub fn apply(mut self) {
            for (key, value) in core::mem::take(&mut self.rollback) {
                self.block.rollback.entry(key).or_insert(value);
            }
        }
        /// Get mutable access to the value stored in
        pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
            self.block.blocks.get_mut(key).map(|value| {
                self.rollback
                    .entry(key.clone())
                    .or_insert_with(|| Some(value.clone()));
                value
            })
        }

        /// Insert key value into the transaction temporary map
        pub fn insert(&mut self, key: K, value: V) {
            let prev_value = self.block.blocks.insert(key.clone(), value);
            self.rollback.entry(key).or_insert(prev_value);
        }

        /// Remove key value from storage
        pub fn remove(&mut self, key: K) {
            let prev_value = self.block.blocks.remove(&key);
            self.rollback.entry(key).or_insert(prev_value);
        }

        /// Read entry from the storage up to certain version non-inclusive
        pub fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.block.get(key)
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            self.block.iter()
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn range<Q, R>(&self, bounds: R) -> impl Iterator<Item = (&K, &V)>
        where
            K: Borrow<Q>,
            Q: Ord,
            R: RangeBounds<Q>,
        {
            self.block.range(bounds)
        }
    }

    impl<'block, 'store: 'block, K: Key, V: Value> Drop for Transaction<'block, 'store, K, V> {
        fn drop(&mut self) {
            // revert changes made so fur by current transaction
            // if transaction was applied set would be empty
            for (key, value) in core::mem::take(&mut self.rollback) {
                match value {
                    None => self.block.blocks.remove(&key),
                    Some(value) => self.block.blocks.insert(key, value),
                };
            }
        }
    }
}
pub use block::{Block, Transaction};

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        ops::Bound,
    };

    use super::*;

    use proptest::proptest;

    #[test]
    fn get() {
        let storage = Storage::<u64, u64>::new();

        let view0 = storage.view();

        {
            let mut block = storage.block(false);
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view1 = storage.view();

        {
            let mut block = storage.block(false);
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view2 = storage.view();

        {
            let mut block = storage.block(false);
            for (key, value) in [(1, 2), (4, 2)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view3 = storage.view();

        assert_eq!(view0.get(&0), None);
        assert_eq!(view0.get(&1), None);
        assert_eq!(view0.get(&2), None);
        assert_eq!(view0.get(&3), None);

        assert_eq!(view1.get(&0), Some(&0));
        assert_eq!(view1.get(&1), Some(&0));
        assert_eq!(view1.get(&2), Some(&0));
        assert_eq!(view1.get(&3), None);

        assert_eq!(view2.get(&0), Some(&1));
        assert_eq!(view2.get(&1), Some(&1));
        assert_eq!(view2.get(&2), Some(&0));
        assert_eq!(view2.get(&3), Some(&1));
        assert_eq!(view2.get(&4), None);

        assert_eq!(view3.get(&0), Some(&1));
        assert_eq!(view3.get(&1), Some(&2));
        assert_eq!(view3.get(&2), Some(&0));
        assert_eq!(view3.get(&3), Some(&1));
        assert_eq!(view3.get(&4), Some(&2));
    }

    #[test]
    fn transaction_step() {
        let storage = Storage::<u64, u64>::new();

        let mut block = storage.block(false);

        // Successful transaction
        {
            let mut transaction = block.transaction();
            transaction.insert(0, 0);
            transaction.apply();
        }

        // Aborted step
        {
            let mut transaction = block.transaction();
            transaction.insert(0, 1);
            transaction.insert(1, 1);
        }

        // Check that aborted transaction changes don't visible for subsequent transactions
        {
            let transaction = block.transaction();
            assert_eq!(transaction.get(&0).as_deref().copied(), Some(0));
            assert_eq!(transaction.get(&1).as_deref().copied(), None);
        }

        block.commit();

        // Check that effect of aborted step is not visible in the storage after committing transaction
        {
            let view = storage.view();
            assert_eq!(view.get(&0).as_deref().copied(), Some(0));
            assert_eq!(view.get(&1).as_deref().copied(), None);
        }
    }

    #[test]
    fn iter() {
        let storage = Storage::<u64, u64>::new();

        {
            let mut block = storage.block(false);
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block(false);
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block(false);
            for (key, value) in [(1, 2), (4, 2)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view = storage.view();

        for (kv_actual, kv_expected) in
            view.iter()
                .zip([(&0, &1), (&1, &2), (&2, &0), (&3, &1), (&4, &2)])
        {
            assert_eq!(kv_actual, kv_expected);
        }

        let mut block = storage.block(false);
        block.insert(0, 3);
        block.insert(5, 3);

        let mut transaction = block.transaction();
        transaction.insert(1, 4);
        transaction.insert(6, 4);

        for (kv_actual, kv_expected) in transaction.iter().zip([
            (&0, &3),
            (&1, &4),
            (&2, &0),
            (&3, &1),
            (&4, &2),
            (&5, &3),
            (&6, &4),
        ]) {
            assert_eq!(kv_actual, kv_expected);
        }
    }

    #[test]
    fn range() {
        let storage = Storage::<u64, u64>::new();

        {
            let mut block = storage.block(false);
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block(false);
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block(false);
            for (key, value) in [(1, 2), (4, 2)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view = storage.view();

        for (kv_actual, kv_expected) in view
            .range((Bound::<u64>::Unbounded, Bound::Unbounded))
            .zip([(&0, &1), (&1, &2), (&2, &0), (&3, &1), (&4, &2)])
        {
            assert_eq!(kv_actual, kv_expected);
        }

        for (kv_actual, kv_expected) in view
            .range((Bound::Included(&1), Bound::Included(&3)))
            .zip([(&1, &2), (&2, &0), (&3, &1)])
        {
            assert_eq!(kv_actual, kv_expected);
        }

        for (kv_actual, kv_expected) in view
            .range((Bound::Excluded(&1), Bound::Excluded(&3)))
            .zip([(&2, &0)])
        {
            assert_eq!(kv_actual, kv_expected);
        }

        let mut block = storage.block(false);
        block.insert(0, 3);
        block.insert(5, 3);

        let mut transaction = block.transaction();
        transaction.insert(1, 4);
        transaction.insert(6, 4);

        for (kv_actual, kv_expected) in transaction
            .range((Bound::<u64>::Unbounded, Bound::Unbounded))
            .zip([
                (&0, &3),
                (&1, &4),
                (&2, &0),
                (&3, &1),
                (&4, &2),
                (&5, &3),
                (&6, &4),
            ])
        {
            assert_eq!(kv_actual, kv_expected);
        }

        for (kv_actual, kv_expected) in transaction
            .range((Bound::Included(&1), Bound::Included(&3)))
            .zip([(&1, &4), (&2, &0), (&3, &1)])
        {
            assert_eq!(kv_actual, kv_expected);
        }

        for (kv_actual, kv_expected) in transaction
            .range((Bound::Excluded(&1), Bound::Excluded(&3)))
            .zip([(&2, &0)])
        {
            assert_eq!(kv_actual, kv_expected);
        }
    }

    #[test]
    fn rollback() {
        let storage = Storage::<u64, u64>::new();

        {
            let mut block = storage.block(false);
            block.insert(0, 0);
            block.commit()
        }

        {
            let mut block = storage.block(false);
            block.insert(0, 1);
            block.commit()
        }

        let view1 = storage.view();

        {
            let block = storage.block(true);
            block.commit();
        }
        let view2 = storage.view();

        // View is persistent so revert is not visible
        assert_eq!(view1.get(&0), Some(&1));
        // Revert is visible in the view created after revert was applied
        assert_eq!(view2.get(&0), Some(&0));
    }

    proptest! {
        #[test]
        fn consistent_with_btreemap(txs: Vec<(bool, Vec<(u64, Option<u64>)>)>) {
            let storage = Storage::<u64, u64>::new();
            let mut map = BTreeMap::new();
            let mut keys = BTreeSet::new();

            for (committed, tx) in txs {
                let mut block = storage.block(false);

                for (key, value) in tx {
                    keys.insert(key);
                    match value {
                        Some(value) => {
                            if committed {
                                map.insert(key, value);
                            }
                            block.insert(key, value);
                        }
                        None => {
                            if committed {
                                map.remove(&key);
                            }
                            block.remove(key);
                        }
                    }
                }

                if committed {
                    block.commit()
                }
            }

            let view = storage.view();

            for key in keys.iter() {
                assert_eq!(
                    view.get(key),
                    map.get(key),
                );
            }
        }
    }
}
