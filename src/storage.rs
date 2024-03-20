use std::{borrow::Borrow, collections::BTreeMap, ops::RangeBounds};

use concread::{
    bptree::{BptreeMap, BptreeMapReadTxn, BptreeMapWriteTxn},
    ebrcell::{EbrCell, EbrCellWriteTxn},
};

use crate::{Key, Value};

/// Multi-version key value storage
pub struct Storage<K: Key, V: Value> {
    /// Previous version of values in the `blocks` map, required to perform revert of the latest changes
    pub(crate) revert: EbrCell<BTreeMap<K, Option<V>>>,
    /// Map which represent aggregated changes of multiple blocks
    pub(crate) blocks: BptreeMap<K, V>,
}

impl<K: Key, V: Value> Storage<K, V> {
    /// Construct new [`Self`]
    pub fn new() -> Self {
        Self {
            revert: EbrCell::new(BTreeMap::new()),
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
    pub fn block(&self) -> Block<'_, K, V> {
        let mut revert = self.revert.write();
        let blocks = self.blocks.write();

        // Clear revert
        revert.get_mut().clear();

        Block { revert, blocks }
    }

    /// Create block to aggregate updates and revert changes created in the latest block
    pub fn block_and_revert(&self) -> Block<'_, K, V> {
        let mut revert = self.revert.write();
        let mut blocks = self.blocks.write();

        {
            let revert = core::mem::take(revert.get_mut());
            for (key, value) in revert {
                match value {
                    None => blocks.remove(&key),
                    Some(value) => blocks.insert(key, value),
                };
            }
        }

        Block { revert, blocks }
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
            revert: EbrCell::new(BTreeMap::new()),
            blocks: iter.into_iter().collect(),
        }
    }
}

pub trait StorageReadOnly<K: Key, V: Value> {
    /// Read entry from the storage
    fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Ord + Borrow<Q>,
        Q: Ord + ?Sized;

    /// Iterate over all entries in the storage
    fn iter(&self) -> Iter<'_, K, V>;

    /// Iterate over range of entries in the storage
    fn range<Q>(&self, bounds: impl RangeBounds<Q>) -> RangeIter<'_, K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Get amount of entries in the storage
    fn len(&self) -> usize;
}

/// Module for [`View`] and it's related impls
mod view {
    use super::*;
    /// Consistent view of the storage at the certain version
    pub struct View<'storage, K: Key, V: Value> {
        pub(crate) blocks: BptreeMapReadTxn<'storage, K, V>,
    }

    impl<K: Key, V: Value> StorageReadOnly<K, V> for View<'_, K, V> {
        fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Ord + Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.blocks.get(key)
        }

        fn iter(&self) -> Iter<'_, K, V> {
            Iter {
                iter: self.blocks.iter(),
            }
        }

        fn range<Q>(&self, bounds: impl RangeBounds<Q>) -> RangeIter<'_, K, V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            RangeIter {
                iter: self.blocks.range(bounds),
            }
        }

        /// Get amount of entries in the storage
        fn len(&self) -> usize {
            self.blocks.len()
        }
    }
}
pub use view::View;

/// Module for [`Block`] and it's related impls
mod block {
    use super::*;

    /// Batched update to the storage that can be reverted later
    pub struct Block<'store, K: Key, V: Value> {
        pub(crate) revert: EbrCellWriteTxn<'store, BTreeMap<K, Option<V>>>,
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
                revert: BTreeMap::new(),
            }
        }

        /// Apply aggregated changes to the storage
        pub fn commit(self) {
            // Commit fields in the inverse order
            self.blocks.commit();
            self.revert.commit();
        }

        /// Get mutable access to the value stored in
        pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
            self.blocks.get_mut(key).map(|value| {
                self.revert
                    .entry(key.clone())
                    .or_insert_with(|| Some(value.clone()));
                value
            })
        }

        /// Insert key value into the storage
        pub fn insert(&mut self, key: K, value: V) -> Option<V> {
            let prev_value = self.blocks.insert(key.clone(), value);
            self.revert.entry(key).or_insert_with(|| prev_value.clone());
            prev_value
        }

        /// Remove key value from storage
        pub fn remove(&mut self, key: K) -> Option<V> {
            let prev_value = self.blocks.remove(&key);
            self.revert.entry(key).or_insert_with(|| prev_value.clone());
            prev_value
        }
    }

    impl<K: Key, V: Value> StorageReadOnly<K, V> for Block<'_, K, V> {
        fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.blocks.get(key)
        }

        fn iter(&self) -> Iter<'_, K, V> {
            Iter {
                iter: self.blocks.iter(),
            }
        }

        fn range<Q>(&self, bounds: impl RangeBounds<Q>) -> RangeIter<'_, K, V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            RangeIter {
                iter: self.blocks.range(bounds),
            }
        }

        fn len(&self) -> usize {
            self.blocks.len()
        }
    }

    /// Part of block's aggregated changes which applied or aborted at the same time
    pub struct Transaction<'block, 'store, K: Key, V: Value> {
        pub(crate) revert: BTreeMap<K, Option<V>>,
        pub(crate) block: &'block mut Block<'store, K, V>,
    }

    impl<'block, 'store: 'block, K: Key, V: Value> Transaction<'block, 'store, K, V> {
        /// Apply aggregated changes of [`Transaction`] to the [`Block`]
        pub fn apply(mut self) {
            for (key, value) in core::mem::take(&mut self.revert) {
                self.block.revert.entry(key).or_insert(value);
            }
        }

        /// Get mutable access to the value stored in
        pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
            self.block.blocks.get_mut(key).map(|value| {
                self.revert
                    .entry(key.clone())
                    .or_insert_with(|| Some(value.clone()));
                value
            })
        }

        /// Insert key value into the transaction temporary map
        pub fn insert(&mut self, key: K, value: V) -> Option<V> {
            let prev_value = self.block.blocks.insert(key.clone(), value);
            self.revert.entry(key).or_insert_with(|| prev_value.clone());
            prev_value
        }

        /// Remove key value from storage
        pub fn remove(&mut self, key: K) -> Option<V> {
            let prev_value = self.block.blocks.remove(&key);
            self.revert.entry(key).or_insert_with(|| prev_value.clone());
            prev_value
        }
    }

    impl<K: Key, V: Value> StorageReadOnly<K, V> for Transaction<'_, '_, K, V> {
        fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.block.get(key)
        }

        fn iter(&self) -> Iter<'_, K, V> {
            self.block.iter()
        }

        fn range<Q>(&self, bounds: impl RangeBounds<Q>) -> RangeIter<'_, K, V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            self.block.range(bounds)
        }

        fn len(&self) -> usize {
            self.block.len()
        }
    }

    impl<'block, 'store: 'block, K: Key, V: Value> Drop for Transaction<'block, 'store, K, V> {
        fn drop(&mut self) {
            // revert changes made so far by current transaction
            // if transaction was applied set would be empty
            for (key, value) in core::mem::take(&mut self.revert) {
                match value {
                    None => self.block.blocks.remove(&key),
                    Some(value) => self.block.blocks.insert(key, value),
                };
            }
        }
    }
}
pub use block::{Block, Transaction};
mod iter {
    use super::*;

    /// Iterate over entries in block, view or transaction
    pub struct Iter<'slf, K: Key, V: Value> {
        pub(crate) iter: concread::internals::bptree::iter::Iter<'slf, 'slf, K, V>,
    }

    /// Iterate over range of entries in block, view or transaction
    pub struct RangeIter<'slf, K: Key, V: Value> {
        pub(crate) iter: concread::internals::bptree::iter::RangeIter<'slf, 'slf, K, V>,
    }

    impl<'slf, K: Key, V: Value> Iterator for Iter<'slf, K, V> {
        type Item = (&'slf K, &'slf V);

        fn next(&mut self) -> Option<Self::Item> {
            self.iter.next()
        }
    }

    impl<'slf, K: Key, V: Value> Iterator for RangeIter<'slf, K, V> {
        type Item = (&'slf K, &'slf V);

        fn next(&mut self) -> Option<Self::Item> {
            self.iter.next()
        }
    }
}
pub use iter::{Iter, RangeIter};

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
            let mut block = storage.block();
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view1 = storage.view();

        {
            let mut block = storage.block();
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view2 = storage.view();

        {
            let mut block = storage.block();
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

        let mut block = storage.block();

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
            let mut block = storage.block();
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block();
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block();
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

        let mut block = storage.block();
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
            let mut block = storage.block();
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block();
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block();
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

        let mut block = storage.block();
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
    fn revert() {
        let storage = Storage::<u64, u64>::new();

        {
            let mut block = storage.block();
            block.insert(0, 0);
            block.commit()
        }

        {
            let mut block = storage.block();
            block.insert(0, 1);
            block.commit()
        }

        let view1 = storage.view();

        {
            let block = storage.block_and_revert();
            block.commit();
        }
        let view2 = storage.view();

        // View is persistent so revert is not visible
        assert_eq!(view1.get(&0), Some(&1));
        // Revert is visible in the view created after revert was applied
        assert_eq!(view2.get(&0), Some(&0));
    }

    #[test]
    fn len() {
        let storage = Storage::<u64, u64>::new();

        {
            let mut block = storage.block();
            for (key, value) in [(0, 0), (1, 0), (2, 0)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block();
            for (key, value) in [(0, 1), (1, 1), (3, 1)] {
                block.insert(key, value);
            }
            block.commit()
        }

        {
            let mut block = storage.block();
            for (key, value) in [(1, 2), (4, 2)] {
                block.insert(key, value);
            }
            block.commit()
        }

        let view = storage.view();
        assert_eq!(view.len(), 5);
    }

    proptest! {
        #[test]
        fn consistent_with_btreemap(txs: Vec<(bool, Vec<(u64, Option<u64>)>)>) {
            let storage = Storage::<u64, u64>::new();
            let mut map = BTreeMap::new();
            let mut keys = BTreeSet::new();

            for (committed, tx) in txs {
                let mut block = storage.block();

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
