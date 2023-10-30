use std::{borrow::Borrow, fmt::Debug, ops::RangeBounds};

use concread::{
    bptree::{BptreeMap, BptreeMapReadTxn, BptreeMapWriteTxn},
    ebrcell::{EbrCell, EbrCellReadTxn, EbrCellWriteTxn},
};
use itertools::Itertools;

pub trait Key: Clone + Ord + Debug + Send + Sync + 'static {}
pub trait Value: Clone + Send + Sync + 'static {}

impl<T: Clone + Ord + Debug + Send + Sync + 'static> Key for T {}
impl<T: Clone + Send + Sync + 'static> Value for T {}

/// Multi-version key value storage
pub struct Storage<K: Key, V: Value> {
    pub(crate) latest_block: EbrCell<block::BlockInner<K, V>>,
    pub(crate) blocks: BptreeMap<K, V>,
}

impl<K: Key, V: Value> Storage<K, V> {
    /// Construct new [`Self`]
    pub fn new() -> Self {
        Self {
            latest_block: EbrCell::new(block::BlockInner::new()),
            blocks: BptreeMap::new(),
        }
    }

    /// Create persistent view of storage at certain point in time
    pub fn view(&self) -> View<'_, K, V> {
        View {
            latest_block: self.latest_block.read(),
            blocks: self.blocks.read(),
        }
    }

    /// Create block to aggregate updates
    pub fn block(&self, rollback_latest_block: bool) -> Block<'_, K, V> {
        let mut latest_block = self.latest_block.write();
        let mut blocks = self.blocks.write();

        {
            let latest_block = core::mem::take(latest_block.get_mut());
            // - Just drop latest block in case of rollback
            // - Apply latest block to the rest of the blocks otherwise
            if !rollback_latest_block {
                for key in &latest_block.removed_keys {
                    blocks.remove(key);
                }

                blocks.extend(latest_block.new_or_updated_keys.into_iter())
            }
        }

        Block {
            latest_block,
            blocks,
        }
    }
}

impl<K: Key, V: Value> Default for Storage<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Module for [`View`] and it's related impls
mod view {
    use std::cmp::Ordering;

    use super::*;
    /// Consistent view of the storage at the certain version
    pub struct View<'storage, K: Key, V: Value> {
        pub(crate) latest_block: EbrCellReadTxn<block::BlockInner<K, V>>,
        pub(crate) blocks: BptreeMapReadTxn<'storage, K, V>,
    }

    impl<K: Key, V: Value> View<'_, K, V> {
        /// Read entry from the list up to certain version non-inclusive
        pub fn get<'slf, Q>(&'slf self, key: &'slf Q) -> Option<&'slf V>
        where
            K: Ord + Borrow<Q>,
            Q: Ord + ?Sized,
        {
            match self.latest_block.new_or_updated_keys.get(key) {
                None => {
                    if self.latest_block.removed_keys.contains(key) {
                        return None;
                    }
                    self.blocks.get(key)
                }
                some => some,
            }
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            let latest_block_iter = self.latest_block.new_or_updated_keys.iter();
            let blocks_iter = self.blocks.iter();
            let mut removed_keys_iter = self.latest_block.removed_keys.iter();
            let mut next_removed_key = removed_keys_iter.next();

            latest_block_iter
                .merge_by(
                    blocks_iter.filter(move |(k, _)| {
                        loop {
                            match next_removed_key.map(|removed_key| removed_key.cmp(k)) {
                                // Removed keys iter is ahead so there is no keys equal to `k`
                                None | Some(Ordering::Greater) => {
                                    return true;
                                }
                                // Removed keys iter is behind
                                Some(Ordering::Less) => {
                                    next_removed_key = removed_keys_iter.next();
                                }
                                Some(Ordering::Equal) => {
                                    next_removed_key = removed_keys_iter.next();
                                    return false;
                                }
                            }
                        }
                    }),
                    |l, r| l.0 <= r.0,
                )
                .dedup_by(|l, r| l.0 == r.0)
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn range<Q>(&self, bounds: impl RangeBounds<Q>) -> impl Iterator<Item = (&K, &V)>
        where
            K: Borrow<Q>,
            Q: Ord,
        {
            let latest_block_iter = self
                .latest_block
                .new_or_updated_keys
                .range((bounds.start_bound(), bounds.end_bound()));
            let blocks_iter = self.blocks.range(bounds);
            let mut removed_keys_iter = self.latest_block.removed_keys.iter();
            let mut next_removed_key = removed_keys_iter.next();

            latest_block_iter
                .merge_by(
                    blocks_iter.filter(move |(k, _)| {
                        loop {
                            match next_removed_key.map(|removed_key| removed_key.cmp(k)) {
                                // Removed keys iter is ahead so there is no keys equal to `k`
                                None | Some(Ordering::Greater) => {
                                    return true;
                                }
                                // Removed keys iter is behind
                                Some(Ordering::Less) => {
                                    next_removed_key = removed_keys_iter.next();
                                }
                                Some(Ordering::Equal) => {
                                    next_removed_key = removed_keys_iter.next();
                                    return false;
                                }
                            }
                        }
                    }),
                    |l, r| l.0 <= r.0,
                )
                .dedup_by(|l, r| l.0 == r.0)
        }
    }
}
pub use view::View;

/// Module for [`Block`] and it's related impls
mod block {
    use super::*;
    use std::{
        cmp::Ordering,
        collections::{BTreeMap, BTreeSet},
    };

    /// Batched update to the storage that can be reverted later
    pub struct Block<'store, K: Key, V: Value> {
        pub(crate) latest_block: EbrCellWriteTxn<'store, BlockInner<K, V>>,
        pub(crate) blocks: BptreeMapWriteTxn<'store, K, V>,
    }

    #[derive(Clone)]
    pub(crate) struct BlockInner<K, V> {
        pub(crate) removed_keys: BTreeSet<K>,
        pub(crate) new_or_updated_keys: BTreeMap<K, V>,
    }

    impl<K, V> BlockInner<K, V> {
        pub(crate) fn new() -> Self {
            Self {
                removed_keys: BTreeSet::new(),
                new_or_updated_keys: BTreeMap::new(),
            }
        }
    }

    impl<K, V> Default for BlockInner<K, V> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<'store, K: Key, V: Value> Block<'store, K, V> {
        /// Create transaction for the block
        pub fn transaction<'block>(&'block mut self) -> Transaction<'block, 'store, K, V>
        where
            'store: 'block,
        {
            Transaction {
                block: self,
                latest_block: BlockInner::new(),
            }
        }

        /// Apply aggregated changes to the storage
        pub fn commit(self) {
            // Commit fields in the inverse order
            self.blocks.commit();
            self.latest_block.commit();
        }

        /// Insert key value into the storage
        pub fn insert(&mut self, key: K, value: V) {
            self.latest_block.removed_keys.remove(&key);
            self.latest_block.new_or_updated_keys.insert(key, value);
        }

        /// Remove key value from storage
        pub fn remove(&mut self, key: K) {
            self.latest_block.new_or_updated_keys.remove(&key);
            self.latest_block.removed_keys.insert(key);
        }

        /// Read entry from the storage up to certain version non-inclusive
        pub fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            match self.latest_block.new_or_updated_keys.get(key) {
                None => {
                    if self.latest_block.removed_keys.contains(key) {
                        return None;
                    }
                    self.blocks.get(key)
                }
                some => some,
            }
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            let latest_block_iter = self.latest_block.new_or_updated_keys.iter();
            let blocks_iter = self.blocks.iter();
            let mut removed_keys_iter = self.latest_block.removed_keys.iter();
            let mut next_removed_key: Option<&K> = removed_keys_iter.next();

            latest_block_iter
                .merge_by(
                    blocks_iter.filter(move |(k, _)| {
                        loop {
                            match next_removed_key.map(|removed_key| removed_key.cmp(k)) {
                                // Removed keys iter is ahead so there is no keys equal to `k`
                                None | Some(Ordering::Greater) => {
                                    return true;
                                }
                                // Removed keys iter is behind
                                Some(Ordering::Less) => {
                                    next_removed_key = removed_keys_iter.next();
                                }
                                Some(Ordering::Equal) => {
                                    next_removed_key = removed_keys_iter.next();
                                    return false;
                                }
                            }
                        }
                    }),
                    |l, r| l.0 <= r.0,
                )
                .dedup_by(|l, r| l.0 == r.0)
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn range<Q, R>(&self, bounds: R) -> impl Iterator<Item = (&K, &V)>
        where
            K: Borrow<Q>,
            Q: Ord,
            R: RangeBounds<Q>,
        {
            let latest_block_iter = self
                .latest_block
                .new_or_updated_keys
                .range((bounds.start_bound(), bounds.end_bound()));
            let blocks_iter = self.blocks.range(bounds);
            let mut removed_keys_iter = self.latest_block.removed_keys.iter();
            let mut next_removed_key: Option<&K> = removed_keys_iter.next();

            latest_block_iter
                .merge_by(
                    blocks_iter.filter(move |(k, _)| {
                        loop {
                            match next_removed_key.map(|removed_key| removed_key.cmp(k)) {
                                // Removed keys iter is ahead so there is no keys equal to `k`
                                None | Some(Ordering::Greater) => {
                                    return true;
                                }
                                // Removed keys iter is behind
                                Some(Ordering::Less) => {
                                    next_removed_key = removed_keys_iter.next();
                                }
                                Some(Ordering::Equal) => {
                                    next_removed_key = removed_keys_iter.next();
                                    return false;
                                }
                            }
                        }
                    }),
                    |l, r| l.0 <= r.0,
                )
                .dedup_by(|l, r| l.0 == r.0)
        }
    }

    /// Part of block's aggregated changes which applied or aborted at the same time
    pub struct Transaction<'block, 'store, K: Key, V: Value> {
        pub(crate) latest_block: BlockInner<K, V>,
        pub(crate) block: &'block mut Block<'store, K, V>,
    }

    impl<'block, 'store: 'block, K: Key, V: Value> Transaction<'block, 'store, K, V> {
        /// Apply aggregated changes of [`Transaction`] to the [`Block`]
        pub fn apply(self) {
            let BlockInner {
                removed_keys,
                new_or_updated_keys,
            } = self.block.latest_block.get_mut();
            new_or_updated_keys.retain(|k, _| !self.latest_block.removed_keys.contains(k));
            new_or_updated_keys.extend(self.latest_block.new_or_updated_keys);
            removed_keys.extend(self.latest_block.removed_keys);
        }

        /// Insert key value into the transaction temporary map
        pub fn insert(&mut self, key: K, value: V) {
            self.latest_block.removed_keys.remove(&key);
            self.latest_block.new_or_updated_keys.insert(key, value);
        }

        /// Remove key value from storage
        pub fn remove(&mut self, key: K) {
            self.latest_block.new_or_updated_keys.remove(&key);
            self.latest_block.removed_keys.insert(key);
        }

        /// Read entry from the storage up to certain version non-inclusive
        pub fn get<Q>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Ord + ?Sized,
        {
            match self.latest_block.new_or_updated_keys.get(key) {
                None => {
                    if self.latest_block.removed_keys.contains(key) {
                        return None;
                    }
                    self.block.get(key)
                }
                some => some,
            }
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn iter<'slf>(
            &'slf self,
        ) -> impl Iterator<Item = (&'slf K, &'slf V)> + Captures<'store> + Captures<'block>
        {
            let latest_block_iter = self.latest_block.new_or_updated_keys.iter();
            let blocks_iter = self.block.iter();
            let mut removed_keys_iter = self.latest_block.removed_keys.iter();
            let mut next_removed_key: Option<&K> = removed_keys_iter.next();

            latest_block_iter
                .merge_by(
                    blocks_iter.filter(move |(k, _)| {
                        loop {
                            match next_removed_key.map(|removed_key| removed_key.cmp(k)) {
                                // Removed keys iter is ahead so there is no keys equal to `k`
                                None | Some(Ordering::Greater) => {
                                    return true;
                                }
                                // Removed keys iter is behind
                                Some(Ordering::Less) => {
                                    next_removed_key = removed_keys_iter.next();
                                }
                                Some(Ordering::Equal) => {
                                    next_removed_key = removed_keys_iter.next();
                                    return false;
                                }
                            }
                        }
                    }),
                    |l, r| l.0 <= r.0,
                )
                .dedup_by(|l, r| l.0 == r.0)
        }

        /// Iterate over all entries in the storage at the certain version non-inclusive
        pub fn range<'slf, Q, R>(
            &'slf self,
            bounds: R,
        ) -> impl Iterator<Item = (&'slf K, &'slf V)> + Captures<'store> + Captures<'block>
        where
            'block: 'slf,
            'store: 'slf,
            K: Borrow<Q>,
            Q: Ord + 'slf,
            R: RangeBounds<Q>,
        {
            let latest_block_iter = self
                .latest_block
                .new_or_updated_keys
                .range((bounds.start_bound(), bounds.end_bound()));
            let blocks_iter = self.block.range(bounds);
            let mut removed_keys_iter = self.latest_block.removed_keys.iter();
            let mut next_removed_key: Option<&K> = removed_keys_iter.next();

            latest_block_iter
                .merge_by(
                    blocks_iter.filter(move |(k, _)| {
                        loop {
                            match next_removed_key.map(|removed_key| removed_key.cmp(k)) {
                                // Removed keys iter is ahead so there is no keys equal to `k`
                                None | Some(Ordering::Greater) => {
                                    return true;
                                }
                                // Removed keys iter is behind
                                Some(Ordering::Less) => {
                                    next_removed_key = removed_keys_iter.next();
                                }
                                Some(Ordering::Equal) => {
                                    next_removed_key = removed_keys_iter.next();
                                    return false;
                                }
                            }
                        }
                    }),
                    |l, r| l.0 <= r.0,
                )
                .dedup_by(|l, r| l.0 == r.0)
        }
    }

    pub trait Captures<'a> {}
    impl<'a, T: ?Sized> Captures<'a> for T {}
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
