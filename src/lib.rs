use core::fmt::Debug;

pub mod cell;
#[cfg(feature = "serde")]
pub mod serde;
pub mod storage;

pub trait Key: Clone + Ord + Debug + Send + Sync + 'static {}
pub trait Value: Clone + Send + Sync + 'static {}

impl<T: Clone + Ord + Debug + Send + Sync + 'static> Key for T {}
impl<T: Clone + Send + Sync + 'static> Value for T {}
