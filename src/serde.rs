//! Module with serialization and deserialization of multi version storage

use core::fmt;
use std::{collections::BTreeMap, ops::Deref};

use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeStruct},
    Deserialize, Deserializer, Serialize,
};

use crate::{Key, Value};

pub use cell::CellSeeded;
pub use storage::StorageSeeded;

mod storage {
    use crate::storage::Storage;

    use super::*;

    /// Struct to deserialize [`Storage`] with provided seed for keys and values
    /// In case seed is only required for keys or values use [`PhantomData`] in place where seed is not required.
    pub struct StorageSeeded<KS, VS> {
        kseed: KS,
        vseed: VS,
    }

    impl<K: Serialize + Key, V: Serialize + Value> Serialize for Storage<K, V> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let rollback = self.rollback.read();
            let blocks = self.blocks.read();

            let mut state = serializer.serialize_struct("Storage", 2)?;
            state.serialize_field("rollback", rollback.deref())?;
            state.serialize_field("blocks", &BlocksSerializeHelper(blocks))?;
            state.end()
        }
    }

    struct BlocksSerializeHelper<'block, K: Key, V: Value>(
        concread::bptree::BptreeMapReadTxn<'block, K, V>,
    );

    impl<K: Serialize + Key, V: Serialize + Value> Serialize for BlocksSerializeHelper<'_, K, V> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for (k, v) in self.0.iter() {
                map.serialize_entry(k, v)?;
            }
            map.end()
        }
    }

    impl<'de, K: Deserialize<'de> + Key, V: Deserialize<'de> + Value> Deserialize<'de>
        for Storage<K, V>
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            StorageSeeded {
                kseed: core::marker::PhantomData::<K>,
                vseed: core::marker::PhantomData::<V>,
            }
            .deserialize(deserializer)
        }
    }

    impl<'de, KS, VS> DeserializeSeed<'de> for StorageSeeded<KS, VS>
    where
        KS: DeserializeSeed<'de> + Clone,
        VS: DeserializeSeed<'de> + Clone,
        KS::Value: Key,
        VS::Value: Value,
    {
        type Value = Storage<KS::Value, VS::Value>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            enum Field {
                Rollback,
                Blocks,
            }

            impl<'de> Deserialize<'de> for Field {
                fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = Field;

                        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                            formatter.write_str("`rollback` or `blocks`")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<Field, E>
                        where
                            E: de::Error,
                        {
                            match value {
                                "rollback" => Ok(Field::Rollback),
                                "blocks" => Ok(Field::Blocks),
                                _ => Err(de::Error::unknown_field(value, FIELDS)),
                            }
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct StorageSeededVisitor<KS, VS> {
                kseed: KS,
                vseed: VS,
            }

            impl<'de, KS, VS> Visitor<'de> for StorageSeededVisitor<KS, VS>
            where
                KS: DeserializeSeed<'de> + Clone,
                VS: DeserializeSeed<'de> + Clone,
                KS::Value: Key,
                VS::Value: Value,
            {
                type Value = Storage<KS::Value, VS::Value>;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_fmt(format_args!(
                        "struct Storage<{}, {}>",
                        core::any::type_name::<KS::Value>(),
                        core::any::type_name::<VS::Value>(),
                    ))
                }

                fn visit_seq<SA>(self, mut seq: SA) -> Result<Self::Value, SA::Error>
                where
                    SA: SeqAccess<'de>,
                {
                    let rollback = seq
                        .next_element_seed(RollbackDeserializeSeeded {
                            kseed: self.kseed.clone(),
                            vseed: self.vseed.clone(),
                        })?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let blocks = seq
                        .next_element_seed(BlocksDeserializeSeeded {
                            kseed: self.kseed.clone(),
                            vseed: self.vseed.clone(),
                        })?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    Ok(Storage { rollback, blocks })
                }

                fn visit_map<MA>(self, mut map: MA) -> Result<Self::Value, MA::Error>
                where
                    MA: MapAccess<'de>,
                {
                    let mut rollback = None;
                    let mut blocks = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::Rollback => {
                                if rollback.is_some() {
                                    return Err(de::Error::duplicate_field("rollback"));
                                }
                                rollback =
                                    Some(map.next_value_seed(RollbackDeserializeSeeded {
                                        kseed: self.kseed.clone(),
                                        vseed: self.vseed.clone(),
                                    })?);
                            }
                            Field::Blocks => {
                                if blocks.is_some() {
                                    return Err(de::Error::duplicate_field("blocks"));
                                }
                                blocks = Some(map.next_value_seed(BlocksDeserializeSeeded {
                                    kseed: self.kseed.clone(),
                                    vseed: self.vseed.clone(),
                                })?);
                            }
                        }
                    }
                    let rollback = rollback.ok_or_else(|| de::Error::missing_field("rollback"))?;
                    let blocks = blocks.ok_or_else(|| de::Error::missing_field("blocks"))?;
                    Ok(Storage { rollback, blocks })
                }
            }

            const FIELDS: &[&str] = &["rollback", "blocks"];
            deserializer.deserialize_struct(
                "Storage",
                FIELDS,
                StorageSeededVisitor {
                    kseed: self.kseed,
                    vseed: self.vseed,
                },
            )
        }
    }

    struct BlocksDeserializeSeeded<KS, VS> {
        kseed: KS,
        vseed: VS,
    }

    impl<'de, KS, VS> DeserializeSeed<'de> for BlocksDeserializeSeeded<KS, VS>
    where
        KS: DeserializeSeed<'de> + Clone,
        VS: DeserializeSeed<'de> + Clone,
        KS::Value: Key,
        VS::Value: Value,
    {
        type Value = concread::bptree::BptreeMap<KS::Value, VS::Value>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct BlocksSeededVisitor<KS, VS> {
                kseed: KS,
                vseed: VS,
            }

            impl<
                    'de,
                    K: Key,
                    V: Value,
                    KS: DeserializeSeed<'de, Value = K> + Clone,
                    VS: DeserializeSeed<'de, Value = V> + Clone,
                > Visitor<'de> for BlocksSeededVisitor<KS, VS>
            where
                KS: DeserializeSeed<'de> + Clone,
                VS: DeserializeSeed<'de> + Clone,
                KS::Value: Key,
                VS::Value: Value,
            {
                type Value = concread::bptree::BptreeMap<K, V>;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a map")
                }

                fn visit_map<MA>(self, mut map: MA) -> Result<Self::Value, MA::Error>
                where
                    MA: MapAccess<'de>,
                {
                    core::iter::from_fn(|| {
                        map.next_entry_seed(self.kseed.clone(), self.vseed.clone())
                            .transpose()
                    })
                    .collect::<Result<concread::bptree::BptreeMap<K, V>, MA::Error>>()
                }
            }

            deserializer.deserialize_map(BlocksSeededVisitor {
                kseed: self.kseed,
                vseed: self.vseed,
            })
        }
    }

    struct RollbackDeserializeSeeded<KS, VS> {
        kseed: KS,
        vseed: VS,
    }

    impl<'de, KS, VS> DeserializeSeed<'de> for RollbackDeserializeSeeded<KS, VS>
    where
        KS: DeserializeSeed<'de> + Clone,
        VS: DeserializeSeed<'de> + Clone,
        KS::Value: Key,
        VS::Value: Value,
    {
        type Value = concread::ebrcell::EbrCell<BTreeMap<KS::Value, Option<VS::Value>>>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            struct RollbackSeededVisitor<KS, VS> {
                kseed: KS,
                vseed: VS,
            }

            impl<
                    'de,
                    K: Key,
                    V: Value,
                    KS: DeserializeSeed<'de, Value = K> + Clone,
                    VS: DeserializeSeed<'de, Value = V> + Clone,
                > Visitor<'de> for RollbackSeededVisitor<KS, VS>
            where
                KS: DeserializeSeed<'de> + Clone,
                VS: DeserializeSeed<'de> + Clone,
                KS::Value: Key,
                VS::Value: Value,
            {
                type Value = concread::ebrcell::EbrCell<BTreeMap<KS::Value, Option<VS::Value>>>;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a map")
                }

                fn visit_map<MA>(self, mut map: MA) -> Result<Self::Value, MA::Error>
                where
                    MA: MapAccess<'de>,
                {
                    core::iter::from_fn(|| {
                        map.next_entry_seed(
                            self.kseed.clone(),
                            OptionSeeded {
                                seed: self.vseed.clone(),
                            },
                        )
                        .transpose()
                    })
                    .collect::<Result<BTreeMap<_, _>, MA::Error>>()
                    .map(concread::EbrCell::new)
                }
            }

            deserializer.deserialize_map(RollbackSeededVisitor {
                kseed: self.kseed,
                vseed: self.vseed,
            })
        }
    }
}

mod cell {
    use concread::EbrCell;

    use crate::cell::Cell;

    use super::*;

    /// Struct to deserialize [`Storage`] with provided seed for keys and values
    /// In case seed is only required for keys or values use [`PhantomData`] in place where seed is not required.
    pub struct CellSeeded<S> {
        seed: S,
    }

    impl<V: Serialize + Value> Serialize for Cell<V> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let rollback = self.rollback.read();
            let blocks = self.blocks.read();

            let mut state = serializer.serialize_struct("Storage", 2)?;
            state.serialize_field("rollback", rollback.deref())?;
            state.serialize_field("blocks", blocks.deref())?;
            state.end()
        }
    }

    impl<'de, V: Deserialize<'de> + Value> Deserialize<'de> for Cell<V> {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            CellSeeded {
                seed: core::marker::PhantomData::<V>,
            }
            .deserialize(deserializer)
        }
    }

    impl<'de, S> DeserializeSeed<'de> for CellSeeded<S>
    where
        S: DeserializeSeed<'de> + Clone,
        S::Value: Value,
    {
        type Value = Cell<S::Value>;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            enum Field {
                Rollback,
                Blocks,
            }

            impl<'de> Deserialize<'de> for Field {
                fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = Field;

                        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                            formatter.write_str("`rollback` or `blocks`")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<Field, E>
                        where
                            E: de::Error,
                        {
                            match value {
                                "rollback" => Ok(Field::Rollback),
                                "blocks" => Ok(Field::Blocks),
                                _ => Err(de::Error::unknown_field(value, FIELDS)),
                            }
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct CellSeededVisitor<S> {
                seed: S,
            }

            impl<'de, S> Visitor<'de> for CellSeededVisitor<S>
            where
                S: DeserializeSeed<'de> + Clone,
                S::Value: Value,
            {
                type Value = Cell<S::Value>;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_fmt(format_args!(
                        "struct Cell<{}>",
                        core::any::type_name::<S::Value>(),
                    ))
                }

                fn visit_seq<SA>(self, mut seq: SA) -> Result<Self::Value, SA::Error>
                where
                    SA: SeqAccess<'de>,
                {
                    let rollback = seq
                        .next_element_seed(OptionSeeded {
                            seed: self.seed.clone(),
                        })?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let blocks = seq
                        .next_element_seed(self.seed.clone())?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    Ok(Cell {
                        rollback: EbrCell::new(rollback),
                        blocks: EbrCell::new(blocks),
                    })
                }

                fn visit_map<MA>(self, mut map: MA) -> Result<Self::Value, MA::Error>
                where
                    MA: MapAccess<'de>,
                {
                    let mut rollback = None;
                    let mut blocks = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::Rollback => {
                                if rollback.is_some() {
                                    return Err(de::Error::duplicate_field("rollback"));
                                }
                                rollback = Some(map.next_value_seed(OptionSeeded {
                                    seed: self.seed.clone(),
                                })?);
                            }
                            Field::Blocks => {
                                if blocks.is_some() {
                                    return Err(de::Error::duplicate_field("blocks"));
                                }
                                blocks = Some(map.next_value_seed(self.seed.clone())?);
                            }
                        }
                    }
                    let rollback = rollback.ok_or_else(|| de::Error::missing_field("rollback"))?;
                    let blocks = blocks.ok_or_else(|| de::Error::missing_field("blocks"))?;
                    Ok(Cell {
                        rollback: EbrCell::new(rollback),
                        blocks: EbrCell::new(blocks),
                    })
                }
            }

            const FIELDS: &[&str] = &["rollback", "blocks"];
            deserializer.deserialize_struct("Cell", FIELDS, CellSeededVisitor { seed: self.seed })
        }
    }
}

struct OptionSeeded<S> {
    seed: S,
}

impl<'de, S> DeserializeSeed<'de> for OptionSeeded<S>
where
    S: DeserializeSeed<'de>,
{
    type Value = Option<S::Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OptionSeededVisitor<S> {
            seed: S,
        }

        impl<'de, S> Visitor<'de> for OptionSeededVisitor<S>
        where
            S: DeserializeSeed<'de>,
        {
            type Value = Option<S::Value>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an option")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                self.seed.deserialize(deserializer).map(Some)
            }
        }

        deserializer.deserialize_option(OptionSeededVisitor { seed: self.seed })
    }
}

#[cfg(test)]
mod tests {
    use crate::{cell::Cell, storage::Storage};

    #[test]
    fn serialize_deserialize_storage() {
        let storage = Storage::<u64, u64>::new();

        for i in 0..100 {
            let mut block = storage.block(false);
            block.insert(i, i);
            block.commit();
        }

        let storage: Storage<u64, u64> = serde_json::from_str(
            &serde_json::to_string(&storage).expect("failed to serialize storage"),
        )
        .expect("failed to deserialize storage");

        let view = storage.view();
        for i in 0..100 {
            let value = view.get(&i);
            assert_eq!(value, Some(&i));
        }
    }

    #[test]
    fn serialize_deserialize_cell() {
        let cell = Cell::new(0_u64);

        {
            let mut block = cell.block(false);
            *block.get_mut() = 1;
            block.commit();
        }

        let cell: Cell<u64> = serde_json::from_str(
            &serde_json::to_string(&cell).expect("failed to serialize storage"),
        )
        .expect("failed to deserialize storage");

        let view = cell.view();
        assert_eq!(view.get(), &1);

        {
            let block = cell.block(true);
            block.commit();
        }

        let view = cell.view();
        assert_eq!(view.get(), &0);
    }
}
