use std::{
    collections::HashSet,
    hash::Hash,
    sync::{Arc, LazyLock},
};

use parking_lot::Mutex;

/// Reuses memory for frequently used values
pub fn intern<T: Intern>(value: T) -> Arc<T> {
    value.intern()
}

#[derive(Debug, Default)]
pub struct Pool<T> {
    interned: HashSet<Arc<T>>,
}

impl<T: Hash + Eq> Pool<T> {
    pub fn intern(&mut self, value: T) -> Arc<T> {
        match self.interned.get(&value) {
            Some(arc) => arc.clone(),
            None => {
                let arc = Arc::new(value);
                self.interned.insert(arc.clone());
                arc
            }
        }
    }
}

// Statics with generic types are not supported, so separate statics are needed for each type

pub trait Intern {
    fn intern(self) -> Arc<Self>;
}

static STRING_POOL: LazyLock<Mutex<Pool<String>>> = LazyLock::new(Default::default);
impl Intern for String {
    fn intern(self) -> Arc<Self> {
        STRING_POOL.lock().intern(self)
    }
}

static VEC_STRINGS_POOL: LazyLock<Mutex<Pool<Vec<String>>>> = LazyLock::new(Default::default);
impl Intern for Vec<String> {
    fn intern(self) -> Arc<Self> {
        VEC_STRINGS_POOL.lock().intern(self)
    }
}
