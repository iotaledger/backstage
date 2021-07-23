use num_traits::{CheckedAdd, FromPrimitive, NumAssignOps, WrappingAdd};
use serde::{Deserialize, Serialize};

/// A pool of unique IDs which can be assigned to services
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct IdPool<T> {
    pool: Vec<T>,
    empty: bool,
}

impl<T: NumAssignOps + FromPrimitive + Copy> IdPool<T> {
    /// Get a new unique ID
    pub fn get_id_unchecked(&mut self) -> T {
        if self.pool.len() == 0 {
            self.pool.push(T::from_isize(1).unwrap());
            T::from_isize(0).unwrap()
        } else if self.pool.len() == 1 {
            let id = self.pool[0];
            self.pool[0] += T::from_isize(1).unwrap();
            id
        } else {
            self.pool.pop().unwrap()
        }
    }
}

impl<T: CheckedAdd + FromPrimitive + Copy> IdPool<T> {
    /// Get a new unique ID
    pub fn get_id(&mut self) -> Option<T> {
        (!self.empty).then(|| {
            if self.pool.len() == 0 {
                self.pool.push(T::from_isize(1).unwrap());
                T::from_isize(0).unwrap()
            } else if self.pool.len() == 1 {
                let id = self.pool[0];
                if let Some(res) = self.pool[0].checked_add(&T::from_isize(1).unwrap()) {
                    self.pool[0] = res;
                } else {
                    self.empty = true;
                }
                id
            } else {
                self.pool.pop().unwrap()
            }
        })
    }
}

impl<T: WrappingAdd + FromPrimitive + Copy> IdPool<T> {
    /// Get a new unique ID
    pub fn get_id_wrapping(&mut self) -> T {
        if self.pool.len() == 0 {
            self.pool.push(T::from_isize(1).unwrap());
            T::from_isize(0).unwrap()
        } else if self.pool.len() == 1 {
            let id = self.pool[0];
            self.pool[0] = self.pool[0].wrapping_add(&T::from_isize(1).unwrap());
            id
        } else {
            self.pool.pop().unwrap()
        }
    }
}

impl<T> IdPool<T> {
    /// Return an unused id
    pub fn return_id(&mut self, id: T) {
        self.pool.push(id);
    }
}
