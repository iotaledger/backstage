#![warn(missing_docs)]
pub mod actor;
pub use actor::*;
pub use async_recursion;
#[cfg(feature = "prefabs")]
pub mod prefabs;

pub enum Boo<'a, B: 'a> {
    Borrowed(&'a mut B),
    Owned(B),
}

impl<'a, B: 'a> std::ops::Deref for Boo<'a, B> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        match self {
            Boo::Borrowed(b) => *b,
            Boo::Owned(o) => o,
        }
    }
}

impl<'a, B: 'a> std::ops::DerefMut for Boo<'a, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Boo::Borrowed(b) => *b,
            Boo::Owned(ref mut o) => o,
        }
    }
}

impl<'a, O: 'a> From<O> for Boo<'a, O> {
    fn from(owned: O) -> Self {
        Self::Owned(owned)
    }
}

impl<'a, B: 'a> From<&'a mut B> for Boo<'a, B> {
    fn from(borrowed: &'a mut B) -> Self {
        Self::Borrowed(borrowed)
    }
}
