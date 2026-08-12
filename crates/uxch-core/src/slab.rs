use core::marker::PhantomData;

#[derive(Debug)]
pub struct Slab<T> {
    _p: PhantomData<T>,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self {
            _p: Default::default(),
        }
    }
}
