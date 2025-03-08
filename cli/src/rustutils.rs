

pub trait ArrayDeconstructor<T, S> {
    fn into_1(self) -> T;
    fn try_single(self) -> Result<T, S>;
    fn into_2(self) -> (T, T);
    fn into_3(self) -> (T, T, T);

    fn into_array<const N: usize>(self) -> [T; N];
}

impl<T> ArrayDeconstructor<T, Vec<T>> for Vec<T> {
    fn into_1(self) -> T {
        self.into_iter().next().unwrap()
    }

    fn into_2(self) -> (T, T) {
        let mut iter = self.into_iter();
        (iter.next().unwrap(), iter.next().unwrap())
    }

    fn into_3(self) -> (T, T, T) {
        let mut iter = self.into_iter();
        (iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())
    }

    fn into_array<const N: usize>(self) -> [T; N] {
        let len = self.len();
        self.try_into().unwrap_or_else(|_| panic!("Expected {} elements, got {}", N, len))
    }
    
    fn try_single(self) -> Result<T, Vec<T>> {
        if self.len() == 1 {
            Ok(self.into_iter().next().unwrap())
        } else {
            Err(self)
        }
    }
}

pub trait AsStrRef<'a, T> {
    fn as_str_ref(&'a self) -> T;
}

impl<'a> AsStrRef<'a, &'a str> for String {
    fn as_str_ref(&self) -> &str {
        self.as_str()
    }
}

impl<'a> AsStrRef<'a, Option<&'a str>> for Option<String> {
    fn as_str_ref(&'a self) -> Option<&'a str> {
        self.as_ref().map(|s| s.as_str())
    }
}
