/// Create the builder type for an actor, which can be used to construct an actor by parts
#[macro_export]
macro_rules! builder {
    ( $(#[derive($($der:tt),*)])?  $struct:ident {$( $field:ident: $type:tt$(<$($i:tt),*>)? ),*$(,)?} ) => {
        #[allow(missing_docs)]
        #[derive($($($der,)*)?Default)]
        pub struct $struct {
            $(
                $field: Option<$type$(<$($i,)*>)?>,
            )*
        }

        impl $struct {
            /// Create a new $struct
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                }
            }

            $(
                /// Set $field on the builder
                pub fn $field(mut self, $field: $type$(<$($i,)*>)?) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }

    };
    ($(#[derive($($der:tt),*)])? $struct:ident$(<$($extra:tt),*>)? {$( $field:ident: $type:tt$(<$($i:tt),*>)? ),*}) => {
        #[derive($($($der,)*)?Default)]
        #[allow(missing_docs)]
        pub struct $struct$(<$($extra,)*>)? {
            $(
                $field: Option<$type$(<$($i,)*>)?>,
            )*
            #[allow(unused_parens)]
            $(
                _phantom: std::marker::PhantomData<$($extra,)*>
            )?
        }

        impl$(<$($extra,)*>)? $struct$(<$($extra,)*>)? {
            /// Create a new $struct
            pub fn new() -> Self {
                Self {
                    $(
                        $field: None,
                    )*
                    _phantom: std::marker::PhantomData,
                }
            }
            $(
                /// Set $field on the builder
                pub fn $field(mut self, $field: $type$(<$($i,)*>)?) -> Self {
                    self.$field.replace($field);
                    self
                }
            )*
        }
    };


}
