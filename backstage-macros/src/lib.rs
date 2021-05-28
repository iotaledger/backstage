use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn build(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // let args = syn::parse_macro_input!(attr as syn::AttributeArgs);

    let syn::ItemFn {
        attrs,
        vis,
        sig,
        mut block,
    } = syn::parse_macro_input!(item as syn::ItemFn);

    let actor = match sig.output {
        syn::ReturnType::Type(_, ref ty) => {
            if let syn::Type::Path(p) = ty.as_ref() {
                &p.path.segments.last().unwrap().ident
            } else {
                panic!("Build function should specify an actor as its return type!");
            }
        }
        syn::ReturnType::Default => panic!("Build function should specify an actor as its return type!"),
    };

    let syn::Signature {
        generics,
        inputs: fn_inputs,
        ..
    } = sig;

    let (generics, bounded_generics, bare_generics, bounds) = if !generics.params.is_empty() {
        let params = generics.params;
        let stripped_params = params
            .iter()
            .map(|param| match param {
                syn::GenericParam::Type(t) => {
                    let id = &t.ident;
                    quote! {#id}
                }
                syn::GenericParam::Lifetime(l) => {
                    let id = &l.lifetime;
                    quote! {#id}
                }
                syn::GenericParam::Const(c) => {
                    panic!("Const generics not supported for builders!");
                }
            })
            .collect::<Vec<_>>();
        if let Some(bounds) = generics.where_clause {
            (
                quote! {<#(#stripped_params),*>},
                quote! {<#params>},
                quote! {#(#stripped_params),*},
                quote! {#bounds},
            )
        } else {
            (
                quote! {<#(#stripped_params),*>},
                quote! {<#params>},
                quote! {#(#stripped_params),*},
                quote! {},
            )
        }
    } else {
        (quote! {}, quote! {}, quote! {}, quote! {})
    };

    let builder = quote::format_ident!("{}Builder", actor);

    let (mut add_fns, mut inputs, mut input_names, mut input_unwraps) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut found_service = false;
    for input in fn_inputs {
        match input {
            syn::FnArg::Typed(mut t) => {
                let name = &t.pat;
                let ty = &t.ty;
                if let syn::Type::Path(p) = ty.as_ref() {
                    if p.path.is_ident("Service") {
                        if found_service {
                            panic!("Duplicate Service in build signature!");
                        }
                        found_service = true;
                    } else {
                        input_names.push(name.clone());

                        if let syn::Type::Path(prev_type) = t.ty.as_ref() {
                            if let Some(seg) = p.path.segments.last() {
                                if seg.ident.to_string() == "Option" {
                                    if let syn::PathArguments::AngleBracketed(ref args) = seg.arguments {
                                        let ty = args.args.first().unwrap();
                                        let add_fn = quote! {
                                            #vis fn #name<I: Into<Option<#ty>>> (mut self, val: I) -> Self {
                                                self.#name = val.into();
                                                self
                                            }
                                        };
                                        add_fns.push(add_fn);

                                        input_unwraps.push(quote! {self.#name});
                                    } else {
                                        panic!("Invalid Option arg!");
                                    }
                                } else {
                                    let add_fn = quote! {
                                        #vis fn #name (mut self, val: #ty) -> Self {
                                            self.#name.replace(val);
                                            self
                                        }
                                    };
                                    add_fns.push(add_fn);

                                    input_unwraps.push(
                                        quote! {self.#name.unwrap_or_else(|| panic!("Config param {} was not provided!", stringify!(self.#name)))},
                                    );

                                    t.ty = syn::parse_quote! {Option<#prev_type>};
                                }
                            }
                        }

                        inputs.push(t);
                    }
                }
            }
            _ => (),
        }
    }
    if !found_service {
        panic!("Build function must contain a Service parameter (ex. service: Service)!");
    }

    let defaults = input_names
        .iter()
        .map(|name| quote! {#name: Default::default()})
        .collect::<Vec<_>>();

    block.stmts.insert(
        0,
        syn::parse_quote! {
            let (#(#input_names),*) = (#(#input_unwraps),*);
        },
    );

    let res = quote! {
        #(#attrs)*
        #vis struct #builder #generics {
            _phantom: std::marker::PhantomData<(#bare_generics)>,
            #(#inputs),*
        }

        impl #bounded_generics Default for #builder #generics #bounds {
            fn default() -> Self {
                Self {
                    _phantom: std::marker::PhantomData,
                    #(#defaults),*
                }
            }
        }

        impl #bounded_generics #builder #generics #bounds {
            #vis fn new() -> Self {
                Self::default()
            }

            #(#add_fns)*
        }

        impl #bounded_generics Builder for #builder #generics #bounds
        {
            type Built = #actor #generics;

            fn build(self, service: Service) -> Self::Built #bounds
                #block

        }
    };
    res.into()
}
