use proc_macro::TokenStream;
use quote::quote;
use syn::parse::Parse;

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
                syn::GenericParam::Const(_) => {
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
    for input in fn_inputs {
        match input {
            syn::FnArg::Typed(mut t) => {
                let name = &t.pat;
                let ty = &t.ty;
                if let syn::Type::Path(p) = ty.as_ref() {
                    input_names.push(name.clone());

                    if let syn::Type::Path(prev_type) = t.ty.as_ref() {
                        if let Some(seg) = p.path.segments.last() {
                            if seg.ident.to_string() == "Option" {
                                if let syn::PathArguments::AngleBracketed(ref args) = seg.arguments {
                                    let ty = args.args.first().unwrap();
                                    let doc_name = match name.as_ref() {
                                        syn::Pat::Ident(i) => i.ident.to_string(),
                                        _ => "???".to_string(),
                                    };
                                    let doc = format!("Provide the builder with the {} field", doc_name);
                                    let add_fn = quote! {
                                        #[doc=#doc]
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
                                let doc_name = match name.as_ref() {
                                    syn::Pat::Ident(i) => i.ident.to_string(),
                                    _ => "???".to_string(),
                                };
                                let doc = format!("Provide the builder with the {} field", doc_name);
                                let add_fn = quote! {
                                    #[doc=#doc]
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
            _ => (),
        }
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

    let builder_doc = format!("A builder for the {} type", actor);
    let new_builder_doc = format!("Create a new `{}`", builder);

    let res = quote! {
        #[doc=#builder_doc]
        #(#attrs)*
        #vis struct #builder #generics #bounds {
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
            #[doc=#new_builder_doc]
            #vis fn new() -> Self {
                Self::default()
            }

            #(#add_fns)*
        }

        impl #bounded_generics Builder for #builder #generics #bounds
        {
            type Built = #actor #generics;

            fn build(self) -> Self::Built #bounds
                #block

        }
    };
    res.into()
}

struct SupArgs(syn::punctuated::Punctuated<syn::Type, syn::Token![,]>);

impl Parse for SupArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(SupArgs(syn::punctuated::Punctuated::parse_terminated(input)?))
    }
}

#[proc_macro_attribute]
pub fn supervise(attr: TokenStream, item: TokenStream) -> TokenStream {
    let SupArgs(children) = syn::parse_macro_input!(attr as SupArgs);

    let mut ident_paths = Vec::new();

    for child in children {
        match child {
            syn::Type::Path(p) => {
                let id = p.path.get_ident().unwrap_or_else(|| &p.path.segments.last().unwrap().ident).clone();
                ident_paths.push((id, p));
            }
            _ => panic!("Expected child type!"),
        }
    }

    let (mut children, mut states, mut from_state_impls, mut from_child_impls) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (ident, path) in ident_paths.iter() {
        children.push(quote! {
            #ident
        });
        states.push(quote! {
            #ident(#path)
        });
        from_state_impls.push(quote! {
            impl From<#path> for ChildStates {
                fn from(s: #path) -> Self {
                    Self::#ident(s)
                }
            }
        });
        from_child_impls.push(quote! {
            impl From<std::marker::PhantomData<#path>> for Children {
                fn from(_: std::marker::PhantomData<#path>) -> Self {
                    Self::#ident
                }
            }
        });
    }

    let syn::ItemEnum {
        attrs,
        vis,
        enum_token: _,
        ident,
        generics,
        brace_token: _,
        variants,
    } = syn::parse_macro_input!(item as syn::ItemEnum);

    let res = if children.len() > 1 {
        quote! {
            #(#attrs)*
            #vis enum #ident #generics {
                #[doc="Event variant used to report children actors exiting. Contains the actor's state and a request from the child."]
                ReportExit(Result<SuccessReport<ChildStates>, ErrorReport<ChildStates>>),
                #[doc="Event variant used to report children status changes."]
                StatusChange(StatusChange<Children>),
                #variants
            }

            #vis enum ChildStates {
                #(#states),*
            }

            #[derive(Copy, Clone)]
            #vis enum Children {
                #(#children),*
            }

            #(#from_state_impls)*
            #(#from_child_impls)*

            impl SupervisorEvent for #ident {
                type ChildStates = ChildStates;
                type Children = Children;

                fn report(res: Result<SuccessReport<Self::ChildStates>, ErrorReport<Self::ChildStates>>) -> Self
                where
                    Self: Sized,
                {
                    Self::ReportExit(res)
                }

                fn status_change(status_change: StatusChange<Self::Children>) -> Self
                where
                    Self: Sized,
                {
                    Self::StatusChange(status_change)
                }
            }
        }
    } else {
        let (child, state) = (children.remove(0), ident_paths.remove(0).1);
        quote! {
            #(#attrs)*
            #vis enum #ident #generics {
                #[doc="Event variant used to report child actor exiting. Contains the actor's state and a request from the child."]
                ReportExit(Result<SuccessReport<#state>, ErrorReport<#state>>),
                #[doc="Event variant used to report child status changes."]
                StatusChange(StatusChange<std::marker::PhantomData<#child>>),
                #variants
            }

            impl SupervisorEvent for #ident {
                type ChildStates = #state;
                type Children = std::marker::PhantomData<#child>;

                fn report(res: Result<SuccessReport<Self::ChildStates>, ErrorReport<Self::ChildStates>>) -> Self
                where
                    Self: Sized,
                {
                    Self::ReportExit(res)
                }

                fn status_change(status_change: StatusChange<Self::Children>) -> Self
                where
                    Self: Sized,
                {
                    Self::StatusChange(status_change)
                }
            }
        }
    };
    res.into()
}
