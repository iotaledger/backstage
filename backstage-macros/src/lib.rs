// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use proc_macro::TokenStream;
use quote::quote;

struct ReportData {
    id: syn::Ident,
    fields: Vec<(Option<syn::Ident>, FieldType)>,
    scope_id: bool,
    service: bool,
    children: Option<syn::TypePath>,
    res: bool,
    named_fields: bool,
}

enum FieldType {
    ScopeId,
    Service,
    OptRes,
    Res,
    OptChildren,
    Children,
}

impl ReportData {
    fn new(id: syn::Ident, named_fields: bool) -> Self {
        Self {
            id,
            fields: Vec::new(),
            scope_id: false,
            service: false,
            children: None,
            res: false,
            named_fields,
        }
    }
}

#[proc_macro_attribute]
pub fn supervise(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let syn::ItemEnum {
        attrs,
        vis,
        enum_token: _,
        ident,
        generics,
        brace_token: _,
        mut variants,
    } = syn::parse_macro_input!(item as syn::ItemEnum);

    let mut shutdown_var = None;
    let mut report_data = None;
    let mut eol_data = None;

    for syn::Variant {
        attrs,
        ident,
        fields,
        discriminant: _,
    } in variants.iter_mut()
    {
        for attr in attrs.drain(..) {
            if let Some(id) = attr.path.get_ident() {
                match id.to_string().as_str() {
                    "shutdown" => {
                        if shutdown_var.is_some() {
                            panic!("Only one variant can be marked with `shutdown`");
                        }
                        if !matches!(fields, syn::Fields::Unit) {
                            panic!("Shutdown variant must not have fields");
                        }
                        shutdown_var = Some(ident.clone());
                    }
                    "report" => {
                        if report_data.is_some() {
                            panic!("Only one variant can be marked with `report`");
                        }
                        if matches!(fields, syn::Fields::Unit) {
                            panic!("Report variant must have at least a ScopeId and Service field!");
                        }
                        if fields.len() > 4 {
                            panic!("Report variant must have at most 4 fields (ScopeId, Service, Option<ActorResult>, and an optional children type)!");
                        }
                        let mut data = ReportData::new(ident.clone(), matches!(fields, syn::Fields::Named(_)));
                        for field in fields.iter() {
                            match field.ty {
                                syn::Type::Path(ref p) => {
                                    match p.path.segments.last().unwrap().ident.to_string().as_str() {
                                        "ScopeId" => {
                                            if data.scope_id {
                                                panic!("Too many ScopeId fields specified for Report variant!");
                                            }
                                            data.scope_id = true;
                                            data.fields.push((field.ident.clone(), FieldType::ScopeId));
                                        }
                                        "Service" => {
                                            if data.service {
                                                panic!("Too many Service fields specified for Report variant!");
                                            }
                                            data.service = true;
                                            data.fields.push((field.ident.clone(), FieldType::Service));
                                        }
                                        "Option" => {
                                            if let syn::PathArguments::AngleBracketed(ref args) =
                                                p.path.segments.last().unwrap().arguments
                                            {
                                                if let syn::GenericArgument::Type(t) = args.args.first().unwrap() {
                                                    match t {
                                                        syn::Type::Path(ref p) => {
                                                            match p
                                                                .path
                                                                .segments
                                                                .last()
                                                                .unwrap()
                                                                .ident
                                                                .to_string()
                                                                .as_str()
                                                            {
                                                                "ActorResult" => {
                                                                    if data.res {
                                                                        panic!("Too many ActorResult fields specified for Report variant!");
                                                                    }
                                                                    data.res = true;
                                                                    data.fields
                                                                        .push((field.ident.clone(), FieldType::OptRes));
                                                                }
                                                                _ => {
                                                                    if data.children.is_some() {
                                                                        panic!("Too many children fields specified for Report variant!");
                                                                    }
                                                                    data.children = Some(p.clone());
                                                                    data.fields.push((
                                                                        field.ident.clone(),
                                                                        FieldType::OptChildren,
                                                                    ));
                                                                }
                                                            }
                                                        }
                                                        _ => {
                                                            panic!("Report variant optional field is not a valid type!")
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        _ => panic!("Unexpected field in report variant!"),
                                    }
                                }
                                _ => panic!("Report variant field is not a valid type!"),
                            }
                        }
                        if !data.scope_id || !data.service {
                            panic!("Report variant must have a ScopeId and Service field!");
                        }
                        report_data = Some(data);
                    }
                    "eol" => {
                        if eol_data.is_some() {
                            panic!("Only one variant can be marked with `eol`");
                        }
                        if matches!(fields, syn::Fields::Unit) {
                            panic!("Eol variant must have at least a ScopeId and Service field!");
                        }
                        if fields.len() > 4 {
                            panic!("Eol variant must have at most 4 fields (ScopeId, Service, ActorResult, and a children type)!");
                        }
                        let mut data = ReportData::new(ident.clone(), matches!(fields, syn::Fields::Named(_)));
                        for field in fields.iter() {
                            match field.ty {
                                syn::Type::Path(ref p) => {
                                    match p.path.segments.last().unwrap().ident.to_string().as_str() {
                                        "ScopeId" => {
                                            if data.scope_id {
                                                panic!("Too many ScopeId fields specified for Eol variant!");
                                            }
                                            data.scope_id = true;
                                            data.fields.push((field.ident.clone(), FieldType::ScopeId));
                                        }
                                        "Service" => {
                                            if data.service {
                                                panic!("Too many Service fields specified for Eol variant!");
                                            }
                                            data.service = true;
                                            data.fields.push((field.ident.clone(), FieldType::Service));
                                        }
                                        "ActorResult" => {
                                            if data.res {
                                                panic!("Too many ActorResult fields specified for Eol variant!");
                                            }
                                            data.res = true;
                                            data.fields.push((field.ident.clone(), FieldType::Res));
                                        }
                                        _ => {
                                            if data.children.is_some() {
                                                panic!("Too many children fields specified for Eol variant!");
                                            }
                                            data.children = Some(p.clone());
                                            data.fields.push((field.ident.clone(), FieldType::Children));
                                        }
                                    }
                                }
                                _ => panic!("Eol variant field is not a valid type!"),
                            }
                        }
                        if !data.scope_id || !data.service {
                            panic!("Eol variant must have a ScopeId and Service field!");
                        }
                        eol_data = Some(data);
                    }
                    _ => (),
                }
            }
        }
    }

    let shutdown_var = shutdown_var.expect("No variant marked with `shutdown`!");
    let report_data = report_data.expect("No variant marked with `report`!");
    if eol_data.is_some() {
        if report_data.res || report_data.children.is_some() {
            panic!("Too many fields specified on report variant while also specifying eol variant!");
        }
    }
    let ReportData {
        id: report_var,
        fields: report_fields,
        children: _report_children,
        named_fields,
        ..
    } = &report_data;

    let basics = quote! {
        #(#attrs)*
        #vis enum #ident #generics {
            #variants
        }

        impl backstage::core::ShutdownEvent for #ident {
            fn shutdown_event() -> Self {
                Self::#shutdown_var
            }
        }
    };
    let mut fields = Vec::new();
    for (id, field_type) in report_fields.iter() {
        if let Some(name) = id {
            fields.push(match field_type {
                FieldType::ScopeId => quote! {
                    #name: scope_id
                },
                FieldType::Service => quote! {
                    #name: service
                },
                FieldType::OptRes => quote! {
                    #name: None
                },
                FieldType::OptChildren => quote! {
                    #name: None
                },
                _ => panic!("Unexpected field type!"),
            });
        } else {
            fields.push(match field_type {
                FieldType::ScopeId => quote! {
                    scope_id
                },
                FieldType::Service => quote! {
                    service
                },
                FieldType::OptRes => quote! {
                    None
                },
                FieldType::OptChildren => quote! {
                    None
                },
                _ => panic!("Unexpected field type!"),
            });
        }
    }
    let fields = if *named_fields {
        quote! {{#(#fields),*}}
    } else {
        quote! {(#(#fields),*)}
    };
    let report = quote! {
        impl<T> backstage::core::ReportEvent<T> for #ident {
            fn report_event(scope_id: ScopeId, service: Service) -> Self {
                Self::#report_var #fields
            }
        }
    };

    let eol_data = eol_data.unwrap_or(report_data);
    let ReportData {
        id: eol_var,
        fields: eol_fields,
        children: eol_children,
        named_fields,
        ..
    } = eol_data;

    let mut fields = Vec::new();
    let mut needs_bounds = None;
    for (id, field_type) in eol_fields.iter() {
        if let Some(name) = id {
            fields.push(match field_type {
                FieldType::ScopeId => quote! {
                    #name: scope_id
                },
                FieldType::Service => quote! {
                    #name: service
                },
                FieldType::OptRes => quote! {
                    #name: Some(res)
                },
                FieldType::OptChildren => {
                    let eol_children = eol_children.as_ref().unwrap();
                    needs_bounds = Some(quote! {#eol_children});
                    quote! {
                        #name: Some(actor.into())
                    }
                }
                FieldType::Res => quote! {
                    #name: res
                },
                FieldType::Children => {
                    let eol_children = eol_children.as_ref().unwrap();
                    needs_bounds = Some(quote! {#eol_children});
                    quote! {
                        #name: actor.into()
                    }
                }
            });
        } else {
            fields.push(match field_type {
                FieldType::ScopeId => quote! {
                    scope_id
                },
                FieldType::Service => quote! {
                    service
                },
                FieldType::OptRes => quote! {
                    Some(res)
                },
                FieldType::OptChildren => {
                    let eol_children = eol_children.as_ref().unwrap();
                    needs_bounds = Some(quote! {#eol_children});
                    quote! {
                        Some(actor.into())
                    }
                }
                FieldType::Res => quote! {
                    res
                },
                FieldType::Children => {
                    let eol_children = eol_children.as_ref().unwrap();
                    needs_bounds = Some(quote! {#eol_children});
                    quote! {
                        actor.into()
                    }
                }
            });
        }
    }
    let fields = if named_fields {
        quote! {{#(#fields),*}}
    } else {
        quote! {(#(#fields),*)}
    };
    let bounds = if let Some(children) = needs_bounds {
        quote! {
            where T: Into<#children>
        }
    } else {
        quote! {}
    };
    let eol = quote! {
        impl<T> backstage::core::EolEvent<T> for #ident #bounds {
            fn eol_event(scope_id: ScopeId, service: Service, actor: T, res: ActorResult) -> Self {
                Self::#eol_var #fields
            }
        }
    };
    let res = quote! {
        #basics

        #report

        #eol
    };

    res.into()
}

#[proc_macro_attribute]
pub fn children(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let syn::ItemEnum {
        attrs,
        vis,
        enum_token: _,
        ident,
        generics,
        brace_token: _,
        variants,
    } = syn::parse_macro_input!(item as syn::ItemEnum);

    let res = quote! {
        #(#attrs)*
        #vis enum #ident #generics {
            #variants
        }
    };

    res.into()
}

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

    let (mut add_fns, mut inputs, mut input_names, mut input_unwraps) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
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
