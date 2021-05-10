use daggy::{Dag, NodeIndex};
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;

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
                p.path.get_ident().unwrap()
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

    let (generics, gen_list, bounds) = if !generics.params.is_empty() {
        let params = generics.params;
        (quote! {}, quote! {#params}, quote! {})
    } else {
        (quote! {<E, S>}, quote! {E, S}, quote! {where S: 'static + Send + EventHandle<E>,})
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

    block.stmts.insert(
        0,
        syn::parse_quote! {
            let (#(#input_names),*) = (#(#input_unwraps),*);
        },
    );

    let res = quote! {
        #(#attrs)*
        #[derive(Default)]
        #vis struct #builder {
            #(#inputs),*
        }

        impl #builder {
            #vis fn new() -> Self {
                Self::default()
            }

            #(#add_fns)*
        }

        impl #generics ActorBuilder<#actor, #gen_list> for #builder
        #bounds
        {
            fn build(self, service: Service) -> #actor #block
        }
    };
    res.into()
}

#[proc_macro_attribute]
pub fn launcher(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // let args = syn::parse_macro_input!(attr as syn::AttributeArgs);
    let syn::ItemStruct {
        attrs: _,
        vis,
        struct_token: _,
        ident: struct_ident,
        generics,
        fields,
        semi_token: _,
    } = syn::parse_macro_input!(item as syn::ItemStruct);

    let mut dependencies = HashMap::<String, (NodeIndex<u32>, Vec<String>)>::new();
    let mut dag = Dag::<(), (), u32>::new();
    let mut names = Vec::new();
    let mut deps = Vec::new();
    let mut spawns = Vec::new();
    let mut starts = Vec::new();
    let mut shutdown_matches = Vec::new();
    let mut startup_matches = Vec::new();
    let mut dependency_matches = Vec::new();
    let mut builder_struct_fields = Vec::new();
    let mut builder_param_fields = Vec::new();
    let mut shutdowns = Vec::new();
    let fields = fields
        .into_iter()
        .map(|mut field| {
            let (mut named, mut depended, mut actor) = (None, None, None);
            let field_ident = field.ident.as_ref().unwrap();
            field.attrs = field
                .attrs
                .into_iter()
                .filter_map(|attr| {
                    dependencies.insert(field_ident.to_string(), (dag.add_node(()), Vec::new()));
                    match attr.parse_meta().unwrap() {
                        syn::Meta::List(l) => {
                            if let Some(ident) = l.path.get_ident() {
                                actor = Some(ident.clone());
                                for nested in l.nested.iter() {
                                    match nested {
                                        syn::NestedMeta::Meta(m) => match m {
                                            syn::Meta::List(list) => match list.path.get_ident().unwrap().to_string().as_str() {
                                                "depends_on" => {
                                                    if depended.is_some() {
                                                        panic!("Already specified dependencies for this actor!");
                                                    }
                                                    let dep_vec = &mut dependencies.get_mut(&field_ident.to_string()).unwrap().1;
                                                    let mut depends_on = Vec::new();
                                                    for m in list.nested.iter() {
                                                        match m {
                                                            syn::NestedMeta::Meta(m) => {
                                                                if let syn::Meta::Path(p) = m {
                                                                    let id = p.get_ident().unwrap();
                                                                    dep_vec.push(id.to_string());
                                                                    depends_on.push(id.clone());
                                                                } else {
                                                                    panic!("Dependencies must be a struct field!");
                                                                }
                                                            }
                                                            syn::NestedMeta::Lit(_) => panic!("Dependencies must be a struct field!"),
                                                        }
                                                    }
                                                    depended = Some(depends_on);
                                                }
                                                _ => panic!("Invalid path found!"),
                                            },
                                            syn::Meta::NameValue(nv) => {
                                                if let Some(i) = nv.path.get_ident() {
                                                    if i.to_string() == "name" {
                                                        if named.is_some() {
                                                            panic!("Already named this actor!");
                                                        }
                                                        if let syn::Lit::Str(ref name) = nv.lit {
                                                            named = Some(name.value());
                                                        } else {
                                                            panic!("Name must be a string literal!")
                                                        }
                                                    }
                                                }
                                            }
                                            syn::Meta::Path(_) => panic!("Invalid path found!"),
                                        },
                                        syn::NestedMeta::Lit(lit) => {
                                            if let syn::Lit::Str(s) = lit {
                                                if named.is_some() {
                                                    panic!("Already named this actor!");
                                                }
                                                named = Some(s.value());
                                            }
                                        }
                                    }
                                }
                                if named.is_none() {
                                    named = Some(ident.to_string());
                                }
                                return None;
                            }
                        }
                        syn::Meta::NameValue(_) => panic!("Invalid name-value found!"),
                        syn::Meta::Path(p) => {
                            if let Some(ident) = p.get_ident() {
                                actor = Some(ident.clone());
                                if named.is_none() {
                                    named = Some(ident.to_string());
                                }
                            }
                            return None;
                        }
                    }
                    Some(attr)
                })
                .collect();
            if let (Some(actor), Some(name)) = (actor, named) {
                let builder_type = &field.ty;
                let name_fn_ident = quote::format_ident!("{}_name", field_ident);
                let deps_fn_ident = quote::format_ident!("{}_deps", field_ident);
                let spawn_fn_ident = quote::format_ident!("spawn_{}", field_ident);
                builder_param_fields.push(quote! {
                    #field_ident: BuilderData<#actor, #builder_type, LauncherEvent, LauncherSender>
                });
                builder_struct_fields.push(quote! {
                    #field_ident: BuilderData::<_, _, LauncherEvent, LauncherSender> {
                        name: #name.into(),
                        builder: #field_ident,
                        event_handle: None,
                        join_handle: None,
                    }
                });
                names.push(quote! {
                    #vis fn #name_fn_ident () -> String {
                        #name.into()
                    }
                });
                spawns.push(quote! {
                    #vis async fn #spawn_fn_ident (&self) -> #actor {
                        let new_service = SERVICE.write().await.spawn(#name);
                        ActorBuilder::<_, LauncherEvent, LauncherSender>::build(self.#field_ident.builder.clone(), new_service)
                    }
                });
                startup_matches.push(quote! {
                    #name => self.#field_ident.startup(self.sender.clone()).await,
                });
                shutdown_matches.push(quote! {
                    #name => self.#field_ident.shutdown(self.sender.clone()),
                });
                dependency_matches.push(quote! {
                    #name => self.#deps_fn_ident,
                });
                shutdowns.push(quote! {
                    self.#field_ident.block_on_shutdown()
                });
                if let Some(parents) = depended.as_ref() {
                    for parent in parents.iter() {
                        starts.push(quote! {
                            if self.#parent.join_handle.is_none() {
                                log::info!("Starting {} as a dependency of {}...", self.#parent.name, self.#field_ident.name);
                                self.#parent.startup(self.sender.clone()).await;
                            }
                        });
                    }
                }
                starts.push(quote! {
                    if self.#field_ident.join_handle.is_none() {
                        self.#field_ident.startup(self.sender.clone()).await;
                    }
                });
                if let Some(depended) = depended {
                    let depended = depended.iter().map(|id| quote::format_ident!("{}_name", id)).collect::<Vec<_>>();
                    deps.push(quote! {
                        #vis fn #deps_fn_ident (&self) -> Option<Vec<String>> {
                            Some(vec![#(Self::#depended ()),*])
                        }
                    });
                } else {
                    deps.push(quote! {
                        #vis fn #deps_fn_ident (&self) -> Option<Vec<String>> {
                            None
                        }
                    });
                }
            } else {
                builder_param_fields.push(quote! {#field});
                builder_struct_fields.push(quote! {#field_ident});
            }
            field
        })
        .collect::<Vec<_>>();
    for (child, (child_node_id, parents)) in dependencies.iter() {
        for parent in parents.iter() {
            if parent == child {
                panic!("{} is dependent on itself!", child);
            } else {
                if let Some((parent_node_id, _)) = dependencies.get(parent) {
                    if let Err(_) = dag.add_edge(parent_node_id.clone(), child_node_id.clone(), ()) {
                        panic!("Cyclical dependencies defined involving {} and {}!", child, parent);
                    }
                } else {
                    panic!("{} is dependent on {}, but it does not exist!", child, parent);
                }
            }
        }
    }

    let res = quote! {
        #vis struct #struct_ident #generics {
            #(#builder_param_fields),*,
            inbox: tokio::sync::mpsc::UnboundedReceiver<LauncherEvent>,
            sender: LauncherSender,
            consumed_ctrl_c: bool,
        }

        impl #struct_ident {
            #(#names)*

            #(#deps)*

            #(#spawns)*

            async fn shutdown_all(&mut self) {
                futures::join!(#(#shutdowns),*);
            }

            #vis fn new(#(#fields),*) -> Self {
                let (sender, inbox) = tokio::sync::mpsc::unbounded_channel::<LauncherEvent>();
                Self {
                    #(#builder_struct_fields),*,
                    inbox,
                    sender: LauncherSender(sender),
                    consumed_ctrl_c: false,
                }
            }

            #vis fn execute<F: FnMut(&mut Self)>(mut self, mut f: F) -> Self {
                f(&mut self);
                self
            }

            #vis async fn execute_async<F: FnOnce(Self) -> O, O: std::future::Future<Output=Self>>(self, f: F) -> Self {
                f(self).await
            }

            #vis async fn launch(self) -> Result<ActorRequest, ActorError> {
                self.start(NullSupervisor).await
            }
        }

        #[async_trait::async_trait]
        impl Actor<(), NullSupervisor> for #struct_ident {
            type Error = std::borrow::Cow<'static, str>;

            fn service(&mut self) -> &mut Service {
                panic!("Cannot access launcher service via a reference!");
            }

            async fn update_status(&mut self, status: ServiceStatus, _supervisor: &mut NullSupervisor)
            {
                SERVICE.write().await.update_status(status);
            }

            async fn init(&mut self, supervisor: &mut NullSupervisor) -> Result<(), Self::Error>
            {
                log::info!("Initializing Launcher!");
                tokio::spawn(ctrl_c(self.sender.clone()));
                Ok(())
            }

            async fn run(&mut self, supervisor: &mut NullSupervisor) -> Result<(), Self::Error>
            {
                log::info!("Running Launcher!");
                #(#starts)*
                while let Some(evt) = self.inbox.recv().await {
                    match evt {
                        LauncherEvent::StartActor(name) => {
                            log::info!("Starting actor: {}", name);
                            match name.as_str() {
                                #(#startup_matches)*
                                _ => log::error!("No builder with name {}!", name),
                            }
                        }
                        LauncherEvent::ShutdownActor(name) => {
                            log::info!("Shutting down actor: {}", name);
                            match name.as_str() {
                                #(#shutdown_matches)*
                                _ => log::error!("No actor with name {}!", name),
                            }
                        }
                        LauncherEvent::StatusChange(service) => {
                            let (status, name) = (service.status, service.name.clone());
                            SERVICE.write().await.insert_or_update_microservice(service);
                        }
                        LauncherEvent::ExitProgram { using_ctrl_c } => {
                            if using_ctrl_c && self.consumed_ctrl_c {
                                panic!("Exiting due to ctrl-c spam! Uh oh...");
                            }
                            if using_ctrl_c {
                                tokio::spawn(ctrl_c(self.sender.clone()));
                                self.consumed_ctrl_c = true;
                            }
                            if !SERVICE.read().await.is_stopping() {
                                log::info!("Shutting down the launcher and all sub-actors!");
                                SERVICE.write().await.update_status(ServiceStatus::Stopping);
                                log::info!("Waiting for all children to shut down...");
                                self.shutdown_all().await;
                                break;
                            }
                        }
                        _ => ()
                    }
                }
                Ok(())
            }

            async fn shutdown(&mut self, status: Result<(), Self::Error>, supervisor: &mut NullSupervisor) -> Result<ActorRequest, ActorError>
            {
                log::info!("Shutting down Launcher!");
                Ok(ActorRequest::Finish)
            }
        }

        impl EventActor<(), NullSupervisor> for #struct_ident {
            type Event = LauncherEvent;
            type Handle = LauncherSender;

            fn handle(&mut self) -> &mut Self::Handle {
                &mut self.sender
            }
        }
    };
    res.into()
}
