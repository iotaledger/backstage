use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn build(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(attr as syn::AttributeArgs);
    let actor = args[0].clone();
    match actor {
        syn::NestedMeta::Meta(ref m) => match m {
            syn::Meta::Path(ref path) => {
                let actor = path.get_ident().unwrap().clone();
                let syn::ItemFn {
                    attrs: _,
                    vis,
                    sig,
                    mut block,
                } = syn::parse_macro_input!(item as syn::ItemFn);

                if let syn::ReturnType::Type(_, _) = sig.output {
                    panic!("Build function should not specify its return type!");
                }

                let syn::Signature { inputs: fn_inputs, .. } = sig;

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
                                    let add_fn = quote! {
                                        #vis fn #name (mut self, val: #ty) -> Self {
                                            self.#name.replace(val);
                                            self
                                        }
                                    };
                                    add_fns.push(add_fn);

                                    input_names.push(name.clone());

                                    input_unwraps.push(quote! {self.#name.expect("Config param #name was not provided!")});

                                    let prev_type = t.ty.clone();
                                    t.ty = syn::parse_quote! {Option<#prev_type>};
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
                    #[derive(Debug, Default, Clone)]
                    #vis struct #builder {
                        #(#inputs),*
                    }

                    impl #builder {
                        #vis fn new() -> Self {
                            Self::default()
                        }

                        #(#add_fns)*
                    }

                    impl ActorBuilder<#actor> for #builder
                    {
                        fn build(self, service: Service) -> #actor #block
                    }
                };
                res.into()
            }
            _ => {
                panic!("Invalid actor meta type!");
            }
        },
        syn::NestedMeta::Lit(_) => {
            panic!("Invalid actor specified!");
        }
    }
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

    // TODO: Add DAG for dependency checking
    let mut names = Vec::new();
    let mut deps = Vec::new();
    let mut spawns = Vec::new();
    let mut starts = Vec::new();
    let mut shutdown_matches = Vec::new();
    let mut startup_matches = Vec::new();
    let mut builder_struct_fields = Vec::new();
    let mut builder_param_fields = Vec::new();
    let mut shutdowns = Vec::new();
    let fields = fields
        .into_iter()
        .map(|mut field| {
            let (mut named, mut depended, mut actor) = (None, None, None);
            field.attrs = field
                .attrs
                .into_iter()
                .filter_map(|attr| {
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
                                                    let mut depends_on = Vec::new();
                                                    for m in list.nested.iter() {
                                                        match m {
                                                            syn::NestedMeta::Meta(m) => {
                                                                if let syn::Meta::Path(p) = m {
                                                                    depends_on.push(p.get_ident().unwrap().clone());
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
            let field_ident = field.ident.as_ref().unwrap();
            if let (Some(actor), Some(name)) = (actor, named) {
                let builder_type = &field.ty;
                let name_fn_ident = quote::format_ident!("{}_name", field_ident);
                let deps_fn_ident = quote::format_ident!("{}_deps", field_ident);
                let spawn_fn_ident = quote::format_ident!("spawn_{}", field_ident);
                builder_param_fields.push(quote! {
                    #field_ident: BuilderData<#actor, #builder_type>
                });
                builder_struct_fields.push(quote! {
                    #field_ident: BuilderData {
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
                        self.#field_ident.builder.clone().build(new_service)
                    }
                });
                startup_matches.push(quote! {
                    #name => self.#field_ident.startup(self.sender.clone()).await,
                });
                shutdown_matches.push(quote! {
                    #name => self.#field_ident.shutdown(self.sender.clone()),
                });
                shutdowns.push(quote! {
                    self.#field_ident.block_on_shutdown()
                });
                starts.push(quote! {
                    let mut #field_ident = self.#spawn_fn_ident ().await;
                    self.#field_ident.event_handle.replace(#field_ident.handle().clone());
                    let join_handle = tokio::spawn(#field_ident.start(self.sender.clone()));
                    self.#field_ident.join_handle.replace(join_handle);
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
                self.start_unsupervised().await
            }
        }

        #[async_trait::async_trait]
        impl Actor for #struct_ident {
            type Error = std::borrow::Cow<'static, str>;
            type Event = LauncherEvent;
            type Handle = LauncherSender;

            fn handle(&mut self) -> &mut Self::Handle {
                &mut self.sender
            }

            fn service(&mut self) -> &mut Service {
                panic!("Cannot access launcher service via a reference!");
            }

            async fn update_status<E, S>(&mut self, status: ServiceStatus, _supervisor: &mut S)
            where
                S: 'static + Send + EventHandle<E>,
            {
                SERVICE.write().await.update_status(status);
            }

            async fn init<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
            where
                S: 'static + Send + EventHandle<E>,
            {
                log::info!("Initializing Launcher!");
                tokio::spawn(ctrl_c(self.sender.clone()));
                SERVICE.write().await.update_status(ServiceStatus::Initializing);
                Ok(())
            }

            async fn run<E, S>(&mut self, supervisor: &mut S) -> Result<(), Self::Error>
            where
                S: 'static + Send + EventHandle<E>,
            {
                log::info!("Running Launcher!");
                SERVICE.write().await.update_status(ServiceStatus::Running);
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

            async fn shutdown<E, S>(&mut self, status: Result<(), Self::Error>, supervisor: &mut S) -> Result<ActorRequest, ActorError>
            where
                S: 'static + Send + EventHandle<E>,
            {
                log::info!("Shutting down Launcher!");
                SERVICE.write().await.update_status(ServiceStatus::Stopping);
                Ok(ActorRequest::Finish)
            }
        }
    };
    res.into()
}
