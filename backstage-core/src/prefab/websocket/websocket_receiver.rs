// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use super::{
    Error,
    Event,
    Interface,
    Response,
    WebsocketSenderEvent,
};
use crate::core::*;
use futures::stream::{
    Stream,
    StreamExt,
};
/// The websocket receiver actor, manages the Stream from the client
pub struct WebsocketReceiver<T>
where
    T: Send + 'static + Sync + Stream<Item = Result<Message, WsError>>,
{
    sender_handle: UnboundedHandle<WebsocketSenderEvent>,
    split_stream: Option<T>,
}

impl<T> WebsocketReceiver<T>
where
    T: Send + 'static + Sync + Stream<Item = Result<Message, WsError>>,
{
    /// Create new WebsocketReceiver struct
    pub fn new(split_stream: T, sender_handle: UnboundedHandle<WebsocketSenderEvent>) -> Self {
        Self {
            sender_handle,
            split_stream: Some(split_stream),
        }
    }
}

#[async_trait::async_trait]
impl<T> ChannelBuilder<IoChannel<T>> for WebsocketReceiver<T>
where
    T: Send + 'static + Sync + Stream<Item = Result<Message, WsError>>,
{
    async fn build_channel<S>(&mut self) -> Result<IoChannel<T>, Reason> {
        if let Some(stream) = self.split_stream.take() {
            Ok(IoChannel(stream))
        } else {
            Err(Reason::Exit)
        }
    }
}

#[async_trait::async_trait]
impl<S, T> Actor<S> for WebsocketReceiver<T>
where
    S: SupHandle<Self>,
    T: Send + 'static + Sync + Stream<Item = Result<Message, WsError>> + Unpin,
{
    type Data = ();
    type Channel = IoChannel<T>;
    async fn init(&mut self, _rt: &mut Rt<Self, S>) -> Result<Self::Data, Reason> {
        Ok(())
    }
    async fn run(&mut self, rt: &mut Rt<Self, S>, _data: Self::Data) -> ActorResult {
        while let Some(Ok(message)) = rt.inbox_mut().next().await {
            // Deserialize message::text
            match message {
                Message::Text(text) => {
                    if let Ok(interface) = serde_json::from_str::<Interface>(&text) {
                        let mut targeted_scope_id_opt = interface.actor_path.destination().await;
                        match interface.event {
                            Event::Shutdown => {
                                if let Some(scope_id) = targeted_scope_id_opt.take() {
                                    if let Err(err) = rt.shutdown_scope(scope_id).await {
                                        let err_string = err.to_string();
                                        let r = Error::Shutdown(interface.actor_path, err_string);
                                        self.sender_handle.send(WebsocketSenderEvent::Result(Err(r))).ok();
                                    } else {
                                        let r = Response::Shutdown(interface.actor_path);
                                        self.sender_handle.send(WebsocketSenderEvent::Result(Ok(r))).ok();
                                    };
                                } else {
                                    let err_string = "Unreachable ActorPath".to_string();
                                    let r = Error::Shutdown(interface.actor_path, err_string);
                                    self.sender_handle.send(WebsocketSenderEvent::Result(Err(r))).ok();
                                }
                            }
                            Event::RequestServiceTree => {
                                if let Some(scope_id) = targeted_scope_id_opt.take() {
                                    if let Some(service) = rt.lookup::<Service>(scope_id).await {
                                        let r = Response::ServiceTree(service);
                                        self.sender_handle.send(WebsocketSenderEvent::Result(Ok(r))).ok();
                                    } else {
                                        let r = Error::ServiceTree("Service not available".into());
                                        self.sender_handle.send(WebsocketSenderEvent::Result(Err(r))).ok();
                                    };
                                } else {
                                    let r = Error::ServiceTree("Unreachable ActorPath".into());
                                    self.sender_handle.send(WebsocketSenderEvent::Result(Err(r))).ok();
                                }
                            }
                            Event::Cast(message_to_route) => {
                                if let Some(scope_id) = targeted_scope_id_opt.take() {
                                    let route_message = message_to_route.clone();
                                    match rt.send(scope_id, message_to_route).await {
                                        Ok(()) => {
                                            let r = Response::Sent(route_message);
                                            self.sender_handle.send(WebsocketSenderEvent::Result(Ok(r))).ok();
                                        }
                                        Err(e) => {
                                            let err = format!("{}", e);
                                            let r = Error::Cast(interface.actor_path, route_message, err);
                                            self.sender_handle.send(WebsocketSenderEvent::Result(Err(r))).ok();
                                        }
                                    };
                                } else {
                                    let r = Error::Cast(
                                        interface.actor_path,
                                        message_to_route,
                                        "Unreachable ActorPath".into(),
                                    );
                                    self.sender_handle.send(WebsocketSenderEvent::Result(Err(r))).ok();
                                }
                            }
                            Event::Call(_message_to_route_with_responder) => {
                                // todo
                                todo!()
                            }
                        }
                    } else {
                        break;
                    };
                }
                Message::Close(_) => {
                    break;
                }
                _ => {}
            }
        }
        self.sender_handle.shutdown().await;
        Ok(())
    }
}
