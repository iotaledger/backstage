use std::time::SystemTime;

use crate::{
    core::{
        AbortableUnboundedChannel,
        Actor,
        ActorResult,
        Rt,
    },
    spawn_task,
};
use async_trait::async_trait;
use futures::StreamExt;
use prometheus::{
    HistogramOpts,
    HistogramVec,
    IntCounter,
    IntCounterVec,
    Opts,
};
use rocket::{
    catch,
    fairing::{
        Fairing,
        Info,
        Kind,
    },
    Build,
    Data,
    Ignite,
    Request,
    Response,
    Rocket,
};

pub struct RocketServer<S: 'static + Send> {
    contructor: fn(&mut Rt<RocketServer<S>, S>) -> Rocket<Build>,
}

impl<S: 'static + Send> RocketServer<S> {
    pub fn new(contructor: fn(&mut Rt<RocketServer<S>, S>) -> Rocket<Build>) -> RocketServer<S> {
        RocketServer { contructor }
    }

    async fn ignite(&self, rt: &mut Rt<RocketServer<S>, S>) -> anyhow::Result<Rocket<Ignite>> {
        (self.contructor)(rt).ignite().await.map_err(|e| anyhow::anyhow!(e))
    }
}

pub enum RocketServerEvent {
    Shutdown,
}

#[async_trait]
impl<S: 'static + Send> Actor<S> for RocketServer<S> {
    type Data = rocket::Shutdown;
    type Channel = AbortableUnboundedChannel<RocketServerEvent>;

    async fn init(&mut self, rt: &mut Rt<Self, S>) -> ActorResult<Self::Data> {
        let server = self.ignite(rt).await?;
        let shutdown = server.shutdown();
        spawn_task("Rocket Server", server.launch());
        Ok(shutdown)
    }

    async fn run(&mut self, rt: &mut Rt<Self, S>, shutdown_handle: Self::Data) -> ActorResult<()> {
        while let Some(event) = rt.inbox_mut().next().await {
            match event {
                RocketServerEvent::Shutdown => {
                    shutdown_handle.notify();
                    break;
                }
            }
        }
        Ok(())
    }
}

pub struct CORS;

#[async_trait]
impl Fairing for CORS {
    fn info(&self) -> rocket::fairing::Info {
        Info {
            name: "Add CORS Headers",
            kind: Kind::Response,
        }
    }

    async fn on_response<'r>(&self, _request: &'r Request<'_>, response: &mut Response<'r>) {
        response.set_raw_header("Access-Control-Allow-Origin", "*");
        response.set_raw_header("Access-Control-Allow-Methods", "GET, OPTIONS");
        response.set_raw_header("Access-Control-Allow-Headers", "*");
        response.set_raw_header("Access-Control-Allow-Credentials", "true");
    }
}

pub struct RequestTimer {
    requests_collector: IntCounter,
    response_code_collector: IntCounterVec,
    response_time_collector: HistogramVec,
}

impl Default for RequestTimer {
    fn default() -> Self {
        Self {
            requests_collector: IntCounter::new("incoming_requests", "Incoming Requests")
                .expect("failed to create metric"),
            response_code_collector: IntCounterVec::new(
                Opts::new("response_code", "Response Codes"),
                &["statuscode", "type"],
            )
            .expect("failed to create metric"),
            response_time_collector: HistogramVec::new(
                HistogramOpts::new("response_time", "Response Times"),
                &["endpoint"],
            )
            .expect("failed to create metric"),
        }
    }
}

#[derive(Copy, Clone)]
struct TimerStart(Option<SystemTime>);

#[rocket::async_trait]
impl Fairing for RequestTimer {
    fn info(&self) -> Info {
        Info {
            name: "Request Timer",
            kind: Kind::Request | Kind::Response,
        }
    }

    /// Stores the start time of the request in request-local state.
    async fn on_request(&self, request: &mut Request<'_>, _: &mut Data<'_>) {
        // Store a `TimerStart` instead of directly storing a `SystemTime`
        // to ensure that this usage doesn't conflict with anything else
        // that might store a `SystemTime` in request-local cache.
        request.local_cache(|| TimerStart(Some(SystemTime::now())));
        self.requests_collector.inc();
    }

    /// Adds a header to the response indicating how long the server took to
    /// process the request.
    async fn on_response<'r>(&self, req: &'r Request<'_>, res: &mut Response<'r>) {
        let start_time = req.local_cache(|| TimerStart(None));
        if let Some(Ok(duration)) = start_time.0.map(|st| st.elapsed()) {
            let ms = (duration.as_secs() * 1000 + duration.subsec_millis() as u64) as f64;
            self.response_time_collector
                .with_label_values(&[&format!("{} {}", req.method(), req.uri())])
                .observe(ms)
        }
        match res.status().code {
            500..=599 => self
                .response_code_collector
                .with_label_values(&[&res.status().code.to_string(), "500"])
                .inc(),
            400..=499 => self
                .response_code_collector
                .with_label_values(&[&res.status().code.to_string(), "400"])
                .inc(),
            300..=399 => self
                .response_code_collector
                .with_label_values(&[&res.status().code.to_string(), "300"])
                .inc(),
            200..=299 => self
                .response_code_collector
                .with_label_values(&[&res.status().code.to_string(), "200"])
                .inc(),
            100..=199 => self
                .response_code_collector
                .with_label_values(&[&res.status().code.to_string(), "100"])
                .inc(),
            _ => (),
        }
    }
}

#[catch(500)]
pub fn internal_error() -> &'static str {
    "Internal server error!"
}

#[catch(404)]
pub fn not_found() -> &'static str {
    "No endpoint found!"
}
