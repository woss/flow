use super::{ItemRouter, RouteConstraint};
use futures::channel::oneshot;
use futures::FutureExt;
use protocol::protocol::{self as broker, journal_client::JournalClient};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tonic::transport::{channel::Connection, Endpoint};
use tower::Service;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("transport error")]
    Transport(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
}

/// Dispatcher manages transports to a dynamic set of endpoints, where
/// endpoints are discovered from observed protocol::Routes. It provides a
/// means for converting a RouteConstraint into a tower::Service that
/// dispatches the request to an endpoint that satisfies the constraint.
pub struct Dispatcher<R> {
    router: R,
    // Preferred zone to which we'll dispatch.
    local_zone: String,
    // Address of last resort (eg, a Kubernetes service).
    default_address: String,
    // Mark set on endpoints as they're used.
    // A sweep will clear idle endpoints (not having the active mark).
    active_mark: u8,
    // Map from address => (address + Connection + mark, wakers).
    // The extra address copy is passed to the caller on checkout,
    // and allows for returning the endpoint with allocating a new String.
    endpoints: HashMap<String, (Option<(String, Connection, u8)>, Vec<Waker>)>,
    // Signals periodic_sweeps() on Drop.
    _tx_drop: oneshot::Sender<()>,
}

impl<R> Dispatcher<R>
where
    R: ItemRouter + Send + 'static,
{
    /// Start a Dispatcher serving from the given zone, with the given default
    /// address. The provided ItemRouter is used to map RouteConstraints to
    /// addresses, and is the means by which the Dispatcher discovers new
    /// service endpoints.
    ///
    /// Unused endpoints are periodically swept, using the provided interval.
    pub fn start(
        zone: &str,
        address: &str,
        router: R,
        sweep: Duration,
    ) -> Arc<Mutex<Dispatcher<R>>> {
        let (tx_drop, rx_drop) = oneshot::channel();

        let d = Dispatcher {
            router,
            local_zone: zone.to_string(),
            default_address: address.to_string(),
            active_mark: 1,
            endpoints: HashMap::new(),
            _tx_drop: tx_drop,
        };
        let d = Arc::new(Mutex::new(d));

        tokio::spawn(Self::periodic_sweeps(sweep, Arc::downgrade(&d), rx_drop));

        d
    }

    /// Obtain mutable access to the Dispatcher's ItemRouter.
    pub fn router(&mut self) -> &mut R {
        &mut self.router
    }

    /// Bind this Dispatcher to a RouteConstraint, returning a Binding that
    /// implements tower::Service, and to which Requests may be dispatched.
    pub fn bind<'a>(
        dispatcher: Arc<Mutex<Self>>,
        constraint: RouteConstraint<'a>,
    ) -> Binding<'a, R> {
        Binding {
            dispatcher,
            constraint,
            ready: None,
        }
    }

    // Poll for a ready endpoint that satisfies a RouteConstraint.
    fn poll_endpoint<'a>(
        &'a mut self,
        cx: &mut Context<'_>,
        constraint: &'a RouteConstraint,
    ) -> Poll<Result<(String, Connection), Error>> {
        let Dispatcher {
            local_zone,
            router,
            default_address,
            endpoints,
            active_mark,
            ..
        } = self;

        // Identify address candidates for the RouteConstraint.
        let mut addresses = constraint.pick_candidates(router, local_zone);
        if addresses.is_empty() {
            addresses.push(default_address);
        }

        // Walk each, starting or polling for a ready endpoint.
        for addr in addresses {
            // If this endpoint is unknown, start it.
            if !endpoints.contains_key(addr) {
                tracing::debug!(addr, "starting endpoint");

                let conn = match Endpoint::from_shared(addr.to_string()) {
                    Ok(ep) => ep.transport_lazy(),
                    Err(err) => return Poll::Ready(Err(err.into())),
                };

                endpoints.insert(
                    addr.to_string(),
                    (Some((addr.to_string(), conn, 0)), Vec::new()),
                );
            }

            let (slot, notify) = endpoints.get_mut(addr).unwrap();
            match slot.take() {
                Some((addr, mut conn, _)) => match conn.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        tracing::trace!(%addr, "endpoint ready");
                        return Poll::Ready(Ok((addr, conn)));
                    }
                    Poll::Ready(Err(err)) => {
                        tracing::trace!(%addr, %err, "endpoint failed");
                        return Poll::Ready(Err(Error::Transport(err)));
                    }
                    Poll::Pending => {
                        tracing::trace!(%addr, "polled endpoint to pending");
                        *slot = Some((addr, conn, *active_mark));
                        notify.push(cx.waker().clone());
                    }
                },
                None => {
                    tracing::trace!(%addr, "endpoint already checked out");
                    notify.push(cx.waker().clone());
                }
            }
        }
        Poll::Pending
    }

    // Return a checked-out endpoint to the Dispatcher.
    fn check_in(&mut self, addr: String, conn: Connection) {
        tracing::trace!(%addr, "checked in endpoint");
        let (slot, notify) = self.endpoints.get_mut(&addr).unwrap();
        if slot.is_some() {
            panic!("checked-in endpoint is not checked out");
        }
        *slot = Some((addr, conn, self.active_mark));
        notify.drain(..).for_each(Waker::wake);
    }

    // Sweep endpoints which haven't been used since the last sweep.
    #[tracing::instrument(skip(dispatcher, rx_drop))]
    async fn periodic_sweeps(
        interval: Duration,
        dispatcher: Weak<Mutex<Dispatcher<R>>>,
        rx_drop: futures::channel::oneshot::Receiver<()>,
    ) {
        tracing::debug!("started periodic sweeps");

        // Start an interval ticker. The first tick is immediate; ignore it.
        let mut interval = tokio::time::interval(interval);
        let _ = interval.tick().await;

        // Pin & fuse for re-entrant select!.
        futures::pin_mut!(rx_drop);
        let mut rx_drop = rx_drop.fuse();

        loop {
            // Select over the next tick OR rx_drop.
            let tick = interval.tick();
            futures::pin_mut!(tick);

            futures::select!(
                _ = tick.fuse() => (),
                _ = rx_drop => (),
            );

            let d = dispatcher.upgrade(); // Weak => Arc.
            let d = match d {
                None => {
                    tracing::debug!("dispatcher dropped");
                    return;
                }
                Some(d) => d,
            };
            let mut d = d.lock().unwrap(); // Mutex => MutexGuard.

            let verify = d.active_mark;
            d.active_mark += 1;

            d.endpoints.retain(|_, (ep, _)| match ep {
                Some((addr, _, mark)) if *mark != verify => {
                    tracing::debug!(%addr, "sweeping unused endpoint");
                    false
                }
                _ => true,
            });
        }
    }
}

/// Binding combines a Dispatcher with a RouteConstraint, and applies the
/// constraint to the Dispatcher in order to implement a tower::Service.
pub struct Binding<'a, R>
where
    R: ItemRouter + Send + 'static,
{
    dispatcher: Arc<Mutex<Dispatcher<R>>>,
    constraint: RouteConstraint<'a>,
    ready: Option<(String, Connection)>,
}

type HttpRequest = http::Request<tonic::body::BoxBody>;

impl<'a, R> Service<HttpRequest> for Binding<'a, R>
where
    R: ItemRouter + Send + 'static,
{
    type Response = <Connection as Service<HttpRequest>>::Response;
    type Error = <Connection as Service<HttpRequest>>::Error;
    type Future = <Connection as Service<HttpRequest>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = self.dispatcher.lock().unwrap();

        match guard.poll_endpoint(cx, &mut self.constraint) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Ready(Ok((ep, conn))) => {
                self.ready = Some((ep, conn));
                Poll::Ready(Ok(()))
            }
        }
    }

    fn call(&mut self, req: HttpRequest) -> Self::Future {
        let (ep, mut conn) = self.ready.take().expect("not ready");
        let fut = conn.call(req);
        self.dispatcher.lock().unwrap().check_in(ep, conn);
        fut
    }
}

#[tracing::instrument(skip(dispatcher))]
async fn do_the_thing(
    index: usize,
    dispatcher: Arc<Mutex<Dispatcher<HashMap<String, broker::Route>>>>,
) {
    let constraint = RouteConstraint::ItemAny("foobar");
    let binding = Dispatcher::bind(dispatcher, constraint);

    let mut jc = JournalClient::new(binding);

    let resp = jc
        .list(broker::ListRequest {
            selector: Some(broker::LabelSelector {
                ..Default::default()
            }),
        })
        .await
        .expect("no failure");

    let status = resp.into_inner().status;
    tracing::info!(status = status, "got response");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_foo() {}

    #[tokio::test]
    async fn test_the_foo() {
        let _guard = {
            let subscriber = ::tracing_subscriber::FmtSubscriber::builder()
                .with_env_filter(::tracing_subscriber::EnvFilter::from_default_env())
                .finish();
            ::tracing::subscriber::set_default(subscriber)
        };

        tracing::trace!("trace");
        tracing::info!("info");
        tracing::warn!("warn");
        tracing::error!("error");

        let router: HashMap<String, broker::Route> = HashMap::new();

        let dispatcher = Dispatcher::start(
            "us-central1-c",
            "http://localhost:8080",
            router,
            Duration::from_secs(1),
        );

        let h1 = tokio::spawn(do_the_thing(1, dispatcher.clone()));
        let h2 = tokio::spawn(do_the_thing(2, dispatcher.clone()));
        let h3 = tokio::spawn(do_the_thing(3, dispatcher.clone()));

        let r = futures::try_join!(h1, h2, h3);
        tracing::info!(response = ?r, "all done");
    }
}
