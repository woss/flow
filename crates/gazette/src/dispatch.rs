use futures::channel::oneshot;
use futures::FutureExt;
use protocol::protocol;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tonic::transport::{channel::Connection, Endpoint};
use tower::Service;

type ProcId = protocol::process_spec::Id;

/// Constraint constrains how a request may be dispatched among members.
#[derive(Debug, Copy, Clone)]
pub enum Constraint<'a> {
    /// Dispatch to the default service address.
    Default,
    /// Dispatch to the closest member of the item's route.
    ItemAny(&'a str),
    /// Dispatch to the primary member of the item's route.
    ItemPrimary(&'a str),
    /// Dispatch to the closest member of the route.
    RouteAny(&'a protocol::Route),
    /// Dispatch to the primary member of the route.
    RoutePrimary(&'a protocol::Route),
}

/// Router maps from an item to a matched Route, or None.
pub trait Router {
    fn route<'a>(&'a self, item: &str) -> Option<&'a protocol::Route>;
}

/// HashMapRouter is a concrete implementation of Router atop a HashMap.
pub type HashMapRouter = HashMap<String, protocol::Route>;

impl<K, V, S> Router for HashMap<K, V, S>
where
    K: Borrow<str> + Eq + std::hash::Hash,
    V: Borrow<protocol::Route>,
    S: std::hash::BuildHasher,
{
    fn route<'a>(&'a self, item: &str) -> Option<&'a protocol::Route> {
        self.get(item).map(Borrow::borrow)
    }
}

static EMPTY_ROUTE: protocol::Route = protocol::Route {
    members: Vec::new(),
    endpoints: Vec::new(),
    primary: -1,
};

impl<'a> Constraint<'a> {
    /// Pick endpoints to which this constrained route may be addressed.
    /// Depending on the constraint, there may be exactly one member
    /// that matches.
    ///
    /// For other scenarios (eg, we can select allow any replica of a
    /// current topology), we have some flexibility in candidate
    /// selection, and seek to pick candidates which are "closest"
    /// to our current |zone|.
    ///
    /// For example, consider a |zone| of 'gcp-us-central1-a',
    /// and Route with members having zones like: [
    ///      'aws-east1-b',
    ///      'gcp-eu-west1-a',
    ///      'gcp-us-central1-b',
    ///      'gcp-us-central1-c',
    /// ]
    ///
    /// Though there isn't an exact match, we certainly want to pick
    /// among the 'gcp-us-central1-*" zones. If we *had* an exact zone
    /// match, we'd prefer that above all others.
    pub fn pick_candidates<R: Router>(
        self,
        router: &'a R,
        local_zone: &'a str,
    ) -> impl Iterator<Item = (&'a ProcId, &'a str)> {
        let (rt, primary) = match self {
            Constraint::Default => (&EMPTY_ROUTE, false),
            Constraint::ItemAny(key) | Constraint::ItemPrimary(key) => match router.route(key) {
                None => (&EMPTY_ROUTE, false),
                Some(rt) => (
                    rt,
                    matches!(self, Constraint::ItemPrimary(..) if rt.primary != -1),
                ),
            },
            Constraint::RouteAny(rt) | Constraint::RoutePrimary(rt) => (
                rt,
                matches!(self, Constraint::RoutePrimary(..) if rt.primary != -1),
            ),
        };

        let best_prefix = rt
            .members
            .iter()
            .map(|id| common_prefix(local_zone.as_bytes(), id.zone.as_bytes()))
            .max()
            .unwrap_or_default();

        rt.members
            .iter()
            .zip(rt.endpoints.iter())
            .enumerate()
            .filter_map(move |(ind, (id, ep))| {
                if primary {
                    if ind == rt.primary as usize {
                        Some((id, ep.as_ref()))
                    } else {
                        None
                    }
                } else if common_prefix(local_zone.as_bytes(), id.zone.as_bytes()) == best_prefix {
                    Some((id, ep.as_ref()))
                } else {
                    None
                }
            })
    }

    /// Query returns a future which resolves this Constraint against the Dispatcher.
    pub fn query<R: Router>(self, dispatcher: Arc<Mutex<Dispatcher<R>>>) -> Query<'a, R> {
        Query {
            dispatcher,
            constraint: self,
        }
    }
}

#[inline]
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .enumerate()
        .filter_map(|(i, (a, b))| if a != b { Some(i) } else { None })
        .next()
        .unwrap_or(std::cmp::min(a.len(), b.len()))
}

#[cfg(test)]
mod test {
    use super::{protocol::Route, *};
    use serde_json::json;

    #[test]
    fn route_constraint_cases() {
        let route = vec![
            (
                "hit".to_string(),
                serde_json::from_value::<Route>(json!({
                    "members": [
                        { "zone": "aws-east1-a", "suffix": "one" },
                        { "zone": "aws-east1-b", "suffix": "two" },
                        { "zone": "gcp-west2-c", "suffix": "three" },
                    ],
                    "endpoints": ["http://one/a", "http://two/b", "http://three/c"],
                    "primary": 1,
                }))
                .unwrap(),
            ),
            (
                "bad".to_string(),
                serde_json::from_value::<Route>(json!({
                    "members": [
                        { "zone": "aws-east1-a", "suffix": "one" },
                        { "zone": "aws-east1-b", "suffix": "two" },
                        { "zone": "gcp-west2-c", "suffix": "three" },
                    ],
                    "endpoints": [],
                    "primary": -12345,
                }))
                .unwrap(),
            ),
        ]
        .into_iter()
        .collect::<HashMapRouter>();

        let do_case = |c: Constraint, zone: &str| -> Vec<String> {
            c.pick_candidates(&route, zone)
                .map(|(_, a)| a.to_string())
                .collect()
        };

        assert!(do_case(Constraint::Default, "aws-east1-a").is_empty());
        // Within a served zone, we prefer the matched endpoint.
        assert_eq!(
            do_case(Constraint::ItemAny("hit"), "aws-east1-b"),
            vec!["http://two/b"],
        );
        // Within a region, we prefer zones of that region.
        assert_eq!(
            do_case(Constraint::ItemAny("hit"), "aws-east1-z"),
            vec!["http://one/a", "http://two/b"],
        );
        // Within a cloud, we prefer the same cloud.
        assert_eq!(
            do_case(Constraint::ItemAny("hit"), "gcp-something"),
            vec!["http://three/c"],
        );
        // We'll traverse whatever we must, to get to the primary if required.
        assert_eq!(
            do_case(Constraint::ItemPrimary("hit"), "gcp-something"),
            vec!["http://two/b"],
        );
        // Item route isn't known.
        assert!(do_case(Constraint::ItemAny("miss"), "gcp-something").is_empty());
        assert!(do_case(Constraint::ItemPrimary("miss"), "gcp-something").is_empty());
        // Route applies prior knowledge of the route to use.
        assert_eq!(
            do_case(
                Constraint::RouteAny(route.route("hit").unwrap()),
                "aws-east1-z"
            ),
            vec!["http://one/a", "http://two/b"],
        );
        // Route applies prior knowledge, and desires the primary.
        assert_eq!(
            do_case(
                Constraint::RoutePrimary(route.route("hit").unwrap()),
                "aws-east1-z"
            ),
            vec!["http://one/a", "http://two/b"],
        );

        // We handle a malformed Route by ignoring broken bits.
        assert!(do_case(Constraint::ItemAny("bad"), "aws-something").is_empty());
        assert!(do_case(Constraint::ItemPrimary("bad"), "aws-something").is_empty());
    }

    #[test]
    fn test_prefix_lengths() {
        assert_eq!(common_prefix(b"foobar", b"foorab"), 3);
        assert_eq!(common_prefix(b"foobar", b"FOORAB"), 0);
        assert_eq!(common_prefix(b"aws-east1-a", b"aws-east1-a"), 11);
        assert_eq!(common_prefix(b"aws-east1-a", b"aws-east1"), 9);
        assert_eq!(common_prefix(b"aws-east1-a", b"aws-west1"), 4);
    }
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
    endpoints: HashMap<ProcId, (Option<(ProcId, Connection, u8)>, Vec<Waker>)>,
    // Signals periodic_sweeps() on Drop.
    _tx_drop: oneshot::Sender<()>,
}

// Re-exported Tonic error type (which is a boxed dyn Error).
pub type Error = <Connection as Service<HttpRequest>>::Error;

impl<R> Dispatcher<R>
where
    R: Router + Send + 'static,
{
    /// Start a Dispatcher serving from the given zone, with the given default
    /// address. The provided Router is used to map Constraints to addresses,
    /// and is the means by which the Dispatcher discovers new service endpoints.
    ///
    /// Unused endpoints are periodically swept, using the provided interval.
    pub fn start(
        router: R,
        local_zone: &str,
        default_address: &str,
        sweep: Duration,
    ) -> Arc<Mutex<Dispatcher<R>>> {
        let (tx_drop, rx_drop) = oneshot::channel();

        let d = Dispatcher {
            router,
            local_zone: local_zone.to_string(),
            default_address: default_address.to_string(),
            active_mark: 1,
            endpoints: HashMap::new(),
            _tx_drop: tx_drop,
        };
        let d = Arc::new(Mutex::new(d));
        tokio::spawn(Self::periodic_sweeps(sweep, Arc::downgrade(&d), rx_drop));
        d
    }

    /// Obtain mutable access to the Dispatcher's Router.
    pub fn router(&mut self) -> &mut R {
        &mut self.router
    }

    // Poll for a ready endpoint that satisfies a RouteConstraint.
    fn poll_endpoint<'a>(
        &mut self,
        cx: &mut Context<'_>,
        constraint: Constraint<'a>,
    ) -> Poll<Result<(ProcId, Connection), Error>> {
        let Dispatcher {
            default_address,
            endpoints,
            active_mark,
            router,
            local_zone,
            ..
        } = self;

        let default_id = ProcId::default();
        let mut empty = true;

        let mut candidates = constraint.pick_candidates(router, local_zone);

        loop {
            let (id, addr) = match candidates.next() {
                Some(i) => i,
                None if empty => (&default_id, default_address.as_ref()),
                None => return Poll::Pending,
            };
            empty = false;

            // If this endpoint is unknown, start it.
            if !endpoints.contains_key(id) {
                tracing::debug!(?id, addr, "starting endpoint");

                let conn = match Endpoint::from_shared(addr.to_string()) {
                    Ok(ep) => ep.transport_lazy(),
                    Err(err) => return Poll::Ready(Err(err.into())),
                };
                endpoints.insert(id.clone(), (Some((id.clone(), conn, 0)), Vec::new()));
            }

            let (slot, notify) = endpoints.get_mut(id).unwrap();
            match slot.take() {
                Some((id, mut conn, _)) => match conn.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        tracing::trace!(?id, "endpoint ready");
                        return Poll::Ready(Ok((id, conn)));
                    }
                    Poll::Ready(Err(err)) => {
                        tracing::trace!(?id, %err, "endpoint failed");
                        return Poll::Ready(Err(err));
                    }
                    Poll::Pending => {
                        tracing::trace!(?id, "polled endpoint to pending");
                        *slot = Some((id, conn, *active_mark));
                        notify.push(cx.waker().clone());
                    }
                },
                None => {
                    tracing::trace!(?id, "endpoint already checked out");
                    notify.push(cx.waker().clone());
                }
            }
        }
    }

    // Return a checked-out endpoint to the Dispatcher.
    fn check_in(&mut self, id: ProcId, conn: Connection) {
        tracing::trace!(?id, "checked in endpoint");
        let (slot, notify) = self.endpoints.get_mut(&id).unwrap();
        if slot.is_some() {
            panic!("checked-in endpoint is not checked out");
        }
        *slot = Some((id, conn, self.active_mark));
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
                Some((id, _, mark)) if *mark != verify => {
                    tracing::debug!(?id, "sweeping unused endpoint");
                    false
                }
                _ => true,
            });
        }
    }
}

/// Query is a Future which resolves a Constraint against a Dispatcher.
pub struct Query<'a, R>
where
    R: Router,
{
    dispatcher: Arc<Mutex<Dispatcher<R>>>,
    constraint: Constraint<'a>,
}

/// Checkout is a checked out, ready-to-use tower::Service to which
/// a single request may be dispatched.
pub struct Checkout<R>
where
    R: Router + Send + 'static,
{
    dispatcher: Arc<Mutex<Dispatcher<R>>>,
    conn: Option<(ProcId, Connection)>,
}

impl<'a, R> std::future::Future for Query<'a, R>
where
    R: Router + Send + 'static,
{
    type Output = Result<Checkout<R>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.dispatcher.lock().unwrap();

        match guard.poll_endpoint(cx, self.constraint) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Ready(Ok((id, conn))) => Poll::Ready(Ok(Checkout {
                dispatcher: self.dispatcher.clone(),
                conn: Some((id, conn)),
            })),
        }
    }
}

type HttpRequest = http::Request<tonic::body::BoxBody>;

impl<R> Service<HttpRequest> for Checkout<R>
where
    R: Router + Send + 'static,
{
    type Response = <Connection as Service<HttpRequest>>::Response;
    type Error = Error;
    type Future = <Connection as Service<HttpRequest>>::Future;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // We've already polled to ready.
    }

    fn call(&mut self, req: HttpRequest) -> Self::Future {
        let (id, mut conn) = self.conn.take().expect("dispatch checkout already used");
        let fut = conn.call(req);
        self.dispatcher.lock().unwrap().check_in(id, conn);
        fut
    }
}

impl<R: Router + Send + 'static> Drop for Checkout<R> {
    fn drop(&mut self) {
        if let Some((id, conn)) = self.conn.take() {
            self.dispatcher.lock().unwrap().check_in(id, conn);
        }
    }
}

impl<R: Router + Send + 'static> Checkout<R> {
    pub fn process_id(&self) -> &ProcId {
        &self
            .conn
            .as_ref()
            .expect("dispatch checkout already used")
            .0
    }
}

#[tracing::instrument(skip(dispatcher))]
async fn do_the_thing(index: usize, dispatcher: Arc<Mutex<Dispatcher<HashMapRouter>>>) {
    let binding = Constraint::Default.query(dispatcher.clone()).await.unwrap();
    tracing::debug!(id = ?binding.process_id(), "bound request to checkout");
    let mut jc = protocol::journal_client::JournalClient::new(binding);

    let resp = jc
        .list(protocol::ListRequest {
            selector: Some(protocol::LabelSelector {
                ..Default::default()
            }),
        })
        .await
        .expect("failed to list journals");

    let resp = resp.into_inner();

    // Update route cache with list response.
    {
        let mut g = dispatcher.lock().unwrap();

        for j in resp.journals.into_iter() {
            if let (Some(spec), Some(route)) = (j.spec, j.route) {
                if !g.router().contains_key(&spec.name) {
                    tracing::debug!(%spec.name, "caching route");
                    g.router().insert(spec.name, route);
                }
            }
        }
    }

    tracing::info!(resp.status, "got list response");

    let journal = "recovery/derivation/stock/daily-stats/00-0000000000000000".to_string();

    let resp = crate::append::append(
        dispatcher,
        protocol::AppendRequest {
            journal,
            ..Default::default()
        },
        futures::stream::once(async { b"hello, world".to_vec() }),
    )
    .await
    .unwrap();

    tracing::info!(?resp, "got append response");
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

        let dispatcher = Dispatcher::start(
            HashMapRouter::new(),
            "us-central1-c",
            "http://localhost:8080",
            Duration::from_secs(1),
        );

        let h1 = tokio::spawn(do_the_thing(1, dispatcher.clone()));
        let h2 = tokio::spawn(do_the_thing(2, dispatcher.clone()));
        let h3 = tokio::spawn(do_the_thing(3, dispatcher.clone()));

        let r = futures::try_join!(h1, h2, h3).unwrap();
        tracing::info!(?r, "all done");
    }
}
