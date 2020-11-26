use super::RouteConstraint;
use protocol::protocol::{self as broker, journal_client::JournalClient};
use std::collections::HashMap;
use std::task::{Context, Poll};
use tonic::transport::{channel::Connection, Endpoint};
use tower::{Service, ServiceExt};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("transport error")]
    Transport(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
}

trait ItemRouter {
    fn route<'s>(&'s self, item: &str) -> Option<&'s broker::Route>;
}

struct Dispatcher<R> {
    zone: String,
    router: R,
    default: String,

    connections: HashMap<String, (Connection, u8)>,
    mark: u8,
}

impl<'a, R> Dispatcher<R>
where
    R: Fn(&str) -> Option<&'a broker::Route>,
{
    // Poll for an endpoint and ready Connection that satisfies the RouteConstraint.
    fn poll_endpoint(
        &'a mut self,
        cx: &mut Context<'_>,
        constraint: &'a RouteConstraint,
    ) -> Poll<Result<(String, Connection), Error>> {
        let Dispatcher {
            zone,
            router,
            default,
            connections,
            mark,
        } = self;

        // Identify endpoint candidates for the RouteConstraint.
        let mut candidates = constraint.pick_candidates(router, zone);
        if candidates.is_empty() {
            candidates.push(default);
        }

        // Walk each, starting or polling for a ready connection.
        for ep in candidates {
            let (ep, mut conn) = match connections.remove_entry(ep) {
                None => {
                    tracing::debug!(endpoint = ep, "starting connection");

                    let conn = match Endpoint::from_shared(ep.to_string()) {
                        Ok(ep) => ep.transport_lazy(),
                        Err(err) => return Poll::Ready(Err(err.into())),
                    };
                    (ep.to_string(), conn)
                }
                Some((ep, (conn, _))) => (ep, conn),
            };

            match conn.poll_ready(cx) {
                Poll::Pending => {
                    connections.insert(ep, (conn, *mark));
                }
                Poll::Ready(Ok(())) => return Poll::Ready(Ok((ep, conn))),
                Poll::Ready(Err(err)) => return Poll::Ready(Err(Error::Transport(err))),
            }
        }

        Poll::Pending
    }

    fn return_connection(&mut self, ep: String, conn: Connection) {
        self.connections.insert(ep, (conn, self.mark));
    }

    // Sweep connections which haven't been used since the last sweep,
    // and increase the marking value.
    fn sweep(&mut self) {
        let m = self.mark;
        self.connections.retain(|k, (_, mark)| *mark == m);
        self.mark += 1;
    }
}

struct Binding<'a, R> {
    dispatcher: &'a mut Dispatcher<R>,
    constraint: RouteConstraint<'a>,
    ready: Option<(String, Connection)>,
}

type Request = http::Request<tonic::body::BoxBody>;

impl<'c, Router> Service<Request> for Binding<'c, Router>
where
    Router: Fn(&str) -> Option<&broker::Route>,
{
    type Response = <Connection as Service<Request>>::Response;
    type Error = <Connection as Service<Request>>::Error;
    type Future = <Connection as Service<Request>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.dispatcher.poll_endpoint(cx, &mut self.constraint) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Ready(Ok((ep, conn))) => {
                self.ready = Some((ep, conn));
                Poll::Ready(Ok(()))
            }
        }
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (ep, mut conn) = self.ready.take().expect("not ready");
        let fut = conn.call(req);
        self.dispatcher.return_connection(ep, conn);
        fut
    }
}

async fn do_the_thing() {
    let router /*: Fn(&str) -> Option<&broker::Route>*/ = |_| None;

    let mut dispatcher = Dispatcher {
        zone: "us-central1-c".to_string(),
        router,
        default: "http://localhost:8080".to_owned(),
        connections: HashMap::new(),
        mark: 1,
    };

    let binding = Binding {
        dispatcher: &mut dispatcher,
        constraint: RouteConstraint::ItemAny("foobar"),
        ready: None,
    };

    let mut jc = JournalClient::new(binding);

    let resp = jc
        .list(broker::ListRequest {
            selector: Some(broker::LabelSelector {
                ..Default::default()
            }),
        })
        .await
        .expect("no failure");

    tracing::info!(?resp);

    /*

    let ep = Endpoint::from_static("http://localhost:8080");
    let mut conn = ep.transport_lazy();

    let os = conn.ready_oneshot();
    {
        let _guard = tracing::span!(tracing::Level::TRACE, "connecting");
        conn = os.await.expect("failed to connect");
    }

    let mut jc = JournalClient::new(conn);

    let resp = jc
        .list(broker::ListRequest {
            selector: Some(broker::LabelSelector {
                ..Default::default()
            }),
        })
        .await
        .expect("no failure");

    tracing::info!(?resp);
    */
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

        do_the_thing().await;
    }
}

/*
#[derive(Clone, Hash, Eq, PartialEq, Default)]
struct ProcessId {
    zone: String,
    suffix: String,
}

impl From<broker::process_spec::Id> for ProcessId {
    fn from(s: broker::process_spec::Id) -> Self {
        Self {
            zone: s.zone,
            suffix: s.suffix,
        }
    }
}
*/
