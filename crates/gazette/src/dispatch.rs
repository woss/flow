use super::{RouteConstraint, Router};
use protocol::protocol::{self as broker, journal_client::JournalClient};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tonic::transport::{channel::Connection, Endpoint};
use tower::{Service, ServiceExt};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("transport error")]
    Transport(Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    InvalidUri(#[from] http::uri::InvalidUri),
}

struct Dispatcher<R: Router> {
    zone: String,
    router: R,
    default: String,
    connections: HashMap<String, (Option<(String, Connection, u8)>, Vec<Waker>)>,
    active_mark: u8,
}

impl<R> Dispatcher<R>
where
    R: Router,
{
    // Poll for an endpoint and ready Connection that satisfies the RouteConstraint.
    fn poll_endpoint<'a>(
        &'a mut self,
        cx: &mut Context<'_>,
        constraint: &'a RouteConstraint,
    ) -> Poll<Result<(String, Connection), Error>> {
        let Dispatcher {
            zone,
            router,
            default,
            connections,
            active_mark,
        } = self;

        // Identify endpoint candidates for the RouteConstraint.
        let mut candidates = constraint.pick_candidates(router, zone);
        if candidates.is_empty() {
            candidates.push(default);
        }

        // Walk each, starting or polling for a ready connection.
        for ep in candidates {
            // If this endpoint is unknown, start a new connection.
            if !connections.contains_key(ep) {
                tracing::debug!(endpoint = ep, "starting connection");

                let conn = match Endpoint::from_shared(ep.to_string()) {
                    Ok(ep) => ep.transport_lazy(),
                    Err(err) => return Poll::Ready(Err(err.into())),
                };

                connections.insert(
                    ep.to_string(),
                    (Some((ep.to_string(), conn, 0)), Vec::new()),
                );
            }

            let (slot, notify) = connections.get_mut(ep).unwrap();
            match slot.take() {
                Some((ep, mut conn, _)) => match conn.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        tracing::trace!(endpoint = %ep, "connection ready");
                        return Poll::Ready(Ok((ep, conn)));
                    }
                    Poll::Ready(Err(err)) => {
                        tracing::trace!(endpoint = %ep, err = %err, "connection failed");
                        return Poll::Ready(Err(Error::Transport(err)));
                    }
                    Poll::Pending => {
                        tracing::trace!(endpoint = %ep, "polled connection to pending");
                        *slot = Some((ep, conn, *active_mark));
                        notify.push(cx.waker().clone());
                    }
                },
                None => {
                    tracing::trace!(endpoint = %ep, "connection is checked out");
                    notify.push(cx.waker().clone());
                }
            }
        }
        Poll::Pending
    }

    fn return_connection(&mut self, ep: String, conn: Connection) {
        let (slot, notify) = self.connections.get_mut(&ep).unwrap();
        if slot.is_some() {
            panic!("returned connection not checked out");
        }
        *slot = Some((ep, conn, self.active_mark));
        notify.drain(..).for_each(Waker::wake);
    }

    // Sweep connections which haven't been used since the last sweep,
    // and increase the marking value.git push --set-upstream origin gazette
    fn sweep(&mut self) {
        /*
        let verify = self.active_mark;
        self.active_mark += 1;

        self.connections.retain(|_, dc| match dc {
            DispatchConn::CheckedIn { endpoint, mark, .. } if *mark != verify => {
                tracing::debug!(endpoint = %endpoint, "sweeping unused connection");
                falsegit push --set-upstream origin gazette
            }
            _ => true,
        });
        */
    }
}

struct Binding<'a, R>
where
    R: Router + 'a,
{
    dispatcher: Arc<Mutex<Dispatcher<R>>>,
    constraint: RouteConstraint<'a>,
    ready: Option<(String, Connection)>,
}

type Request = http::Request<tonic::body::BoxBody>;

impl<'a, R> Service<Request> for Binding<'a, R>
where
    R: Router + 'a,
{
    type Response = <Connection as Service<Request>>::Response;
    type Error = <Connection as Service<Request>>::Error;
    type Future = <Connection as Service<Request>>::Future;

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

    fn call(&mut self, req: Request) -> Self::Future {
        let (ep, mut conn) = self.ready.take().expect("not ready");
        let fut = conn.call(req);
        self.dispatcher.lock().unwrap().return_connection(ep, conn);
        fut
    }
}

#[tracing::instrument(skip(dispatcher))]
async fn do_the_thing(
    index: usize,
    dispatcher: Arc<Mutex<Dispatcher<HashMap<String, broker::Route>>>>,
) {
    let binding = Binding {
        dispatcher,
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

    let status = resp.into_inner().status;
    tracing::info!(status = status, "got response");

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

        let router: HashMap<String, broker::Route> = HashMap::new();

        let dispatcher = Dispatcher {
            zone: "us-central1-c".to_string(),
            router,
            default: "http://localhost:8080".to_owned(),
            connections: HashMap::new(),
            active_mark: 1,
        };
        let dispatcher = Arc::new(Mutex::new(dispatcher));

        let h1 = tokio::spawn(do_the_thing(1, dispatcher.clone()));
        let h2 = tokio::spawn(do_the_thing(2, dispatcher.clone()));
        let h3 = tokio::spawn(do_the_thing(3, dispatcher.clone()));

        let r = futures::try_join!(h1, h2, h3);
        tracing::info!(response = ?r, "all done");
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
