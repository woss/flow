use crate::dispatch::{Constraint, Dispatcher, Error as DispatchError, Router};
use ::protocol::protocol::{self as broker, journal_client::JournalClient};
use broker::AppendRequest;
use futures::stream::{Stream, StreamExt};
use std::fs::File as StdFile;
use std::sync::{Arc, Mutex};

struct PendingAppend {
    fb: StdFile,
}

pub async fn append<S, R>(
    dispatcher: Arc<Mutex<Dispatcher<R>>>,
    request: broker::AppendRequest,
    data: S,
) -> Result<broker::AppendResponse, DispatchError>
where
    S: Stream + Send + Sync + 'static,
    S::Item: Into<Vec<u8>>,
    R: Router + Send + 'static,
{
    let constraint = Constraint::ItemPrimary(&request.journal);
    let binding = constraint.query(dispatcher).await.unwrap();
    let mut jc = JournalClient::new(binding);

    let s = futures::stream::once(async move { request })
        .chain(data.map(|d| AppendRequest {
            content: d.into(),
            ..Default::default()
        }))
        .chain(futures::stream::once(async { AppendRequest::default() }));

    let resp = jc.append(s).await?.into_inner();

    /*
    if let Some(h) = resp.header.as_mut() {
        if let Some(route) = h.route.take() {
            let r = dispatcher.lock().unwrap().router().observe_route(h.route.take());
        }
    }
    */

    Ok(resp)
}
