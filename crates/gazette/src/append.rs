/*
use crate::dispatch::{Dispatcher, Error};
use crate::route_constraint::{ItemRouter, RouteConstraint};
use futures::stream::{Stream, StreamExt};
use protocol::protocol::{self as broker, journal_client::JournalClient};
use std::sync::{Arc, Mutex};


pub async fn append<J, S, R>(
    dispatcher: Arc<Mutex<Dispatcher<R>>>,
    journal: J,
    data: S,
) -> Result<broker::AppendResponse, Error>
where
    J: Into<String>,
    S: Stream,
    S::Item: Into<Vec<u8>>,
    R: ItemRouter + Send + 'static,
{
    let journal = journal.into();
    let constraint = RouteConstraint::ItemPrimary(&journal);
    let binding = Dispatcher::bind(dispatcher, constraint);
    let mut _jc = JournalClient::new(binding);

    /*

    data.map(|chunk| broker::AppendRequest{


    })

    jc.append(request)
    */

    Ok(broker::AppendResponse::default())
}
*/
