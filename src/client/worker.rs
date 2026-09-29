use std::{
    collections::BTreeMap,
    future::Future,
    ops::ControlFlow,
    panic::{
        resume_unwind,
        AssertUnwindSafe,
    },
    time::Duration,
};

use convex_sync_types::{
    backoff::Backoff,
    UdfPath,
};
use futures::FutureExt;
use tokio::{
    sync::{
        broadcast,
        mpsc,
        oneshot,
        watch,
    },
    task::JoinError,
};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    base_client::{
        AuthTokenFetcher,
        BaseConvexClient,
        SubscriberId,
    },
    client::{
        ClientEvent,
        ClientObserver,
        QueryResults,
        QuerySubscription,
    },
    sync::{
        ProtocolResponse,
        ReconnectProtocolReason,
        ReconnectRequest,
        SyncProtocol,
        WebSocketState,
    },
    value::Value,
    FunctionResult,
};

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(15);

pub enum ClientRequest {
    Mutation(
        MutationRequest,
        oneshot::Sender<oneshot::Receiver<FunctionResult>>,
    ),
    Action(
        ActionRequest,
        oneshot::Sender<oneshot::Receiver<FunctionResult>>,
    ),
    Subscribe(
        SubscribeRequest,
        oneshot::Sender<QuerySubscription>,
        mpsc::UnboundedSender<ClientRequest>,
    ),
    Unsubscribe(UnsubscribeRequest),
    Authenticate(Option<AuthTokenFetcher>),
}

pub struct MutationRequest {
    pub udf_path: UdfPath,
    pub args: BTreeMap<String, Value>,
}

pub struct ActionRequest {
    pub udf_path: UdfPath,
    pub args: BTreeMap<String, Value>,
}

pub struct SubscribeRequest {
    pub udf_path: UdfPath,
    pub args: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub struct UnsubscribeRequest {
    pub subscriber_id: SubscriberId,
}

pub fn worker<T: SyncProtocol>(
    protocol_response_receiver: mpsc::Receiver<ProtocolResponse>,
    client_request_receiver: mpsc::UnboundedReceiver<ClientRequest>,
    watch_sender: broadcast::Sender<QueryResults>,
    base_client: BaseConvexClient,
    mut protocol_manager: T,
    observer: Option<ClientObserver>,
    mut shutdown: watch::Receiver<bool>,
) -> impl Future<Output = Result<(), JoinError>> + Send {
    struct NotifyClosed(Option<ClientObserver>);
    impl Drop for NotifyClosed {
        fn drop(&mut self) {
            if let Some(observer) = &self.0 {
                observer(ClientEvent::Closed);
            }
        }
    }
    // Construct the guard before spawning: the last client can be dropped
    // before this future is polled for the first time.
    let notify_closed = NotifyClosed(observer.clone());
    async move {
        let _notify_closed = notify_closed;
        // Cancel the whole loop, including a pending auth callback or send. Closing
        // must not wait for another application request to reach the worker queue.
        let result = AssertUnwindSafe(async {
            tokio::select! { biased;
                _ = shutdown.changed() => {},
                () = run(
                    protocol_response_receiver,
                    client_request_receiver,
                    watch_sender,
                    base_client,
                    &mut protocol_manager,
                    observer,
                ) => {},
            }
        })
        .catch_unwind()
        .await;
        // Preserve the panic after joining the transport. Unwinding directly
        // would only request its abort, allowing close to finish too early.
        let closed = protocol_manager.close().await;
        if let Err(panic) = result {
            resume_unwind(panic);
        }
        closed
    }
}

async fn run<T: SyncProtocol>(
    mut protocol_response_receiver: mpsc::Receiver<ProtocolResponse>,
    mut client_request_receiver: mpsc::UnboundedReceiver<ClientRequest>,
    mut watch_sender: broadcast::Sender<QueryResults>,
    mut base_client: BaseConvexClient,
    protocol_manager: &mut T,
    observer: Option<ClientObserver>,
) {
    let mut backoff = Backoff::new(INITIAL_BACKOFF, MAX_BACKOFF);
    loop {
        let e = loop {
            match _worker_once(
                &mut protocol_response_receiver,
                &mut client_request_receiver,
                &mut watch_sender,
                &mut base_client,
                protocol_manager,
                observer.as_ref(),
            )
            .await
            {
                Ok(ControlFlow::Continue(())) => backoff.reset(),
                Ok(ControlFlow::Break(())) => return,
                Err(e) => break e,
            }
        };

        if let Some(observer) = &observer {
            // A protocol/auth failure also invalidates the view before any
            // reconnect backoff, even when the transport socket remains open.
            observer(ClientEvent::ConnectionState(WebSocketState::Connecting));
        }

        let delay = backoff.fail(&mut rand::rng());
        tracing::error!(
            "Convex Client Worker failed: {e:?}. Backing off for {delay:?} and retrying."
        );
        tokio::time::sleep(delay).await;

        // Tell the sync protocol to reconnect followed by an immediate resend of
        // ongoing queries/mutations. It's important these happen together to
        // ensure mutation ordering. If an auth token fetcher is stored,
        // resend_ongoing_queries_mutations will refresh the token first.
        protocol_manager
            .reconnect(ReconnectRequest {
                reason: e,
                max_observed_timestamp: base_client.max_observed_timestamp(),
            })
            .await;
        base_client.resend_ongoing_queries_mutations().await;
        // We'll flush messages from base_client inside the next call to
        // `_worker_once`.
    }
}

async fn _worker_once<T: SyncProtocol>(
    protocol_response_receiver: &mut mpsc::Receiver<ProtocolResponse>,
    client_request_receiver: &mut mpsc::UnboundedReceiver<ClientRequest>,
    watch_sender: &mut broadcast::Sender<QueryResults>,
    base_client: &mut BaseConvexClient,
    protocol_manager: &mut T,
    observer: Option<&ClientObserver>,
) -> Result<ControlFlow<()>, ReconnectProtocolReason> {
    // If there are any outgoing messages to flush (e.g. from an outer reconnect),
    // do so first.
    communicate(
        base_client,
        protocol_response_receiver,
        watch_sender,
        protocol_manager,
        observer,
    )
    .await?;

    tokio::select! {
        protocol_response = protocol_response_receiver.recv() => {
            let Some(protocol_response) = protocol_response else {
                // A terminated transport cannot reconnect. End pending requests and
                // join it through the shared close owner, even with live client clones.
                return Ok(ControlFlow::Break(()));
            };
            handle_protocol_response(base_client, watch_sender, protocol_response, observer)?;
        }
        Some(client_request) = client_request_receiver.recv() => {
            match client_request {
                ClientRequest::Subscribe(query, tx, request_sender) => {
                    let watch = watch_sender.subscribe();
                    let SubscribeRequest {
                        udf_path,
                        args,
                    } =  query;
                    let subscriber_id = base_client.subscribe(udf_path, args);
                    communicate(
                        base_client,
                        protocol_response_receiver,
                        watch_sender,
                        protocol_manager,
                        observer,
                    )
                    .await?;

                    let watch = BroadcastStream::new(watch);
                    let subscription = QuerySubscription {
                        subscriber_id,
                        request_sender,
                        watch,
                        initial: base_client.latest_results().get(&subscriber_id).cloned(),
                    };
                    let _ = tx.send(subscription);
                },
                ClientRequest::Mutation(mutation, tx) => {
                    let MutationRequest {
                        udf_path,
                        args,
                    } = mutation;
                    let result_receiver = base_client
                        .mutation(udf_path, args);
                        communicate(
                            base_client,
                            protocol_response_receiver,
                            watch_sender,
                            protocol_manager,
                            observer,
                        )
                        .await?;
                    let _ = tx.send(result_receiver);
                },
                ClientRequest::Action(action, tx) => {
                    let ActionRequest {
                        udf_path,
                        args,
                    } = action;
                    let result_receiver = base_client
                        .action(udf_path, args);
                        communicate(
                            base_client,
                            protocol_response_receiver,
                            watch_sender,
                            protocol_manager,
                            observer,
                        )
                        .await?;
                    let _ = tx.send(result_receiver);
                },
                ClientRequest::Unsubscribe(unsubscribe) => {
                    let UnsubscribeRequest {subscriber_id} = unsubscribe;
                    base_client.unsubscribe(subscriber_id);
                    communicate(
                        base_client,
                        protocol_response_receiver,
                        watch_sender,
                        protocol_manager,
                        observer,
                    )
                    .await?;
                },
                ClientRequest::Authenticate(fetcher) => {
                    base_client.set_auth_fetcher(fetcher).await;
                    communicate(
                        base_client,
                        protocol_response_receiver,
                        watch_sender,
                        protocol_manager,
                        observer,
                    )
                    .await?;
                },
            }
        },
    }
    Ok(ControlFlow::Continue(()))
}

/// Flush all messages to the protocol while processing server mesages.
async fn communicate<P: SyncProtocol>(
    base_client: &mut BaseConvexClient,
    protocol_response_receiver: &mut mpsc::Receiver<ProtocolResponse>,
    watch_sender: &mut broadcast::Sender<QueryResults>,
    protocol: &mut P,
    observer: Option<&ClientObserver>,
) -> Result<(), ReconnectProtocolReason> {
    while let Some(modification) = base_client.pop_next_message() {
        let mut send_future = protocol.send(modification);
        loop {
            tokio::select! {
               _ = &mut send_future => break,
               // Keep processing protocol responses while waiting so that we
               // don't deadlock with the websocket worker.
               Some(protocol_response) = protocol_response_receiver.recv() => {
                   handle_protocol_response(
                       base_client, watch_sender, protocol_response, observer,
                   )?;
               }
            }
        }
    }
    Ok(())
}

fn handle_protocol_response(
    base_client: &mut BaseConvexClient,
    watch_sender: &mut broadcast::Sender<QueryResults>,
    protocol_response: ProtocolResponse,
    observer: Option<&ClientObserver>,
) -> Result<(), ReconnectProtocolReason> {
    match protocol_response {
        ProtocolResponse::ServerMessage(msg) => {
            if let Some(subscriber_id_to_latest_value) = base_client.receive_message(msg)? {
                if let Some(observer) = observer {
                    observer(ClientEvent::QueryResults(
                        subscriber_id_to_latest_value.clone(),
                    ));
                }
                // Notify watchers of the new consistent query results at new timestamp
                let _ = watch_sender.send(subscriber_id_to_latest_value);
            }
        },
        ProtocolResponse::Failure => {
            return Err("ProtocolFailure".into());
        },
    }
    Ok(())
}
