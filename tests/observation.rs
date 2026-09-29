use std::{
    collections::BTreeMap,
    sync::{
        atomic::{
            AtomicBool,
            Ordering,
        },
        Arc,
        Barrier,
    },
    task::Poll,
    time::Duration,
};

use anyhow::Context;
use convex::{
    ClientEvent,
    ConvexClientBuilder,
    FunctionResult,
    Value,
    WebSocketState,
};
use convex_sync_types::{
    ClientMessage,
    LogLinesMessage,
    QuerySetModification,
    ServerMessage,
    StateModification,
    StateVersion,
};
use futures::{
    poll,
    SinkExt,
    StreamExt,
};
use tokio::{
    net::{
        TcpListener,
        TcpStream,
    },
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::Message,
    WebSocketStream,
};

async fn receive(socket: &mut WebSocketStream<TcpStream>) -> anyhow::Result<ClientMessage> {
    let Message::Text(text) = socket.next().await.context("Client disconnected")?? else {
        anyhow::bail!("Expected text message");
    };
    serde_json::from_str::<serde_json::Value>(&text)?.try_into()
}

#[tokio::test]
async fn last_client_drop_before_first_worker_poll_notifies_closed() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let (observed, mut events) = mpsc::unbounded_channel();
    let client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
        .with_observer(move |event| {
            let _ = observed.send(event);
        })
        .build()
        .await?;
    // This current-thread task has not yielded since spawning either SDK task.
    drop(client);
    timeout(Duration::from_secs(5), async {
        while let Some(event) = events.recv().await {
            if matches!(event, ClientEvent::Closed) {
                return Ok(());
            }
        }
        anyhow::bail!("Missing close event before first worker poll");
    })
    .await?
}

#[tokio::test]
async fn cancelled_close_waiter_preserves_shared_shutdown_and_ends_pending_query(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .build()
            .await?;
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let mut querying_client = client.clone();
        let query =
            tokio::spawn(async move { querying_client.query("test:value", BTreeMap::new()).await });
        while !matches!(
            receive(&mut socket).await?,
            ClientMessage::ModifyQuerySet { .. }
        ) {}
        let mut clone = client.clone();
        // A later subscription reply proves the query's subscription was handed
        // off, so shutdown exercises its empty stream rather than a send error.
        let mut subscription = clone.subscribe("test:value", BTreeMap::new()).await?;
        let mut first_close = Box::pin(client.close());
        // Poll shutdown once without yielding to the worker, then cancel only
        // this waiter. Another clone must still join the same shutdown.
        assert!(matches!(poll!(&mut first_close), Poll::Pending));
        drop(first_close);
        clone.close().await?;
        client.close().await?;
        assert!(query.await?.is_err());
        assert!(subscription.next().await.is_none());
        assert!(clone
            .subscribe("test:value", BTreeMap::new())
            .await
            .is_err());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn close_after_query_worker_panic_joins_transport_and_retains_error() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (observed, mut events) = mpsc::unbounded_channel();
        let mut client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .with_observer(move |event| {
                let _ = observed.send(event);
            })
            .build()
            .await?;
        let (stream, _) = listener.accept().await?;
        let _socket = accept_async(stream).await?;
        while !matches!(
            events.recv().await.context("Observer closed")?,
            ClientEvent::ConnectionState(WebSocketState::Connected)
        ) {}
        client
            .set_auth_callback(Some(Box::new(|_| {
                Box::pin(async { panic!("auth fixture failed") })
            })))
            .await;
        // Transport teardown must precede query-task completion even on panic.
        // The callback itself never panics or blocks.
        assert!(matches!(
            events.recv().await,
            Some(ClientEvent::ConnectionState(WebSocketState::Connecting))
        ));
        assert!(matches!(events.recv().await, Some(ClientEvent::Closed)));
        let first = client.close().await.unwrap_err();
        let second = client.clone().close().await.unwrap_err();
        let first = first
            .downcast_ref::<Arc<tokio::task::JoinError>>()
            .context("Missing worker panic")?;
        let second = second
            .downcast_ref::<Arc<tokio::task::JoinError>>()
            .context("Missing shared worker panic")?;
        assert!(first.is_panic());
        assert!(Arc::ptr_eq(first, second));
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn transport_panic_ends_pending_query_before_explicit_close() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (observed, mut events) = mpsc::unbounded_channel();
        let connected = AtomicBool::new(false);
        let client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .with_observer(move |event| {
                match event {
                    ClientEvent::ConnectionState(WebSocketState::Connected) => {
                        connected.store(true, Ordering::SeqCst);
                    },
                    ClientEvent::ConnectionState(WebSocketState::Connecting)
                        if connected.swap(false, Ordering::SeqCst) =>
                    {
                        // Inject one transport-task panic before it can send Failure.
                        // Observer panic recovery is not a supported callback contract.
                        panic!("transport fixture failed");
                    },
                    _ => {},
                }
                let _ = observed.send(event);
            })
            .build()
            .await?;
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let mut querying = client.clone();
        let query =
            tokio::spawn(async move { querying.query("test:value", BTreeMap::new()).await });
        while !matches!(
            receive(&mut socket).await?,
            ClientMessage::ModifyQuerySet { .. }
        ) {}
        socket.close(None).await?;
        while !matches!(
            events.recv().await.context("Observer closed")?,
            ClientEvent::Closed
        ) {}
        assert!(query.await?.is_err());
        let error = client.close().await.unwrap_err();
        assert!(error
            .downcast_ref::<Arc<tokio::task::JoinError>>()
            .context("Missing transport panic")?
            .is_panic());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn observer_preserves_transitions_and_detects_application_overflow() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (events, mut observations) = mpsc::unbounded_channel();
        let (bounded, mut queued) = mpsc::channel(1);
        let revoked = Arc::new(AtomicBool::new(false));
        let callback_revoked = revoked.clone();
        let mut client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .with_observer(move |event| {
                if let ClientEvent::QueryResults(results) = event {
                    if bounded.try_send(results.clone()).is_err() {
                        callback_revoked.store(true, Ordering::SeqCst);
                    }
                    events.send(results).unwrap();
                }
            })
            .build()
            .await?;
        let mut latest = client.watch_all();
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        assert!(matches!(
            receive(&mut socket).await?,
            ClientMessage::Connect { .. }
        ));
        let subscription = client.subscribe("test:value", BTreeMap::new()).await?;
        let (query_id, query_set) = loop {
            if let ClientMessage::ModifyQuerySet {
                new_version,
                modifications,
                ..
            } = receive(&mut socket).await?
            {
                if let Some(QuerySetModification::Add(query)) = modifications.into_iter().next() {
                    break (query.query_id, new_version);
                }
            }
        };
        let mut version = StateVersion::initial();
        for value in [1_i64, 2, 1] {
            let next = StateVersion {
                query_set,
                ts: version.ts.succ()?,
                ..version
            };
            let message = ServerMessage::Transition {
                start_version: version,
                end_version: next,
                modifications: vec![StateModification::QueryUpdated {
                    query_id,
                    value: Value::Int64(value),
                    journal: None,
                    log_lines: LogLinesMessage(vec![]),
                }],
                client_clock_skew: None,
                server_ts: None,
            };
            socket
                .send(Message::Text(
                    serde_json::Value::from(message).to_string().into(),
                ))
                .await?;
            let received = observations.recv().await.context("Observer closed")?;
            assert_eq!(
                received.get(subscription.id()),
                Some(&FunctionResult::Value(Value::Int64(value)))
            );
            version = next;
        }
        assert!(revoked.load(Ordering::SeqCst));
        assert_eq!(
            queued.recv().await.unwrap().get(subscription.id()),
            Some(&FunctionResult::Value(Value::Int64(1)))
        );
        assert_eq!(
            latest.next().await.unwrap().get(subscription.id()),
            Some(&FunctionResult::Value(Value::Int64(1)))
        );
        client.close().await?;
        assert!(latest.next().await.is_none());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn observer_detects_disconnect_when_legacy_state_channel_is_full() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (legacy, mut legacy_receiver) = mpsc::channel(1);
        let (observed, mut events) = mpsc::unbounded_channel();
        let revoked = Arc::new(AtomicBool::new(false));
        let callback_revoked = revoked.clone();
        let connected = AtomicBool::new(false);
        let client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .with_on_state_change(legacy)
            .with_observer(move |event| {
                match &event {
                    ClientEvent::ConnectionState(WebSocketState::Connected) => {
                        connected.store(true, Ordering::SeqCst);
                    },
                    ClientEvent::ConnectionState(WebSocketState::Connecting)
                        if connected.load(Ordering::SeqCst) =>
                    {
                        callback_revoked.store(true, Ordering::SeqCst);
                    },
                    ClientEvent::Closed => {
                        callback_revoked.store(true, Ordering::SeqCst);
                    },
                    ClientEvent::ConnectionState(WebSocketState::Connecting)
                    | ClientEvent::QueryResults(_) => {},
                }
                observed.send(event).unwrap();
            })
            .build()
            .await?;
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        while !matches!(
            events.recv().await.context("Observer closed")?,
            ClientEvent::ConnectionState(WebSocketState::Connected)
        ) {}
        socket.close(None).await?;
        assert!(matches!(
            events.recv().await,
            Some(ClientEvent::ConnectionState(WebSocketState::Connecting))
        ));
        assert_eq!(
            legacy_receiver.recv().await,
            Some(WebSocketState::Connecting)
        );
        // The initial legacy Connecting occupied its only slot. Both Connected
        // and the disconnect notification were dropped before it was drained.
        assert!(revoked.load(Ordering::SeqCst));
        let (stream, _) = listener.accept().await?;
        let _replacement_socket = accept_async(stream).await?;
        while !matches!(
            events.recv().await.context("Observer closed")?,
            ClientEvent::ConnectionState(WebSocketState::Connected)
        ) {}
        assert!(revoked.load(Ordering::SeqCst));
        let clone = client.clone();
        let (first, second) = tokio::join!(client.close(), clone.close());
        first?;
        second?;
        while let Some(event) = events.recv().await {
            if matches!(event, ClientEvent::Closed) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        anyhow::bail!("Missing close event");
    })
    .await?
}

#[tokio::test]
async fn close_cancels_pending_auth_and_waits_for_socket_drop() -> anyhow::Result<()> {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .build()
            .await?;
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        let (entered, mut started) = mpsc::channel(1);
        client
            .set_auth_callback(Some(Box::new(move |_| {
                entered.try_send(()).unwrap();
                Box::pin(std::future::pending())
            })))
            .await;
        started.recv().await.context("Auth did not start")?;
        client.close().await?;
        while let Some(message) = socket.next().await {
            if message.is_err() || matches!(message?, Message::Close(_)) {
                return Ok::<_, anyhow::Error>(());
            }
        }
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_client_drop_ends_transport_waiting_for_reconnect_ack() -> anyhow::Result<()> {
    // A non-yielding regression can also block runtime teardown after this
    // timeout fires, so the test runner needs an external process deadline.
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (observed, mut events) = mpsc::unbounded_channel();
        let release_transport = Arc::new(Barrier::new(2));
        let transport_gate = release_transport.clone();
        let connected = AtomicBool::new(false);
        let mut client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .with_observer(move |event| {
                let hold_transport = match event {
                    ClientEvent::ConnectionState(WebSocketState::Connected) => {
                        connected.store(true, Ordering::SeqCst);
                        false
                    },
                    ClientEvent::ConnectionState(WebSocketState::Connecting) => {
                        connected.swap(false, Ordering::SeqCst)
                    },
                    _ => false,
                };
                let _ = observed.send(event);
                if hold_transport {
                    // Fixture-only scheduler gate: keep this transport poll active
                    // while its owner drops. Release it before requiring termination.
                    transport_gate.wait();
                }
            })
            .build()
            .await?;
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        while !matches!(
            events.recv().await.context("Observer closed")?,
            ClientEvent::ConnectionState(WebSocketState::Connected)
        ) {}
        let mut subscription = client.subscribe("test:value", BTreeMap::new()).await?;

        // Failure has not reached the query worker, so it cannot acknowledge
        // reconnect. The subscription keeps its sender without keeping the owner.
        socket.close(None).await?;
        assert!(matches!(
            events.recv().await,
            Some(ClientEvent::ConnectionState(WebSocketState::Connecting))
        ));
        drop(client);
        assert!(matches!(events.recv().await, Some(ClientEvent::Closed)));
        // One of the two runtime threads is held in the transport callback.
        // A spawned task on the other thread runs only after the query worker's
        // cancellation destructor has returned and dropped the transport owner.
        tokio::spawn(async {}).await?;
        release_transport.wait();
        while let Some(event) = events.recv().await {
            assert!(matches!(
                event,
                ClientEvent::ConnectionState(WebSocketState::Connecting)
            ));
        }
        // Closed alone is insufficient: channel closure also requires the
        // transport task to release its observer, despite the retained token.
        assert!(subscription.next().await.is_none());
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
