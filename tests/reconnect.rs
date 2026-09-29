use std::{
    collections::BTreeMap,
    time::Duration,
};

use anyhow::Context;
use convex::{
    base_client::BaseConvexClient,
    ClientEvent,
    ConvexClient,
    ConvexClientBuilder,
    FunctionResult,
    Value,
};
use convex_sync_types::{
    types::ErrorPayload,
    ClientMessage,
    LogLinesMessage,
    ServerMessage,
    SessionId,
    StateVersion,
    Timestamp,
};
use futures::{
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
use uuid::Uuid;

async fn receive_message(socket: &mut WebSocketStream<TcpStream>) -> anyhow::Result<ClientMessage> {
    let message = socket.next().await.context("Client disconnected")??;
    let Message::Text(text) = message else {
        anyhow::bail!("Expected a text message, got {message:?}");
    };
    serde_json::from_str::<serde_json::Value>(&text)?.try_into()
}

async fn accept_connection(
    listener: &TcpListener,
) -> anyhow::Result<(
    WebSocketStream<TcpStream>,
    SessionId,
    u32,
    Option<Timestamp>,
)> {
    let (stream, _) = listener.accept().await?;
    let mut socket = accept_async(stream).await?;
    let ClientMessage::Connect {
        session_id,
        connection_count,
        max_observed_timestamp,
        ..
    } = receive_message(&mut socket).await?
    else {
        anyhow::bail!("Expected Connect as the first message");
    };
    Ok((socket, session_id, connection_count, max_observed_timestamp))
}

#[tokio::test]
async fn mutation_is_deduplicated_after_losing_responses() -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = ConvexClient::new(&format!("http://{}", listener.local_addr()?)).await?;

        let server = async {
            let mut committed = BTreeMap::new();
            let mut counter = 0_i64;
            let mut sessions = Vec::new();
            let mut mutations = Vec::new();
            for attempt in 0..4 {
                let (mut socket, session_id, connection_count, observed_ts) =
                    accept_connection(&listener).await?;
                assert_eq!(connection_count, attempt);
                assert_eq!(
                    observed_ts,
                    if attempt == 3 {
                        Some(Timestamp::try_from(1_i64)?)
                    } else {
                        None
                    }
                );
                sessions.push(session_id);

                let mut version = StateVersion::initial();
                let mut restored_query_sets = 0;
                let request_id = loop {
                    let message = receive_message(&mut socket).await?;
                    match message {
                        ClientMessage::ModifyQuerySet {
                            base_version,
                            new_version,
                            modifications,
                        } => {
                            assert!(attempt > 0);
                            assert_eq!(base_version, StateVersion::initial().query_set);
                            assert_eq!(*new_version, *base_version + 1);
                            assert!(modifications.is_empty());
                            restored_query_sets += 1;
                            version.query_set = new_version;
                        },
                        ClientMessage::Mutation { request_id, .. } => {
                            mutations.push(message);
                            break request_id;
                        },
                        _ => anyhow::bail!("Unexpected client message: {message:?}"),
                    }
                };
                assert_eq!(restored_query_sets, usize::from(attempt > 0));

                // Model the backend's committed-success lookup by (session_id, request_id).
                let result = *committed
                    .entry((Uuid::from(session_id), request_id))
                    .or_insert_with(|| {
                        counter += 1;
                        counter
                    });
                if attempt < 2 {
                    // The mutation committed, but the connection drops before its response.
                    drop(socket);
                    continue;
                }

                let response = ServerMessage::MutationResponse {
                    request_id,
                    result: Ok(Value::Int64(result)),
                    ts: Some(Timestamp::try_from(result)?),
                    log_lines: LogLinesMessage(vec![]),
                };
                if attempt == 2 {
                    socket
                        .send(Message::Text(
                            serde_json::Value::from(response).to_string().into(),
                        ))
                        .await?;
                    // Close after the response but before any transition. The next
                    // Connect's timestamp proves the client processed the success.
                    socket.close(None).await?;
                    drop(socket);
                    continue;
                }

                // The replay is in flight when this transition completes the retained
                // mutation. Its later response must leave this connection usable.
                version.ts = Timestamp::try_from(result + 1)?;
                let transition = ServerMessage::<Value>::Transition {
                    start_version: StateVersion::initial(),
                    end_version: version,
                    modifications: vec![],
                    client_clock_skew: None,
                    server_ts: None,
                };
                socket
                    .send(Message::Text(
                        serde_json::Value::from(transition).to_string().into(),
                    ))
                    .await?;
                socket
                    .send(Message::Text(
                        serde_json::Value::from(response).to_string().into(),
                    ))
                    .await?;

                let following = receive_message(&mut socket).await?;
                let mut expected = mutations[0].clone();
                let ClientMessage::Mutation {
                    request_id: next, ..
                } = &mut expected
                else {
                    anyhow::bail!("Expected original mutation");
                };
                *next = request_id + 1;
                assert_eq!(
                    following, expected,
                    "Following mutation must use the same socket"
                );
                mutations.push(following);
                counter += 1;
                assert!(committed
                    .insert((Uuid::from(session_id), request_id + 1), counter)
                    .is_none());
                let end_version = StateVersion {
                    ts: version.ts.succ()?,
                    ..version
                };
                for response in [
                    ServerMessage::MutationResponse {
                        request_id: request_id + 1,
                        result: Ok(Value::Int64(counter)),
                        ts: Some(end_version.ts),
                        log_lines: LogLinesMessage(vec![]),
                    },
                    ServerMessage::Transition {
                        start_version: version,
                        end_version,
                        modifications: vec![],
                        client_clock_skew: None,
                        server_ts: None,
                    },
                ] {
                    socket
                        .send(Message::Text(
                            serde_json::Value::from(response).to_string().into(),
                        ))
                        .await?;
                }
                // Keep the peer alive through explicit close; no sixth submission is valid.
                while let Some(frame) = socket.next().await {
                    match frame {
                        Ok(Message::Text(_)) => anyhow::bail!("Unexpected sixth submission"),
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {},
                    }
                }
            }
            Ok::<_, anyhow::Error>((counter, sessions, mutations))
        };

        let caller = async {
            for result in [1, 2] {
                assert_eq!(
                    client.mutation("incrementCounter", BTreeMap::new()).await?,
                    FunctionResult::Value(Value::Int64(result))
                );
            }
            client.close().await
        };
        let ((), (counter, sessions, mutations)) = futures::try_join!(caller, server)?;
        assert_eq!(counter, 2, "Each logical mutation must commit only once");
        assert_eq!(sessions.len(), 4);
        assert!(sessions.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(
            mutations.len(),
            5,
            "Four original submissions and one following mutation"
        );
        assert!(mutations[..4].windows(2).all(|pair| pair[0] == pair[1]));
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn independent_clients_have_distinct_sessions() -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let _first_client = ConvexClient::new(&url).await?;
        let (_first_socket, first_session, first_count, _) = accept_connection(&listener).await?;
        let _second_client = ConvexClient::new(&url).await?;
        let (_second_socket, second_session, second_count, _) =
            accept_connection(&listener).await?;

        assert_ne!(first_session, second_session);
        assert_eq!(first_count, 0);
        assert_eq!(second_count, 0);
        Ok(())
    })
    .await?
}

#[test]
fn mutation_responses_still_validate_request_identity_and_type() -> anyhow::Result<()> {
    let mut client = BaseConvexClient::new();
    let committed = Timestamp::try_from(1_i64)?;
    let response = |request_id| ServerMessage::MutationResponse {
        request_id,
        result: Ok(Value::Int64(1)),
        ts: Some(committed),
        log_lines: LogLinesMessage(vec![]),
    };
    assert!(client.receive_message(response(0)).is_err());

    let mut action = client.action("test:action".parse()?, BTreeMap::new());
    let Some(ClientMessage::Action { request_id, .. }) = client.pop_next_message() else {
        anyhow::bail!("Expected action");
    };
    assert!(client.receive_message(response(request_id)).is_err());
    assert!(matches!(
        action.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let response = ServerMessage::ActionResponse {
        request_id,
        result: Ok(Value::Int64(2)),
        log_lines: LogLinesMessage(vec![]),
    };
    assert!(client.receive_message(response.clone()).is_ok());
    assert_eq!(action.try_recv()?, FunctionResult::Value(Value::Int64(2)));
    assert!(client.receive_message(response).is_err());
    assert!(client
        .receive_message(ServerMessage::MutationResponse {
            request_id: request_id + 1,
            result: Ok(Value::Int64(3)),
            ts: Some(committed),
            log_lines: LogLinesMessage(vec![]),
        })
        .is_err());
    Ok(())
}

#[tokio::test]
async fn mutation_completion_respects_commit_timestamp_and_structured_errors() -> anyhow::Result<()>
{
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (observed, mut transitions) = mpsc::unbounded_channel();
        let mut client = ConvexClientBuilder::new(&format!("http://{}", listener.local_addr()?))
            .with_observer(move |event| {
                if let ClientEvent::QueryResults(results) = event {
                    let _ = observed.send(results);
                }
            })
            .build()
            .await?;
        let (mut socket, ..) = accept_connection(&listener).await?;
        let mut mutation_client = client.clone();
        let mut mutation = Box::pin(mutation_client.mutation("test:write", BTreeMap::new()));
        assert!(matches!(
            futures::poll!(&mut mutation),
            std::task::Poll::Pending
        ));
        let ClientMessage::Mutation { request_id, .. } = receive_message(&mut socket).await? else {
            anyhow::bail!("Expected mutation");
        };
        let committed = Timestamp::try_from(10_i64)?;
        let response = ServerMessage::MutationResponse {
            request_id,
            result: Ok(Value::Int64(1)),
            ts: Some(committed),
            log_lines: LogLinesMessage(vec![]),
        };
        socket
            .send(Message::Text(
                serde_json::Value::from(response).to_string().into(),
            ))
            .await?;
        let mut version = StateVersion::initial();
        for ts in [Timestamp::try_from(5_i64)?, committed] {
            let end_version = StateVersion { ts, ..version };
            let transition = ServerMessage::<Value>::Transition {
                start_version: version,
                end_version,
                modifications: vec![],
                client_clock_skew: None,
                server_ts: None,
            };
            socket
                .send(Message::Text(
                    serde_json::Value::from(transition).to_string().into(),
                ))
                .await?;
            transitions.recv().await.context("Missing transition")?;
            if ts < committed {
                assert!(
                    matches!(futures::poll!(&mut mutation), std::task::Poll::Pending),
                    "An older query snapshot cannot complete the write"
                );
            }
            version = end_version;
        }
        assert_eq!(mutation.await?, FunctionResult::Value(Value::Int64(1)));

        let error = ErrorPayload::ErrorData {
            message: "rejected".to_owned(),
            data: Value::Int64(2),
        };
        let server = async {
            let ClientMessage::Mutation {
                request_id: next, ..
            } = receive_message(&mut socket).await?
            else {
                anyhow::bail!("Expected following mutation");
            };
            assert_ne!(request_id, next);
            let response = ServerMessage::MutationResponse {
                request_id: next,
                result: Err(error.clone()),
                ts: None,
                log_lines: LogLinesMessage(vec![]),
            };
            socket
                .send(Message::Text(
                    serde_json::Value::from(response).to_string().into(),
                ))
                .await?;
            Ok::<_, anyhow::Error>(())
        };
        let (result, ()) =
            futures::try_join!(client.mutation("test:reject", BTreeMap::new()), server)?;
        assert_eq!(result, FunctionResult::from(Err(error)));
        client.close().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn terminal_occ_does_not_replay_and_following_mutation_survives_disconnect(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = ConvexClient::new(&format!("http://{}", listener.local_addr()?)).await?;
        let message = "OptimisticConcurrencyControlFailure: Data read or written in this mutation \
                       changed while it was being run.";
        let server = async {
            let (mut socket, session, ..) = accept_connection(&listener).await?;
            let first = receive_message(&mut socket).await?;
            let ClientMessage::Mutation {
                request_id: failed, ..
            } = first
            else {
                anyhow::bail!("Expected first mutation");
            };
            let response = ServerMessage::<Value>::MutationResponse {
                request_id: failed,
                result: Err(ErrorPayload::Message(message.to_owned())),
                ts: None,
                log_lines: LogLinesMessage(vec![]),
            };
            socket
                .send(Message::Text(
                    serde_json::Value::from(response).to_string().into(),
                ))
                .await?;
            let second = receive_message(&mut socket).await?;
            let ClientMessage::Mutation {
                request_id: pending,
                ..
            } = second
            else {
                anyhow::bail!("Expected following mutation on the same socket");
            };
            assert_ne!(failed, pending);
            drop(socket);
            let (mut socket, resumed, count, _) = accept_connection(&listener).await?;
            assert_eq!(session, resumed);
            assert_eq!(count, 1);
            let ClientMessage::ModifyQuerySet { new_version, .. } =
                receive_message(&mut socket).await?
            else {
                anyhow::bail!("Expected restored query set");
            };
            let replay = receive_message(&mut socket).await?;
            assert_eq!(replay, second, "Only the pending mutation may be replayed");
            let version = StateVersion {
                query_set: new_version,
                ts: Timestamp::try_from(1_i64)?,
                ..StateVersion::initial()
            };
            for response in [
                ServerMessage::MutationResponse {
                    request_id: pending,
                    result: Ok(Value::Int64(7)),
                    ts: Some(version.ts),
                    log_lines: LogLinesMessage(vec![]),
                },
                ServerMessage::Transition {
                    start_version: StateVersion::initial(),
                    end_version: version,
                    modifications: vec![],
                    client_clock_skew: None,
                    server_ts: None,
                },
            ] {
                socket
                    .send(Message::Text(
                        serde_json::Value::from(response).to_string().into(),
                    ))
                    .await?;
            }
            // Keep the peer alive until explicit close and reject any fourth submission.
            while let Some(frame) = socket.next().await {
                match frame {
                    Ok(Message::Text(_)) => anyhow::bail!("Unexpected fourth submission"),
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {},
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let caller = async {
            assert_eq!(
                client.mutation("test:conflict", BTreeMap::new()).await?,
                FunctionResult::ErrorMessage(message.to_owned())
            );
            assert_eq!(
                client.mutation("test:following", BTreeMap::new()).await?,
                FunctionResult::Value(Value::Int64(7))
            );
            client.close().await
        };
        futures::try_join!(caller, server)?;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
