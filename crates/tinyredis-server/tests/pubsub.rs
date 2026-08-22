use std::time::Duration;

use tinyredis_core::Database;
use tinyredis_protocol::{encode_frame, RespDecoder, RespFrame};
use tinyredis_server::{AppState, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

fn command_frame(cmd: &str, args: &[&str]) -> RespFrame {
    let mut items = vec![RespFrame::Bulk(Some(cmd.as_bytes().to_vec()))];
    for arg in args {
        items.push(RespFrame::Bulk(Some(arg.as_bytes().to_vec())));
    }
    RespFrame::Array(items)
}

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<RespFrame> {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let decoder = RespDecoder::new();
    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            anyhow::bail!("connection closed");
        }
        if let Some(frame) = decoder.decode(&mut buf)? {
            return Ok(frame);
        }
    }
}

async fn send_command(stream: &mut TcpStream, cmd: &str, args: &[&str]) -> anyhow::Result<()> {
    stream
        .write_all(&encode_frame(&command_frame(cmd, args)))
        .await?;
    Ok(())
}

#[tokio::test]
async fn pubsub_delivers_messages_without_client_input() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut config = ServerConfig::default();
    config.host = "127.0.0.1".into();
    config.port = addr.port();
    let state = std::sync::Arc::new(AppState::new(config, Database::new())?);

    let accept_state = state.clone();
    let accept_task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await?;
            let state = accept_state.clone();
            tokio::spawn(async move {
                let _ = tinyredis_server::handler::handle_connection(stream, state).await;
            });
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });

    let mut subscriber = TcpStream::connect(addr).await?;
    send_command(&mut subscriber, "SUBSCRIBE", &["notifications"]).await?;
    let subscribe_frame = read_frame(&mut subscriber).await?;
    assert!(matches!(subscribe_frame, RespFrame::Array(_)));

    let mut publisher = TcpStream::connect(addr).await?;
    send_command(&mut publisher, "PUBLISH", &["notifications", "Hello"]).await?;
    let publish_frame = timeout(Duration::from_secs(2), read_frame(&mut publisher)).await??;
    assert!(matches!(publish_frame, RespFrame::Integer(1)));

    let message_frame = timeout(Duration::from_secs(2), read_frame(&mut subscriber)).await??;
    match message_frame {
        RespFrame::Array(items) => {
            assert_eq!(items.len(), 3);
            assert!(matches!(&items[0], RespFrame::Bulk(Some(b)) if b == b"message"));
            assert!(matches!(&items[1], RespFrame::Bulk(Some(b)) if b == b"notifications"));
            assert!(matches!(&items[2], RespFrame::Bulk(Some(b)) if b == b"Hello"));
        }
        other => anyhow::bail!("expected message frame, got {other:?}"),
    }

    send_command(&mut publisher, "PUBLISH", &["notifications", "Rust"]).await?;
    let _ = timeout(Duration::from_secs(2), read_frame(&mut publisher)).await??;
    let second = timeout(Duration::from_secs(2), read_frame(&mut subscriber)).await??;
    match second {
        RespFrame::Array(items) => {
            assert!(matches!(&items[2], RespFrame::Bulk(Some(b)) if b == b"Rust"));
        }
        other => anyhow::bail!("expected second message frame, got {other:?}"),
    }

    send_command(&mut publisher, "PUBLISH", &["notifications", "Working"]).await?;
    let _ = timeout(Duration::from_secs(2), read_frame(&mut publisher)).await??;
    let third = timeout(Duration::from_secs(2), read_frame(&mut subscriber)).await??;
    match third {
        RespFrame::Array(items) => {
            assert!(matches!(&items[2], RespFrame::Bulk(Some(b)) if b == b"Working"));
        }
        other => anyhow::bail!("expected third message frame, got {other:?}"),
    }

    send_command(&mut subscriber, "UNSUBSCRIBE", &["notifications"]).await?;
    let _ = timeout(Duration::from_secs(2), read_frame(&mut subscriber)).await??;

    send_command(&mut subscriber, "PING", &[]).await?;
    let ping = timeout(Duration::from_secs(2), read_frame(&mut subscriber)).await??;
    assert!(matches!(ping, RespFrame::Simple(s) if s == "PONG"));

    send_command(&mut publisher, "SET", &["foo", "bar"]).await?;
    let set = timeout(Duration::from_secs(2), read_frame(&mut publisher)).await??;
    assert!(matches!(set, RespFrame::Simple(s) if s == "OK"));

    send_command(&mut publisher, "GET", &["foo"]).await?;
    let get = timeout(Duration::from_secs(2), read_frame(&mut publisher)).await??;
    assert!(matches!(get, RespFrame::Bulk(Some(b)) if b == b"bar"));

    subscriber.shutdown().await?;
    publisher.shutdown().await?;
    accept_task.abort();
    Ok(())
}
