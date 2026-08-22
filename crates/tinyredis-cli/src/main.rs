use anyhow::{Context, Result};
use clap::Parser;
use tinyredis_protocol::{encode_frame, RespDecoder, RespFrame};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(name = "tinyredis-cli", about = "Interactive CLI for TinyRedis")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 6379)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect to {addr}"))?;

    let prompt = format!("{}> ", addr);
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        print!("{prompt}");
        use std::io::Write;
        std::io::stdout().flush().ok();

        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") {
            break;
        }

        let frame = build_command_frame(input);
        stream.write_all(&encode_frame(&frame)).await?;

        let response = read_response(&mut stream).await?;
        println!("{response}");
    }

    Ok(())
}

fn build_command_frame(input: &str) -> RespFrame {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let items: Vec<RespFrame> = parts
        .iter()
        .map(|p| RespFrame::Bulk(Some(p.as_bytes().to_vec())))
        .collect();
    RespFrame::Array(items)
}

async fn read_response(stream: &mut TcpStream) -> Result<String> {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let decoder = RespDecoder::new();

    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            anyhow::bail!("connection closed");
        }
        if let Some(frame) = decoder.decode(&mut buf)? {
            return Ok(format_frame(&frame));
        }
    }
}

fn format_frame(frame: &RespFrame) -> String {
    match frame {
        RespFrame::Simple(s) => s.clone(),
        RespFrame::Error(s) => format!("(error) {s}"),
        RespFrame::Integer(n) => n.to_string(),
        RespFrame::Bulk(None) => "(nil)".into(),
        RespFrame::Bulk(Some(b)) => {
            let text = String::from_utf8_lossy(b);
            format!("\"{text}\"")
        }
        RespFrame::Array(items) => {
            if items.len() >= 3
                && matches!(items.first(), Some(RespFrame::Bulk(Some(b))) if b == b"message")
            {
                let channel = format_bulk(&items[1]);
                let message = format_bulk(&items[2]);
                return format!("message: {channel} -> {message}");
            }
            let parts: Vec<String> = items.iter().map(format_frame).collect();
            parts.join("\n")
        }
    }
}

fn format_bulk(frame: &RespFrame) -> String {
    match frame {
        RespFrame::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => "(nil)".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_frame_splits_whitespace() {
        let frame = build_command_frame("SET name Udit");
        assert!(matches!(frame, RespFrame::Array(items) if items.len() == 3));
    }
}
