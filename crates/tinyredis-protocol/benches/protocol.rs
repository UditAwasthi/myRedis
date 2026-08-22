use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tinyredis_core::Command;
use tinyredis_protocol::{encode_frame, parse_command, RespDecoder, RespFrame};

fn sample_command_frame() -> RespFrame {
    RespFrame::Array(vec![
        RespFrame::Bulk(Some(b"SET".to_vec())),
        RespFrame::Bulk(Some(b"benchmark".to_vec())),
        RespFrame::Bulk(Some(b"value".to_vec())),
    ])
}

fn bench_decode(c: &mut Criterion) {
    let frame = sample_command_frame();
    let encoded = encode_frame(&frame);
    let decoder = RespDecoder::with_max_bulk_len(64 * 1024);

    c.bench_function("resp_decode", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(encoded.as_slice());
            let decoded = decoder.decode(&mut buf).unwrap().unwrap();
            black_box(decoded);
        });
    });
}

fn bench_encode(c: &mut Criterion) {
    let frame = sample_command_frame();
    c.bench_function("resp_encode", |b| {
        b.iter(|| black_box(encode_frame(&frame)));
    });
}

fn bench_parse_command(c: &mut Criterion) {
    let frame = sample_command_frame();
    c.bench_function("parse_command", |b| {
        b.iter(|| {
            let cmd = parse_command(&frame).unwrap();
            black_box(cmd);
        });
    });
}

fn bench_decode_parse_pipeline(c: &mut Criterion) {
    let frame = sample_command_frame();
    let encoded = encode_frame(&frame);
    let decoder = RespDecoder::with_max_bulk_len(64 * 1024);

    c.bench_function("decode_then_parse", |b| {
        b.iter(|| {
            let mut buf = BytesMut::from(encoded.as_slice());
            let decoded = decoder.decode(&mut buf).unwrap().unwrap();
            let cmd: Command = parse_command(&decoded).unwrap();
            black_box(cmd);
        });
    });
}

criterion_group!(
    benches,
    bench_decode,
    bench_encode,
    bench_parse_command,
    bench_decode_parse_pipeline
);
criterion_main!(benches);
