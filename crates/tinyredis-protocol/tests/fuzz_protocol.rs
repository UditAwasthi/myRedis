use bytes::BytesMut;
use proptest::prelude::*;
use tinyredis_protocol::{parse_command, RespDecoder, RespFrame};

proptest! {
    #[test]
    fn decoder_never_panics(input in prop::collection::vec(any::<u8>(), 0..8192)) {
        let mut buf = BytesMut::from(input.as_slice());
        let decoder = RespDecoder::with_max_bulk_len(64 * 1024);
        loop {
            match decoder.decode(&mut buf) {
                Ok(None) => break,
                Ok(Some(_)) => continue,
                Err(_) => break,
            }
        }
    }

    #[test]
    fn parse_command_never_panics_on_decoded_frames(
        cmd in prop::string::string_regex("[A-Za-z]{1,12}").unwrap(),
        arg in prop::string::string_regex("[a-z0-9]{0,24}").unwrap(),
    ) {
        let frame = RespFrame::Array(vec![
            RespFrame::Bulk(Some(cmd.into_bytes())),
            RespFrame::Bulk(Some(arg.into_bytes())),
        ]);
        let _ = parse_command(&frame);
    }

    #[test]
    fn encode_decode_roundtrip_simple(value in prop::string::string_regex("[A-Za-z0-9:_-]{0,32}").unwrap()) {
        use tinyredis_protocol::encode_frame;
        let frame = RespFrame::Bulk(Some(value.into_bytes()));
        let encoded = encode_frame(&frame);
        let mut buf = BytesMut::from(encoded.as_slice());
        let decoder = RespDecoder::new();
        let decoded = decoder.decode(&mut buf).unwrap().expect("one frame");
        prop_assert_eq!(decoded, frame);
    }
}
