#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespFrame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<RespFrame>),
}

impl RespFrame {
    pub fn as_bulk_strings(frames: &[RespFrame]) -> Option<Vec<Option<String>>> {
        let mut out = Vec::with_capacity(frames.len());
        for frame in frames {
            match frame {
                RespFrame::Bulk(Some(bytes)) => {
                    out.push(Some(String::from_utf8_lossy(bytes).into_owned()));
                }
                RespFrame::Bulk(None) => out.push(None),
                _ => return None,
            }
        }
        Some(out)
    }

    pub fn command_args(frame: &RespFrame) -> Option<Vec<String>> {
        let RespFrame::Array(items) = frame else {
            return None;
        };
        let mut args = Vec::with_capacity(items.len());
        for item in items {
            match item {
                RespFrame::Bulk(Some(bytes)) => {
                    args.push(String::from_utf8_lossy(bytes).into_owned());
                }
                RespFrame::Bulk(None) => return None,
                _ => return None,
            }
        }
        Some(args)
    }
}
