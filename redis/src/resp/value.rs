#[derive(Clone, Debug, PartialEq)]
pub enum RespValue {
    SimpleString(Vec<u8>),
    SimpleError(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<RespValue>),
    NullArray,
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(Vec<u8>),
    BulkError(Vec<u8>),
    VerbatimString { format: [u8; 3], data: Vec<u8> },
    Map(Vec<(RespValue, RespValue)>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Decoded {
    pub value: RespValue,
    pub consumed: usize,
}
