use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum BencodeVal {
  Int(i64),
  String(String),
  List(Vec<BencodeVal>),
  Dict(BencodeDict),
  Bytes(Vec<u8>),
}

pub type BencodeDict = HashMap<String, BencodeVal>;
