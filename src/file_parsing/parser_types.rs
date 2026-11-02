use std::collections::HashMap;

use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum BencodeVal {
  Int(i64),
  String(String),
  List(Vec<BencodeVal>),
  Dict(BencodeDict),
  Bytes(Bytes),
}

pub type BencodeDict = HashMap<String, BencodeVal>;
