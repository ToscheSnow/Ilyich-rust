use crate::file_parsing::parser_types::{BencodeDict, BencodeVal};

pub struct BencodeEncoder;

impl BencodeEncoder {
  pub fn new() -> Self {
    Self
  }

  // Generic encode
  pub fn encode(&self, value: &BencodeVal) -> Vec<u8> {
    match value {
      BencodeVal::Bytes(bytes) => self.encode_raw_bytes(bytes),
      BencodeVal::String(content) => self.encode_byte_string(content),
      BencodeVal::Int(num) => self.encode_num(*num),
      BencodeVal::List(list) => self.encode_list(list),
      BencodeVal::Dict(dict) => self.encode_dict(dict),
    }
  }

  // Encode numbers to bytes
  fn encode_num(&self, num: i64) -> Vec<u8> {
    format!("i{}e", num).into_bytes()
  }

  // Encode byte strings for decoded strings
  fn encode_byte_string(&self, content: &str) -> Vec<u8> {
    let bytes = content.as_bytes();
    let prefix = format!("{}:", bytes.len());

    let mut result = Vec::with_capacity(prefix.len() + bytes.len());

    result.extend_from_slice(prefix.as_bytes());
    result.extend_from_slice(bytes);

    result
  }

  // Encode raw bytes for info and other hashes
  fn encode_raw_bytes(&self, bytes: &[u8]) -> Vec<u8> {
    // Prefix => "length" + ":"
    let prefix = format!("{}:", bytes.len());

    // Result => content as bytes
    let mut res = Vec::with_capacity(prefix.len() + bytes.len());

    // Lay out the arrays
    res.extend_from_slice(prefix.as_bytes());
    res.extend_from_slice(bytes);

    res
  }

  // Encode list bytes to bencode
  fn encode_list(&self, list: &[BencodeVal]) -> Vec<u8> {
    let mut serial: Vec<Vec<u8>> = Vec::new();
    let mut length = 0;

    // Store elements of the list by encoding recursively
    for item in list {
      let encoded = self.encode(item);
      length += encoded.len();
      serial.push(encoded);
    }

    // Extra 2 bytes for l and e
    let mut res = Vec::with_capacity(length + 2);

    res.push(b'l');

    // Lay out encoded entries
    for encoded in serial {
      res.extend_from_slice(&encoded);
    }

    res.push(b'e');

    res
  }

  // Encode dict bytes to bencode
  fn encode_dict(&self, dict: &BencodeDict) -> Vec<u8> {
    let mut encoded_entries: Vec<Vec<u8>> = Vec::new();

    // Loop over sorted keys to ensure valid sorted byte strings
    let mut keys: Vec<&String> = dict.keys().collect();
    keys.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    let mut length = 0;

    // Encode dictionary entries recursively
    for key in keys {
      let encoded_key = self.encode_byte_string(key);
      let encoded_entry = self.encode(dict.get(key).unwrap());

      let mut entry = Vec::with_capacity(encoded_key.len() + encoded_entry.len());

      entry.extend_from_slice(&encoded_key);
      entry.extend_from_slice(&encoded_entry);

      length += entry.len();
      encoded_entries.push(entry);
    }

    // Extra two bytes for d and e
    let mut res = Vec::with_capacity(length + 2);

    res.push(b'd');

    // Lay out encoded entries
    for encoded_entry in encoded_entries {
      res.extend_from_slice(&encoded_entry);
    }

    res.push(b'e');

    res
  }
}
