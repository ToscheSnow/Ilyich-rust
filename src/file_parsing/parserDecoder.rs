use crate::file_parsing::parser_types::{BencodeDict, BencodeVal};

pub enum ASCII {
  ZERO = 0x30,
  NINE = 0x39,
  COLON = 0x3a,
  L = 0x6c,
  E = 0x65,
  D = 0x64,
  I = 0x69,
  HP = 0x2d,
}

pub struct BencodeDecoder {
  buf: Vec<u8>,
  pos: usize,
}

impl BencodeDecoder {
  pub fn new(buf: Vec<u8>) -> Self {
    Self { buf, pos: 0 }
  }

  // Check if any remaining bytes
  fn done(&self) -> bool {
    self.pos >= self.buf.len()
  }

  fn peek(&self) -> Result<u8, String> {
    if self.done() {
      return Err("unexpected EOF".into());
    }

    Ok(self.buf[self.pos])
  }

  fn next(&mut self) -> Result<u8, String> {
    let b = self.peek()?;
    self.pos += 1;
    Ok(b)
  }

  fn take(&mut self, n: usize) -> Result<Vec<u8>, String> {
    let end = self.pos.checked_add(n).ok_or("unexpected EOF")?;

    if end > self.buf.len() {
      return Err("unexpected EOF".into());
    }

    let bytes = self.buf[self.pos..end].to_vec();

    // Move the pointer ahead here
    self.pos += n;

    // Returns the actual bytes
    Ok(bytes)
  }

  // Check required bytes at a location
  fn expect(&mut self, byte: u8) -> Result<(), String> {
    if self.next()? != byte {
      return Err(format!("expected {:x}", byte));
    }

    Ok(())
  }

  // Consume if what you needed, else skip
  fn match_byte(&mut self, byte: u8) -> bool {
    if !self.done() && self.buf[self.pos] == byte {
      self.pos += 1;
      return true;
    }

    false
  }

  fn is_digit(byte: u8) -> bool {
    byte <= ASCII::NINE as u8 && byte >= ASCII::ZERO as u8
  }

  // Integer i<base10>e
  fn parse_integer(&mut self) -> Result<i64, String> {
    let mut num: i64 = 0;
    let mut sign: i64 = 1;

    if self.done() || self.peek()? != ASCII::I as u8 {
      return Err("Expected integer".into());
    }

    self.next()?;

    // Handle negative sign
    if self.match_byte(ASCII::HP as u8) {
      if self.peek()? == ASCII::ZERO as u8 || !Self::is_digit(self.peek()?) {
        return Err("Invalid integer".into());
      }

      sign = -1;
    }

    // Handle leading 0s
    if self.match_byte(ASCII::ZERO as u8) {
      self.expect(ASCII::E as u8)?;
      return Ok(0);
    }

    // Get number in base 10
    while !self.done() && Self::is_digit(self.peek()?) {
      num = num
        .checked_mul(10)
        .and_then(|n| n.checked_add((self.next().ok()? - ASCII::ZERO as u8) as i64))
        .ok_or("Integer overflow")?;
    }

    // Integer termination symbol e
    self.expect(ASCII::E as u8)?;

    num
      .checked_mul(sign)
      .ok_or_else(|| "Integer overflow".into())
  }

  // Byte strings lengthInBytes:bytes
  fn parse_byte_string(&mut self) -> Result<Vec<u8>, String> {
    if !Self::is_digit(self.peek()?) {
      return Err("Invalid call to byte string parser".into());
    }

    let mut len: usize = 0;

    // Count bytes to be consumed
    while !self.done() && Self::is_digit(self.peek()?) {
      len = len
        .checked_mul(10)
        .and_then(|n| n.checked_add((self.next().ok()? - ASCII::ZERO as u8) as usize))
        .ok_or("Invalid byte string length")?;
    }

    self.expect(ASCII::COLON as u8)?;

    // Consume len bytes from the buffer
    let bytes = self.take(len)?;

    Ok(bytes)
  }

  fn parse_list(&mut self) -> Result<Vec<BencodeVal>, String> {
    if self.done() || self.peek()? != ASCII::L as u8 {
      return Err("invalid call to list parser".into());
    }

    self.next()?;

    let mut res_list: Vec<BencodeVal> = Vec::new();

    // Parse items recursively
    while !self.done() && self.peek()? != ASCII::E as u8 {
      let item = self.parse_value()?;
      res_list.push(item);
    }

    self.expect(ASCII::E as u8)?;

    Ok(res_list)
  }

  // Dict d<pairs>e
  fn parse_dict(&mut self) -> Result<BencodeDict, String> {
    // Function to ensure sorted keys in actual byte order
    fn compare_bytes(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
      a.cmp(b)
    }

    if self.done() || self.peek()? != ASCII::D as u8 {
      return Err("Invalid call to dict parser".into());
    }

    let mut last_key_bytes: Option<Vec<u8>> = None;

    // Consume byte representing d
    self.next()?;

    let mut res: BencodeDict = BencodeDict::new();

    while !self.done() && self.peek()? != ASCII::E as u8 {
      // Check sorted
      let key_bytes = self.parse_byte_string()?;

      if let Some(last) = &last_key_bytes {
        if compare_bytes(last, &key_bytes) != std::cmp::Ordering::Less {
          return Err("Unsorted key in dict".into());
        }
      }

      let key = String::from_utf8(key_bytes.clone()).map_err(|_| "Invalid UTF-8 dictionary key")?;

      last_key_bytes = Some(key_bytes);

      let value = self.parse_value()?;

      res.insert(key, value);
    }

    self.expect(ASCII::E as u8)?;

    Ok(res)
  }

  // Choose functions to execute recursively
  fn parse_value(&mut self) -> Result<BencodeVal, String> {
    if self.done() {
      return Err("unexpected EOF".into());
    }

    let c = self.peek()?;

    if c == ASCII::I as u8 {
      return Ok(BencodeVal::Int(self.parse_integer()?));
    }

    if c == ASCII::L as u8 {
      return Ok(BencodeVal::List(self.parse_list()?));
    }

    if c == ASCII::D as u8 {
      return Ok(BencodeVal::Dict(self.parse_dict()?));
    }

    if Self::is_digit(c) {
      return Ok(BencodeVal::Bytes(self.parse_byte_string()?));
    }

    Err(format!("Unexpected byte: 0x{:x} at pos {}", c, self.pos))
  }

  // General parsing function for torrent files
  pub fn parse(&mut self) -> Result<BencodeVal, String> {
    let val = self.parse_value()?;

    if !self.done() {
      return Err(format!("Trailing data at pos {}", self.pos));
    }

    Ok(val)
  }
}
