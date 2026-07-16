use std::{
    collections::HashSet,
    io::{self, Read, Write},
};

const MAGIC: &[u8; 4] = b"HUFF";
const VERSION: u8 = 1;

pub(crate) fn decode<R: Read, W: Write>(mut reader: R, mut writer: W) -> io::Result<()> {
    let magic = read_array::<4>(&mut reader)?;
    if &magic != MAGIC {
        return Err(invalid_data("invalid Huffman file magic"));
    }

    let version = read_array::<1>(&mut reader)?[0];
    if version != VERSION {
        return Err(invalid_data(format!(
            "unsupported Huffman format version {version}"
        )));
    }

    let entry_count = u32::from_be_bytes(read_array(&mut reader)?);
    let character_count = u64::from_be_bytes(read_array(&mut reader)?);
    if entry_count == 0 && character_count != 0 {
        return Err(invalid_data(
            "non-empty content requires at least one prefix code",
        ));
    }

    let mut entries = Vec::with_capacity(entry_count as usize);
    let mut characters = HashSet::with_capacity(entry_count as usize);
    for _ in 0..entry_count {
        let scalar = u32::from_be_bytes(read_array(&mut reader)?);
        let character = char::from_u32(scalar)
            .ok_or_else(|| invalid_data(format!("invalid Unicode scalar value {scalar:#x}")))?;
        let code = u32::from_be_bytes(read_array(&mut reader)?);
        let bit_length = read_array::<1>(&mut reader)?[0] as u32;
        validate_code(code, bit_length)?;
        if !characters.insert(character) {
            return Err(invalid_data(format!(
                "duplicate prefix entry for {character:?}"
            )));
        }
        entries.push((character, code, bit_length));
    }

    if let Some(&(character, _, 0)) = entries.iter().find(|entry| entry.2 == 0) {
        if entries.len() != 1 {
            return Err(invalid_data(
                "a zero-length code must be the only prefix entry",
            ));
        }
        require_end(&mut reader)?;
        let mut utf8 = [0; 4];
        let encoded = character.encode_utf8(&mut utf8).as_bytes();
        for _ in 0..character_count {
            writer.write_all(encoded)?;
        }
        return writer.flush();
    }

    let mut root = DecodeNode::default();
    for (character, code, bit_length) in entries {
        root.insert(character, code, bit_length)?;
    }

    if character_count == 0 {
        require_end(&mut reader)?;
        return writer.flush();
    }

    decode_payload(&mut reader, &mut writer, &root, character_count)?;
    writer.flush()
}

fn decode_payload<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    root: &DecodeNode,
    character_count: u64,
) -> io::Result<()> {
    let mut decoded = 0u64;
    let mut current = root;

    while decoded < character_count {
        let byte = read_array::<1>(reader)?[0];
        for bit_index in (0..8).rev() {
            let bit = (byte >> bit_index) & 1;
            current = if bit == 0 {
                current.zero.as_deref()
            } else {
                current.one.as_deref()
            }
            .ok_or_else(|| invalid_data("payload does not match the prefix table"))?;

            if let Some(character) = current.character {
                let mut utf8 = [0; 4];
                writer.write_all(character.encode_utf8(&mut utf8).as_bytes())?;
                decoded += 1;
                current = root;

                if decoded == character_count {
                    let padding_mask = if bit_index == 0 {
                        0
                    } else {
                        (1u8 << bit_index) - 1
                    };
                    if byte & padding_mask != 0 {
                        return Err(invalid_data("payload has nonzero padding bits"));
                    }
                    require_end(reader)?;
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct DecodeNode {
    character: Option<char>,
    zero: Option<Box<DecodeNode>>,
    one: Option<Box<DecodeNode>>,
}

impl DecodeNode {
    fn insert(&mut self, character: char, code: u32, bit_length: u32) -> io::Result<()> {
        let mut current = self;
        for bit_index in (0..bit_length).rev() {
            if current.character.is_some() {
                return Err(invalid_data("a prefix code extends an existing code"));
            }
            let branch = if (code >> bit_index) & 1 == 0 {
                &mut current.zero
            } else {
                &mut current.one
            };
            current = branch.get_or_insert_with(Default::default);
        }

        if current.character.is_some() {
            return Err(invalid_data("duplicate prefix code"));
        }
        if current.zero.is_some() || current.one.is_some() {
            return Err(invalid_data("a prefix code contains an existing code"));
        }
        current.character = Some(character);
        Ok(())
    }
}

fn validate_code(code: u32, bit_length: u32) -> io::Result<()> {
    if bit_length > u32::BITS {
        return Err(invalid_data("Huffman code is longer than 32 bits"));
    }
    if bit_length == 0 {
        if code != 0 {
            return Err(invalid_data("zero-length Huffman code must be zero"));
        }
    } else if bit_length < u32::BITS && code >= (1u32 << bit_length) {
        return Err(invalid_data("Huffman code does not fit its bit length"));
    }
    Ok(())
}

fn read_array<const N: usize>(reader: &mut impl Read) -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn require_end(reader: &mut impl Read) -> io::Result<()> {
    let mut byte = [0; 1];
    if reader.read(&mut byte)? == 0 {
        Ok(())
    } else {
        Err(invalid_data("unexpected data after the Huffman payload"))
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::decode;
    use crate::serialize::encode;
    use std::{collections::HashMap, io::ErrorKind};

    fn round_trip(content: &str, table: &HashMap<char, (u32, u32)>) -> String {
        let mut compressed = Vec::new();
        encode(content, table, &mut compressed).unwrap();
        let mut decoded = Vec::new();
        decode(compressed.as_slice(), &mut decoded).unwrap();
        String::from_utf8(decoded).unwrap()
    }

    #[test]
    fn round_trips_codes_that_cross_byte_boundaries() {
        let table = HashMap::from([('a', (0, 1)), ('b', (0b10, 2)), ('c', (0b11, 2))]);
        assert_eq!(round_trip("abccabacabbca", &table), "abccabacabbca");
    }

    #[test]
    fn round_trips_unicode() {
        let table = HashMap::from([('a', (0, 1)), ('中', (0b10, 2)), ('🦀', (0b11, 2))]);
        assert_eq!(round_trip("中🦀a🦀中", &table), "中🦀a🦀中");
    }

    #[test]
    fn round_trips_empty_and_single_symbol_content() {
        assert_eq!(round_trip("", &HashMap::new()), "");
        assert_eq!(
            round_trip("xxxxx", &HashMap::from([('x', (0, 0))])),
            "xxxxx"
        );
    }

    #[test]
    fn rejects_bad_magic_and_version() {
        let mut bad_magic = b"NOPE\x01\0\0\0\0\0\0\0\0\0\0\0\0".to_vec();
        assert_eq!(
            decode(bad_magic.as_slice(), Vec::new()).unwrap_err().kind(),
            ErrorKind::InvalidData
        );

        bad_magic[..4].copy_from_slice(b"HUFF");
        bad_magic[4] = 2;
        assert_eq!(
            decode(bad_magic.as_slice(), Vec::new()).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn rejects_truncated_payload() {
        let table = HashMap::from([('a', (0, 1)), ('b', (1, 1))]);
        let mut compressed = Vec::new();
        encode("aaaaaaaaa", &table, &mut compressed).unwrap();
        compressed.pop();

        assert_eq!(
            decode(compressed.as_slice(), Vec::new())
                .unwrap_err()
                .kind(),
            ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn rejects_prefix_collisions() {
        let mut input = b"HUFF\x01".to_vec();
        input.extend_from_slice(&2u32.to_be_bytes());
        input.extend_from_slice(&1u64.to_be_bytes());
        for (character, code, length) in [('a', 0u32, 1u8), ('b', 0u32, 2u8)] {
            input.extend_from_slice(&(character as u32).to_be_bytes());
            input.extend_from_slice(&code.to_be_bytes());
            input.push(length);
        }
        input.push(0);

        assert_eq!(
            decode(input.as_slice(), Vec::new()).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn rejects_nonzero_padding_and_trailing_bytes() {
        let table = HashMap::from([('a', (0, 1)), ('b', (1, 1))]);
        let mut nonzero_padding = Vec::new();
        encode("a", &table, &mut nonzero_padding).unwrap();
        *nonzero_padding.last_mut().unwrap() = 1;
        assert_eq!(
            decode(nonzero_padding.as_slice(), Vec::new())
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );

        let mut trailing = Vec::new();
        encode("a", &table, &mut trailing).unwrap();
        trailing.push(0);
        assert_eq!(
            decode(trailing.as_slice(), Vec::new()).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
    }
}
