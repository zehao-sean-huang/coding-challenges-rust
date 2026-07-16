use std::{
    collections::HashMap,
    io::{self, Write},
};

const MAGIC: &[u8; 4] = b"HUFF";
const VERSION: u8 = 1;

pub(crate) fn encode<W: Write>(
    content: &str,
    prefix_table: &HashMap<char, (u32, u32)>,
    mut writer: W,
) -> io::Result<()> {
    let entry_count = u32::try_from(prefix_table.len())
        .map_err(|_| invalid_data("prefix table has too many entries"))?;
    let character_count = u64::try_from(content.chars().count())
        .map_err(|_| invalid_data("input has too many characters"))?;

    writer.write_all(MAGIC)?;
    writer.write_all(&[VERSION])?;
    writer.write_all(&entry_count.to_be_bytes())?;
    writer.write_all(&character_count.to_be_bytes())?;

    let mut entries = prefix_table.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(character, _)| **character as u32);

    for (character, &(code, bit_length)) in entries {
        validate_code(code, bit_length)?;
        writer.write_all(&(*character as u32).to_be_bytes())?;
        writer.write_all(&code.to_be_bytes())?;
        writer.write_all(&[bit_length as u8])?;
    }

    let mut bit_writer = BitWriter::new(writer);
    for character in content.chars() {
        let &(code, bit_length) = prefix_table
            .get(&character)
            .ok_or_else(|| invalid_data(format!("missing prefix code for {character:?}")))?;
        validate_code(code, bit_length)?;
        bit_writer.write_code(code, bit_length)?;
    }
    bit_writer.finish()
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

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

struct BitWriter<W> {
    writer: W,
    byte: u8,
    used_bits: u8,
}

impl<W: Write> BitWriter<W> {
    fn new(writer: W) -> Self {
        Self {
            writer,
            byte: 0,
            used_bits: 0,
        }
    }

    fn write_code(&mut self, code: u32, bit_length: u32) -> io::Result<()> {
        for bit_index in (0..bit_length).rev() {
            let bit = ((code >> bit_index) & 1) as u8;
            self.byte |= bit << (7 - self.used_bits);
            self.used_bits += 1;

            if self.used_bits == 8 {
                self.writer.write_all(&[self.byte])?;
                self.byte = 0;
                self.used_bits = 0;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        if self.used_bits != 0 {
            self.writer.write_all(&[self.byte])?;
        }
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::encode;
    use std::collections::HashMap;

    const HEADER_LEN: usize = 4 + 1 + 4 + 8;
    const ENTRY_LEN: usize = 4 + 4 + 1;

    fn encode_to_vec(content: &str, table: &HashMap<char, (u32, u32)>) -> Vec<u8> {
        let mut output = Vec::new();
        encode(content, table, &mut output).unwrap();
        output
    }

    #[test]
    fn writes_an_exact_empty_header() {
        let output = encode_to_vec("", &HashMap::new());

        assert_eq!(
            output,
            [
                b'H', b'U', b'F', b'F', 1, // magic and version
                0, 0, 0, 0, // entry count
                0, 0, 0, 0, 0, 0, 0, 0, // character count
            ]
        );
    }

    #[test]
    fn sorts_unicode_header_entries_by_code_point() {
        let table = HashMap::from([('🦀', (1, 1)), ('a', (0, 1))]);
        let output = encode_to_vec("a🦀", &table);

        let first_character =
            u32::from_be_bytes(output[HEADER_LEN..HEADER_LEN + 4].try_into().unwrap());
        let second_start = HEADER_LEN + ENTRY_LEN;
        let second_character =
            u32::from_be_bytes(output[second_start..second_start + 4].try_into().unwrap());

        assert_eq!(first_character, 'a' as u32);
        assert_eq!(second_character, '🦀' as u32);
    }

    #[test]
    fn packs_codes_most_significant_bit_first_across_bytes() {
        let table = HashMap::from([('a', (0b0, 1)), ('b', (0b10, 2)), ('c', (0b11, 2))]);
        let output = encode_to_vec("abccaba", &table);
        let payload_start = HEADER_LEN + table.len() * ENTRY_LEN;

        assert_eq!(&output[payload_start..], &[0b01011110, 0b10000000]);
    }

    #[test]
    fn zero_length_code_needs_no_payload_bits() {
        let table = HashMap::from([('x', (0, 0))]);
        let output = encode_to_vec("xxxxx", &table);

        assert_eq!(output.len(), HEADER_LEN + ENTRY_LEN);
    }

    #[test]
    fn output_is_deterministic_across_hashmap_insertion_order() {
        let first = HashMap::from([('b', (1, 1)), ('a', (0, 1))]);
        let mut second = HashMap::new();
        second.insert('a', (0, 1));
        second.insert('b', (1, 1));

        assert_eq!(
            encode_to_vec("abba", &first),
            encode_to_vec("abba", &second)
        );
    }

    #[test]
    fn rejects_a_code_that_does_not_fit_its_length() {
        let table = HashMap::from([('a', (0b10, 1))]);
        let error = encode("a", &table, Vec::new()).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
