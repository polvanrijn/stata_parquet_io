use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use glob::glob;
use serde::{Deserialize, Serialize};

use crate::mapping::StataColumnInfo;

const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
const T_STOP: u8 = 0;
const T_BOOLEAN_TRUE: u8 = 1;
const T_BOOLEAN_FALSE: u8 = 2;
const T_BYTE: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;

#[derive(Debug, Clone, Default)]
pub struct StataMetadata {
    pub label: String,
    pub comment: String,
    pub format: String,
    pub stata_type: String,
    pub value_label_name: String,
    pub value_labels: Vec<StataValueLabelMetadata>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StataValueLabelMetadata {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Serialize)]
struct StataMetadataFileOut {
    version: u8,
    variables: HashMap<String, StataMetadataJson>,
}

#[derive(Debug, Deserialize)]
struct StataMetadataFile {
    variables: HashMap<String, StataMetadataJson>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StataMetadataJson {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    label: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    comment: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    notes: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    format: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    stata_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    value_label_name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    value_labels: Vec<StataValueLabelMetadata>,
}

struct CompactProtocol<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> CompactProtocol<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_u8(&mut self) -> Option<u8> {
        let value = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(value)
    }

    fn read_exact(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let out = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }

    fn read_varint(&mut self) -> Option<u64> {
        let mut shift = 0u32;
        let mut result = 0u64;
        loop {
            let byte = self.read_u8()?;
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }

    fn read_zigzag_i64(&mut self) -> Option<i64> {
        let raw = self.read_varint()?;
        Some(((raw >> 1) as i64) ^ (-((raw & 1) as i64)))
    }

    fn read_binary(&mut self) -> Option<String> {
        let len = self.read_varint()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    fn read_field_begin(&mut self, previous_field_id: i16) -> Option<(u8, i16)> {
        let header = self.read_u8()?;
        let field_type = header & 0x0f;
        if field_type == T_STOP {
            return Some((T_STOP, previous_field_id));
        }

        let modifier = (header & 0xf0) >> 4;
        let field_id = if modifier == 0 {
            self.read_zigzag_i64()? as i16
        } else {
            previous_field_id + modifier as i16
        };
        Some((field_type, field_id))
    }

    fn read_list_begin(&mut self) -> Option<(u8, usize)> {
        let header = self.read_u8()?;
        let elem_type = header & 0x0f;
        let size_nibble = (header & 0xf0) >> 4;
        let size = if size_nibble == 15 {
            self.read_varint()? as usize
        } else {
            size_nibble as usize
        };
        Some((elem_type, size))
    }

    fn skip_field(&mut self, field_type: u8) -> Option<()> {
        match field_type {
            T_STOP => Some(()),
            T_BOOLEAN_TRUE | T_BOOLEAN_FALSE => Some(()),
            T_BYTE => self.read_u8().map(|_| ()),
            T_I16 | T_I32 | T_I64 => self.read_zigzag_i64().map(|_| ()),
            T_DOUBLE => self.read_exact(8).map(|_| ()),
            T_BINARY => {
                let len = self.read_varint()? as usize;
                self.read_exact(len).map(|_| ())
            }
            T_STRUCT => self.skip_struct(),
            T_LIST | T_SET => {
                let (elem_type, size) = self.read_list_begin()?;
                for _ in 0..size {
                    self.skip_field(elem_type)?;
                }
                Some(())
            }
            T_MAP => {
                let size = self.read_varint()? as usize;
                if size == 0 {
                    return Some(());
                }
                let type_header = self.read_u8()?;
                let key_type = (type_header & 0xf0) >> 4;
                let value_type = type_header & 0x0f;
                for _ in 0..size {
                    self.skip_field(key_type)?;
                    self.skip_field(value_type)?;
                }
                Some(())
            }
            _ => None,
        }
    }

    fn skip_struct(&mut self) -> Option<()> {
        let mut previous_field_id = 0i16;
        loop {
            let (field_type, field_id) = self.read_field_begin(previous_field_id)?;
            if field_type == T_STOP {
                return Some(());
            }
            previous_field_id = field_id;
            self.skip_field(field_type)?;
        }
    }

    fn read_key_value_struct(&mut self) -> Option<(String, String)> {
        let mut key = String::new();
        let mut value = String::new();
        let mut previous_field_id = 0i16;

        loop {
            let (field_type, field_id) = self.read_field_begin(previous_field_id)?;
            if field_type == T_STOP {
                return Some((key, value));
            }
            previous_field_id = field_id;

            match (field_id, field_type) {
                (1, T_BINARY) => key = self.read_binary()?,
                (2, T_BINARY) => value = self.read_binary()?,
                _ => self.skip_field(field_type)?,
            }
        }
    }
}

fn first_physical_parquet_file(path: &str) -> Option<String> {
    let path_obj = Path::new(path);
    if path_obj.is_file() {
        return Some(path.to_string());
    }

    let mut pattern = if path_obj.is_dir() {
        let mut base = path.to_string();
        if base.ends_with('/') || base.ends_with('\\') {
            base.pop();
        }
        format!("{}/**/*.parquet", base.replace('\\', "/"))
    } else {
        path.replace('\\', "/")
    };

    if pattern.contains("**.") {
        pattern = pattern.replace("**.", "**/*.");
    }

    glob(&pattern)
        .ok()?
        .filter_map(Result::ok)
        .find(|p| p.is_file())
        .map(|p| p.to_string_lossy().to_string())
}

fn read_footer_metadata(path: &str) -> Option<Vec<u8>> {
    let physical_path = first_physical_parquet_file(path)?;
    let mut file = File::open(physical_path).ok()?;
    let len = file.metadata().ok()?.len();
    if len < 8 {
        return None;
    }

    file.seek(SeekFrom::End(-8)).ok()?;
    let mut tail = [0u8; 8];
    file.read_exact(&mut tail).ok()?;
    if &tail[4..8] != PARQUET_MAGIC {
        return None;
    }

    let metadata_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
    if metadata_len > len.saturating_sub(8) {
        return None;
    }

    file.seek(SeekFrom::Start(len - 8 - metadata_len)).ok()?;
    let mut metadata = vec![0u8; metadata_len as usize];
    file.read_exact(&mut metadata).ok()?;
    Some(metadata)
}

fn read_parquet_key_value_metadata(path: &str) -> Option<HashMap<String, String>> {
    let footer = read_footer_metadata(path)?;
    let mut protocol = CompactProtocol::new(&footer);
    let mut previous_field_id = 0i16;

    loop {
        let (field_type, field_id) = protocol.read_field_begin(previous_field_id)?;
        if field_type == T_STOP {
            return None;
        }
        previous_field_id = field_id;

        if field_id == 5 && field_type == T_LIST {
            let (elem_type, size) = protocol.read_list_begin()?;
            if elem_type != T_STRUCT {
                return None;
            }

            let mut out = HashMap::new();
            for _ in 0..size {
                let (key, value) = protocol.read_key_value_struct()?;
                if !key.is_empty() {
                    out.insert(key, value);
                }
            }
            return Some(out);
        }

        protocol.skip_field(field_type)?;
    }
}

/// Returns the raw `stata.variable_metadata` JSON string embedded in a parquet
/// file/dataset, if present. Used to carry metadata through operations (such as
/// directory consolidation) that rewrite the file.
pub fn read_stata_variable_metadata_raw(path: &str) -> Option<String> {
    let kv = read_parquet_key_value_metadata(path)?;
    kv.get("stata.variable_metadata").cloned()
}

pub fn read_stata_variable_metadata(path: &str) -> HashMap<String, StataMetadata> {
    let Some(kv) = read_parquet_key_value_metadata(path) else {
        return HashMap::new();
    };
    let Some(json) = kv.get("stata.variable_metadata") else {
        return HashMap::new();
    };
    let Ok(parsed) = serde_json::from_str::<StataMetadataFile>(json) else {
        return HashMap::new();
    };

    parsed
        .variables
        .into_iter()
        .map(|(name, info)| {
            let mut notes = info.notes;
            if notes.is_empty() && !info.comment.is_empty() {
                notes.push(info.comment.clone());
            }
            (
                name,
                StataMetadata {
                    label: info.label,
                    comment: info.comment,
                    format: info.format,
                    stata_type: info.stata_type,
                    value_label_name: info.value_label_name,
                    value_labels: info.value_labels,
                    notes,
                },
            )
        })
        .collect()
}

pub fn stata_variable_metadata_json(column_info: &[StataColumnInfo]) -> Option<String> {
    let mut variables = HashMap::new();

    for col in column_info {
        let value_labels = col
            .value_labels
            .iter()
            .map(|item| StataValueLabelMetadata {
                value: item.value.clone(),
                label: item.label.clone(),
            })
            .collect::<Vec<_>>();

        let metadata = StataMetadataJson {
            label: col.variable_label.clone(),
            comment: col.notes.first().cloned().unwrap_or_default(),
            notes: col.notes.clone(),
            format: col.format.clone(),
            stata_type: stata_type_name(&col.dtype, col.str_length),
            value_label_name: col.value_label_name.clone(),
            value_labels,
        };

        variables.insert(col.name.clone(), metadata);
    }

    if variables.is_empty() {
        return None;
    }

    serde_json::to_string(&StataMetadataFileOut {
        version: 1,
        variables,
    }).ok()
}

fn stata_type_name(dtype: &str, str_length: usize) -> String {
    let dtype_lower = dtype.to_ascii_lowercase();
    if dtype_lower == "string" {
        format!("str{}", str_length.max(1))
    } else if dtype_lower == "strl" {
        "strL".to_string()
    } else {
        dtype_lower
    }
}
