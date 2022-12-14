use std::collections::HashMap;
use std::ops::Index;
use std::str::from_utf8;

use mysql::binlog::events::{EventData, RowsEventData};
use mysql::binlog::value::BinlogValue;
use mysql::prelude::*;
use mysql::*;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp_serde as rmps;

use rmps::Serializer;
use serde::Serialize;

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
enum IntegerValue {
    U64(u64),
    I64(i64),
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
enum FloatValue {
    F32(f32),
    F64(f64),
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
enum ChangesValue {
    Str(String),
    Int(IntegerValue),
    Float(FloatValue),
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Event {
    action: String,
    table_name: String,
    attributes: HashMap<String, ChangesValue>,
    changes: HashMap<String, Vec<ChangesValue>>,
}

#[tokio::main]
async fn main() -> core::result::Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:23578").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let url = "mysql://username:password@127.0.0.1:1234/db_name";
            let pool = Pool::new(url).unwrap();
            let mut conn = pool.get_conn().unwrap();

            // "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            let query_results = conn.query_iter("SELECT DISTINCT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'themis_development_1' ORDER BY ORDINAL_POSITION").unwrap();
            let mut table_columns_map: HashMap<String, Vec<String>> = HashMap::new();

            for query_result in query_results {
                let result = query_result.unwrap();

                if let Value::Bytes(table_name) = &result.index(0) {
                    if let Value::Bytes(column_name) = &result.index(1) {
                        let table_name = from_utf8(table_name).unwrap();
                        let column_name = from_utf8(column_name).unwrap();

                        let unknown_table = !&table_columns_map.contains_key(table_name);

                        if unknown_table {
                            let columns: Vec<String> = vec![column_name.to_string()];
                            table_columns_map.insert(table_name.to_string(), columns);
                        } else {
                            let columns = table_columns_map.get_mut(table_name).unwrap();
                            columns.push(column_name.to_string());
                        }

                        // let table_data = table_columns_map.get_key_value(table_name).unwrap();
                        // let table_data_bytes = format!("{:#?}", table_data).to_string();
                        // let table_data_bytes = table_data_bytes.as_bytes();

                        // if let Err(e) = socket.write_all(table_data_bytes).await {
                        //     eprintln!("failed to write to socket; err = {:?}", e);
                        // }
                        // if let Err(e) = socket.write_all(b"\n").await {
                        //     eprintln!("failed to write to socket; err = {:?}", e);
                        // }
                    }
                }
            }

            let request = BinlogRequest::new(1338);
            let mut binlog_stream = conn.get_binlog_stream(request).unwrap();
            let mut table_maps: HashMap<u64, binlog::events::TableMapEvent> = HashMap::new();

            while let Some(binlog_data) = binlog_stream.next() {
                let binlog_event = binlog_data.unwrap();

                if let Some(binlog_event_data) = binlog_event.read_data().unwrap() {
                    match binlog_event_data {
                        EventData::TableMapEvent(event_data) => {
                            table_maps.insert(event_data.table_id(), event_data.into_owned());
                        }
                        EventData::RowsEvent(event_data) => {
                            match &event_data {
                                RowsEventData::WriteRowsEvent(row_data) => {
                                    let table_map = table_maps.get(&row_data.table_id()).unwrap();
                                    let table_name = &table_map.table_name().to_string();

                                    if let Some(table_columns) = table_columns_map.get(table_name) {
                                        for row in row_data.rows(table_map) {
                                            let (_before, after) = row.unwrap();

                                            let after_row = after.unwrap();

                                            let mut result_hash = HashMap::new();

                                            result_hash.insert("table_name", table_name);

                                            let mut changes_hash = HashMap::new();
                                            let mut attributes_hash = HashMap::new();

                                            for (idx, after_binlog_value) in
                                                after_row.unwrap().iter().enumerate()
                                            {
                                                let before_state =
                                                    ChangesValue::Str("".to_string());

                                                let after_state = match after_binlog_value {
                                                    BinlogValue::Value(after_value) => {
                                                        match after_value {
                                                            Value::NULL => {
                                                                ChangesValue::Str("".into())
                                                            }
                                                            Value::Bytes(value) => {
                                                                ChangesValue::Str(
                                                                    from_utf8(value.as_slice())
                                                                        .unwrap()
                                                                        .to_string(),
                                                                )
                                                            }
                                                            Value::Int(value) => ChangesValue::Int(
                                                                IntegerValue::I64(*value),
                                                            ),
                                                            Value::UInt(value) => {
                                                                ChangesValue::Int(
                                                                    IntegerValue::U64(*value),
                                                                )
                                                            }
                                                            Value::Float(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F32(*value),
                                                                )
                                                            }
                                                            Value::Double(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F64(*value),
                                                                )
                                                            }
                                                            Value::Date(y, mo, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}, {}",
                                                                    y, mo, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                            Value::Time(signed, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}",
                                                                    signed, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                        }
                                                    }
                                                    _ => ChangesValue::Str("".into()),
                                                };

                                                let column_name = table_columns.get(idx).unwrap();
                                                let column_changes =
                                                    vec![before_state, after_state.clone()];

                                                attributes_hash
                                                    .insert(column_name.to_string(), after_state);
                                                changes_hash.insert(
                                                    column_name.to_string(),
                                                    column_changes,
                                                );
                                            }

                                            let mut buf = Vec::new();
                                            let event = Event {
                                                action: "create".into(),
                                                table_name: result_hash
                                                    .get("table_name")
                                                    .unwrap()
                                                    .to_string(),
                                                attributes: attributes_hash,
                                                changes: changes_hash,
                                            };

                                            event
                                                .serialize(&mut Serializer::new(&mut buf))
                                                .unwrap();

                                            if let Err(e) = socket.write_all(&buf.clone()).await {
                                                eprintln!(
                                                    "failed to write to socket; err = {:?}",
                                                    e
                                                );
                                            }
                                        }
                                    } else {
                                        println!(
                                            "[Error] Create for unknown table: {:#?}",
                                            table_name
                                        );
                                    }
                                }
                                RowsEventData::UpdateRowsEvent(row_data) => {
                                    let table_map = table_maps.get(&row_data.table_id()).unwrap();
                                    let table_name = &table_map.table_name().to_string();

                                    if let Some(table_columns) = table_columns_map.get(table_name) {
                                        for row in row_data.rows(table_map) {
                                            let (before, after) = row.unwrap();

                                            let before_row = before.unwrap();
                                            let before_state = &before_row.unwrap();

                                            let after_row = after.unwrap();

                                            let mut result_hash = HashMap::new();

                                            result_hash.insert("table_name", table_name);

                                            let mut changes_hash = HashMap::new();
                                            let mut attributes_hash = HashMap::new();

                                            for (idx, after_binlog_value) in
                                                after_row.unwrap().iter().enumerate()
                                            {
                                                let before_binlog_value =
                                                    before_state.get(idx).unwrap_or_else(|| {
                                                        &BinlogValue::Value(Value::NULL)
                                                    });

                                                let before_state = match before_binlog_value {
                                                    BinlogValue::Value(before_value) => {
                                                        match before_value {
                                                            Value::NULL => {
                                                                ChangesValue::Str("".into())
                                                            }
                                                            Value::Bytes(value) => {
                                                                ChangesValue::Str(
                                                                    from_utf8(value.as_slice())
                                                                        .unwrap()
                                                                        .to_string(),
                                                                )
                                                            }
                                                            Value::Int(value) => ChangesValue::Int(
                                                                IntegerValue::I64(*value),
                                                            ),
                                                            Value::UInt(value) => {
                                                                ChangesValue::Int(
                                                                    IntegerValue::U64(*value),
                                                                )
                                                            }
                                                            Value::Float(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F32(*value),
                                                                )
                                                            }
                                                            Value::Double(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F64(*value),
                                                                )
                                                            }
                                                            Value::Date(y, mo, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}, {}",
                                                                    y, mo, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                            Value::Time(signed, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}",
                                                                    signed, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                        }
                                                    }
                                                    _ => ChangesValue::Str("".into()),
                                                };

                                                let after_state = match after_binlog_value {
                                                    BinlogValue::Value(after_value) => {
                                                        match after_value {
                                                            Value::NULL => {
                                                                ChangesValue::Str("".into())
                                                            }
                                                            Value::Bytes(value) => {
                                                                ChangesValue::Str(
                                                                    from_utf8(value.as_slice())
                                                                        .unwrap()
                                                                        .to_string(),
                                                                )
                                                            }
                                                            Value::Int(value) => ChangesValue::Int(
                                                                IntegerValue::I64(*value),
                                                            ),
                                                            Value::UInt(value) => {
                                                                ChangesValue::Int(
                                                                    IntegerValue::U64(*value),
                                                                )
                                                            }
                                                            Value::Float(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F32(*value),
                                                                )
                                                            }
                                                            Value::Double(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F64(*value),
                                                                )
                                                            }
                                                            Value::Date(y, mo, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}, {}",
                                                                    y, mo, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                            Value::Time(signed, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}",
                                                                    signed, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                        }
                                                    }
                                                    _ => ChangesValue::Str("".into()),
                                                };

                                                let column_name = table_columns.get(idx).unwrap();

                                                attributes_hash.insert(
                                                    column_name.to_string(),
                                                    after_state.clone(),
                                                );

                                                if before_state != after_state {
                                                    let column_changes =
                                                        vec![before_state, after_state];
                                                    changes_hash.insert(
                                                        column_name.to_string(),
                                                        column_changes,
                                                    );
                                                }
                                            }

                                            let mut buf = Vec::new();
                                            let event = Event {
                                                action: "update".into(),
                                                table_name: result_hash
                                                    .get("table_name")
                                                    .unwrap()
                                                    .to_string(),
                                                attributes: attributes_hash,
                                                changes: changes_hash,
                                            };

                                            event
                                                .serialize(&mut Serializer::new(&mut buf))
                                                .unwrap();

                                            if let Err(e) = socket.write_all(&buf.clone()).await {
                                                eprintln!(
                                                    "failed to write to socket; err = {:?}",
                                                    e
                                                );
                                            }
                                        }
                                    } else {
                                        println!(
                                            "[Error] Update for unknown table: {:#?}",
                                            table_name
                                        );
                                    }
                                }
                                RowsEventData::DeleteRowsEvent(row_data) => {
                                    let table_map = table_maps.get(&row_data.table_id()).unwrap();
                                    let table_name = &table_map.table_name().to_string();

                                    if let Some(table_columns) = table_columns_map.get(table_name) {
                                        for row in row_data.rows(table_map) {
                                            let (before, _after) = row.unwrap();
                                            let before_row = before.unwrap();

                                            let mut result_hash = HashMap::new();

                                            result_hash.insert("table_name", table_name);

                                            let mut changes_hash = HashMap::new();
                                            let mut attributes_hash = HashMap::new();

                                            for (idx, before_binlog_value) in
                                                before_row.unwrap().iter().enumerate()
                                            {
                                                let before_state = match before_binlog_value {
                                                    BinlogValue::Value(before_value) => {
                                                        match before_value {
                                                            Value::NULL => {
                                                                ChangesValue::Str("".into())
                                                            }
                                                            Value::Bytes(value) => {
                                                                ChangesValue::Str(
                                                                    from_utf8(value.as_slice())
                                                                        .unwrap()
                                                                        .to_string(),
                                                                )
                                                            }
                                                            Value::Int(value) => ChangesValue::Int(
                                                                IntegerValue::I64(*value),
                                                            ),
                                                            Value::UInt(value) => {
                                                                ChangesValue::Int(
                                                                    IntegerValue::U64(*value),
                                                                )
                                                            }
                                                            Value::Float(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F32(*value),
                                                                )
                                                            }
                                                            Value::Double(value) => {
                                                                ChangesValue::Float(
                                                                    FloatValue::F64(*value),
                                                                )
                                                            }
                                                            Value::Date(y, mo, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}, {}",
                                                                    y, mo, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                            Value::Time(signed, d, h, m, s, ms) => {
                                                                let date = format!(
                                                                    "{}, {}, {}, {}, {}, {}",
                                                                    signed, d, h, m, s, ms
                                                                )
                                                                .to_string();
                                                                ChangesValue::Str(date)
                                                            }
                                                        }
                                                    }
                                                    _ => ChangesValue::Str("".into()),
                                                };

                                                let after_state = ChangesValue::Str("".into());
                                                let column_name = table_columns.get(idx).unwrap();
                                                let column_changes =
                                                    vec![before_state, after_state.clone()];

                                                attributes_hash
                                                    .insert(column_name.to_string(), after_state);
                                                changes_hash.insert(
                                                    column_name.to_string(),
                                                    column_changes,
                                                );
                                            }

                                            let mut buf = Vec::new();
                                            let event = Event {
                                                action: "delete".into(),
                                                table_name: result_hash
                                                    .get("table_name")
                                                    .unwrap()
                                                    .to_string(),
                                                attributes: attributes_hash,
                                                changes: changes_hash,
                                            };

                                            event
                                                .serialize(&mut Serializer::new(&mut buf))
                                                .unwrap();

                                            if let Err(e) = socket.write_all(&buf.clone()).await {
                                                eprintln!(
                                                    "failed to write to socket; err = {:?}",
                                                    e
                                                );
                                            }
                                        }
                                    } else {
                                        println!(
                                            "[Error] Update for unknown table: {:#?}",
                                            table_name
                                        );
                                    }
                                }
                                _ => {}
                            }

                            table_maps.remove(&event_data.table_id());
                        }
                        _ => {}
                    }
                }
            }
        });
    }
}
