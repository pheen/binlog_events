use std::collections::HashMap;
use std::io::Read;
use std::ops::Index;
use std::str::from_utf8;

use mysql::binlog::events::{EventData, RowsEventData, RowsEvent};
use mysql::binlog::value::BinlogValue;
use mysql::prelude::*;
use mysql::*;

use mysql::serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> core::result::Result<(), Box<dyn std::error::Error>> {
    println!("Hello!");

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

                        let table_data = table_columns_map.get_key_value(table_name).unwrap();
                        let table_data_bytes = format!("{:#?}", table_data).to_string();
                        let table_data_bytes = table_data_bytes.as_bytes();

                        if let Err(e) = socket.write_all(table_data_bytes).await {
                            eprintln!("failed to write to socket; err = {:?}", e);
                        }
                        if let Err(e) = socket.write_all(b"\n").await {
                            eprintln!("failed to write to socket; err = {:?}", e);
                        }
                    }
                }
            }

            let request = BinlogRequest::new(1337);
            let mut binlog_stream = conn.get_binlog_stream(request).unwrap();

            let mut table_maps: HashMap<u64, binlog::events::TableMapEvent> = HashMap::new();

            while let Some(binlog_data) = binlog_stream.next() {
                let ev = binlog_data.unwrap();

                if let Some(thing) = ev.read_data().unwrap() {
                    match thing {
                        EventData::TableMapEvent(data) => {
                            table_maps.insert(data.table_id(), data.into_owned());
                        },
                        EventData::RowsEvent(data) => {
                            match data {
                                RowsEventData::WriteRowsEvent(row_data) => {

                                },
                                RowsEventData::UpdateRowsEvent(row_data) => {
                                    let table_map = table_maps.get(&row_data.table_id()).unwrap();
                                    let table_name = &table_map.table_name().to_string();

                                    println!("table_name: {}", table_name);

                                    if let Some(table_columns) = table_columns_map.get(table_name) {
                                        // println!("row_data:");

                                        for row in row_data.rows(table_map) {
                                            let (before, after) = row.unwrap();

                                            let before_row = before.unwrap();
                                            let before = &before_row.unwrap();

                                            let after_row = after.unwrap();
                                            let after = &after_row.unwrap();

                                            let mut result_hash = HashMap::new();
                                            let mut id_serialized_value: String = "".to_string();

                                            result_hash.insert("table_name", table_name);

                                            let id_column_index = table_columns
                                                .iter()
                                                .position(|name| name == "id")
                                                .unwrap();

                                            // let id_ref = after_row.as_ref(id_column_index).unwrap();

                                            // match id_ref {
                                            //     BinlogValue::Value(id_value) => {
                                            //         match *id_value {
                                            //             Value::Int(id) => {
                                            //                 id_serialized_value.push_str(&id.to_string());
                                            //                 result_hash.insert("id", &id_serialized_value);
                                            //             },
                                            //             _ => {}
                                            //         }
                                            //     },
                                            //     _ => {},
                                            // }

                                            let mut changes_hash = HashMap::new();

                                            for (idx, after_binlog_value) in after.iter().enumerate() {
                                                let before_binlog_value = before.get(idx).unwrap_or_else(|| &BinlogValue::Value(Value::NULL));

                                                let before_state = match before_binlog_value {
                                                    BinlogValue::Value(before_value) => {
                                                        match before_value {
                                                            Value::NULL => { "".to_string() },
                                                            Value::Bytes(value) => { from_utf8(value.as_slice()).unwrap().to_string() },
                                                            Value::Int(value) => { value.to_string() },
                                                            Value::UInt(value) => { value.to_string() },
                                                            Value::Float(value) => { value.to_string() },
                                                            Value::Double(value) => { value.to_string() },
                                                            Value::Date(y, mo, d, h, m, s, ms) => { format!("{}, {}, {}, {}, {}, {}, {}", y, mo, d, h, m, s, ms).to_string() },
                                                            Value::Time(signed, d, h, m, s, ms) => { format!("{}, {}, {}, {}, {}, {}", signed, d, h, m, s, ms).to_string() },
                                                        }
                                                    },
                                                    _ => { "".to_string() }
                                                };

                                                let after_state = match after_binlog_value {
                                                    BinlogValue::Value(after_value) => {
                                                        match after_value {
                                                            Value::NULL => { "".to_string() },
                                                            Value::Bytes(value) => { from_utf8(value.as_slice()).unwrap().to_string() },
                                                            Value::Int(value) => { value.to_string() },
                                                            Value::UInt(value) => { value.to_string() },
                                                            Value::Float(value) => { value.to_string() },
                                                            Value::Double(value) => { value.to_string() },
                                                            Value::Date(y, mo, d, h, m, s, ms) => { format!("{}, {}, {}, {}, {}, {}, {}", y, mo, d, h, m, s, ms).to_string() },
                                                            Value::Time(signed, d, h, m, s, ms) => { format!("{}, {}, {}, {}, {}, {}", signed, d, h, m, s, ms).to_string() },
                                                        }
                                                    },
                                                    _ => { "".to_string() }
                                                };

                                                let column_name = table_columns.get(idx).unwrap();

                                                if before_state != after_state {
                                                    let column_changes = vec![before_state, after_state];
                                                    changes_hash.insert(column_name, column_changes);
                                                }
                                            }

                                            let changes_json = json!(changes_hash).to_string();
                                            result_hash.insert("changes", &changes_json);

                                            println!("{:#?}", &result_hash);
                                        }
                                    } else {
                                        println!("[Error] Update for unknown table: {:#?}", table_name);
                                    }
                                },
                                RowsEventData::DeleteRowsEvent(row_data) => {

                                },
                                _ => {}
                            }
                        },
                        _ => {}
                    }
                }
            }
        });
    }
}
