use std::collections::HashMap;
use std::ops::Index;
use std::str::from_utf8;

use mysql::*;
use mysql::binlog::events::{EventData, RowsEventData};
use mysql::prelude::*;

use tokio::net::TcpListener;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> core::result::Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:23578").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let url = "mysql://root:root@127.0.0.1:3306/themis_development_1";
            let pool = Pool::new(url).unwrap();
            let mut conn = pool.get_conn().unwrap();

            // "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            let query_results = conn.query_iter("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY ORDINAL_POSITION").unwrap();
            let mut table_columns_map = HashMap::new();

            for query_result in query_results {
                let result = query_result.unwrap();

                let table_name_result = result.index(0).clone();
                let column_name_result = result.index(1).clone();



                if let Value::Bytes(table_name) = table_name_result {
                    if let Value::Bytes(column_name) = column_name_result {
                        // let b = table_name.clone(); // b: &Vec<u8>
                        // let table_name: &[u8] = &b; // c: &[u8]
                        // let column_name: &[u8] = &column_name.clone(); // c: &[u8]

                        let table_name = from_utf8(&table_name).unwrap();
                        let column_name = from_utf8(&column_name).unwrap();


                        if table_columns_map.contains_key(table_name) {
                            let mut columns: Vec<String> = Vec::new();
                            table_columns_map.insert(table_name, columns);
                        }

                        // table_columns_map

                        // table_columns_map. .insert(table_name)

                        // if let Err(e) = socket.write_all(table_name).await {
                        //     eprintln!("failed to write to socket; err = {:?}", e);
                        // }
                        // if let Err(e) = socket.write_all(b"\n").await {
                        //     eprintln!("failed to write to socket; err = {:?}", e);
                        // }
                    }
                }
            }

            let request = BinlogRequest::new(1337);
            let mut binlog_stream = conn.get_binlog_stream(request).unwrap();

            while let Some(event) = binlog_stream.next() {
                let ev = event.unwrap();
                // event.header().event_type().unwrap();

                let mut tmes = HashMap::new();

                if let Some(EventData::TableMapEvent(data)) = ev.read_data().unwrap() {
                    tmes.insert(data.table_id(), data.into_owned());
                }

                if let Some(EventData::RowsEvent(data)) = ev.read_data().unwrap() {
                    let table_map_event = &tmes[&data.table_id()];

                    for row in data.rows(table_map_event) {
                        let (before, after) = row.unwrap();
                        match data {
                            RowsEventData::WriteRowsEvent(_) => {
                                // assert!(before.is_none());
                                // let after = after.unwrap().unwrap();
                                // let mut j = 0;
                                // for v in after {
                                //     j += 1;
                                //     match j {
                                //         1 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".into())),
                                //         2 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                //         3 => assert_eq!(v, BinlogValue::Value(1_i8.into())),
                                //         4 => assert_eq!(v, BinlogValue::Value([0b00000101_u8].into())),
                                //         5 => assert_eq!(v, BinlogValue::Value("0123456789".into())),

                                //         _ => panic!(),
                                //     }
                                // }
                                // assert_eq!(j, 5);
                            }
                            RowsEventData::UpdateRowsEvent(_) => {
                                if let Some(val) = before {
                                    let table_name = table_map_event.table_name();
                                    // let mut column_names: Vec<&str> = Vec::new();

                                    // if let Some(names) = table_column_names.get(&table_name) {
                                    //     // for name in
                                    // } else {
                                    //     column_names.push("hi");
                                    //     table_column_names.insert(table_name, column_names);
                                    // };

                                    // println!("{:#?}", column_names);


                                    // for value in val.unwrap() {
                                    //     row_map.insert(index, value);
                                    //     index += 1;
                                    // }

                                    // println!("{:#?}", row_map);

                                    // val.columns()
                                    // let values = val.unwrap();

                                    // for col in val.columns_ref() {
                                    //     let wut = table_map_event.get_column_type(col_idx) .get_column_metadata(index).unwrap();

                                    //     index += 1;

                                    //     println!("{:#?}", wut);
                                    // }


                                    // for col in data.columns_before_image() {
                                    //     println!("{:#?}", col.);
                                    // }

                                }

                                // for val in before {
                                //     println!("{:#?}", val);
                                // }

                                // let before = before.unwrap().unwrap();
                                // let mut j = 0;
                                // for v in before {
                                //     j += 1;
                                //     match j {
                                //         1 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789".into())),
                                //         2 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                //         3 => assert_eq!(v, BinlogValue::Value(1_i8.into())),
                                //         4 => assert_eq!(v, BinlogValue::Value([0b00000101_u8].into())),
                                //         5 => assert_eq!(v, BinlogValue::Value("0123456789".into())),

                                //         _ => panic!(),
                                //     }
                                // }
                                // assert_eq!(j, 5);

                                // let after = after.unwrap().unwrap();
                                // let mut j = 0;
                                // for v in after {
                                //     j += 1;
                                //     match j {
                                //         1 => assert_eq!(v, BinlogValue::Value("field1".into())),
                                //         2 => assert_eq!(v, BinlogValue::Value("field_2".into())),
                                //         3 => assert_eq!(v, BinlogValue::Value(2_i8.into())),
                                //         4 => assert_eq!(v, BinlogValue::Value([0b00001010_u8].into())),
                                //         5 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                //         _ => panic!(),
                                //     }
                                // }
                                // assert_eq!(j, 5);
                            }
                            RowsEventData::DeleteRowsEvent(_) => {
                                // assert!(after.is_none());

                                // let before = before.unwrap().unwrap();
                                // let mut j = 0;
                                // for v in before {
                                //     j += 1;
                                //     match j {
                                //         1 => assert_eq!(v, BinlogValue::Value("field1".into())),
                                //         2 => assert_eq!(v, BinlogValue::Value("field_2".into())),
                                //         3 => assert_eq!(v, BinlogValue::Value(2_i8.into())),
                                //         4 => assert_eq!(v, BinlogValue::Value([0b00001010_u8].into())),
                                //         5 => assert_eq!(v, BinlogValue::Value("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456780123456789012345678901234567890123456789".into())),
                                //         _ => panic!(),
                                //     }
                                // }
                                // assert_eq!(j, 5);
                            }
                            _ => panic!(),
                        }
                    }
                }
            }
        });
    }
}
