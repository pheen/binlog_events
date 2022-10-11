use std::borrow::Borrow;
use std::collections::HashMap;
use std::io::Read;
use std::ops::Index;
use std::str::from_utf8;

use mysql::*;
use mysql::binlog::events::Event;
use mysql::binlog::events::EventData;
use mysql::binlog::events::RowsEventData;
use mysql::binlog::events::TableMapEvent;
use mysql::binlog::events::UpdateRowsEvent;
use mysql::prelude::*;
use mysql::BinlogStream;
use mysql::binlog::BinlogVersion;
use mysql::BinlogRequest;

#[macro_use]
extern crate rutie;

use rutie::{Class, Object, RString, VM};

class!(BinlogEvents);

methods!(
    BinlogEvents,
    _rtself,
    fn pub_listen(mysql_url: RString) -> RString {
        // if !VM::is_block_given() {
        //     //     let argument = ruby_string.to_any_object();
        //     //     let result = VM::yield_object(argument);
        //     // } else {
        //     VM::raise(
        //         Class::from_existing("LocalJumpError"),
        //         "no block given (yield)",
        //     );
        //     unreachable!();
        // }

        let ruby_string = mysql_url.map_err(|e| VM::raise_ex(e)).unwrap();
        // let url = ruby_string.to_str();
        let url = "mysql://root:root@127.0.0.1:3306/themis_development_1";
        // let pool = Pool::new(url).unwrap();
        match Pool::new(url) {
            Ok(_) => {},
            Err(err) => { println!("{:#?}", err); }
        }
        // let mut conn = pool.get_conn().unwrap();

        // for result in conn.query_iter("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS ORDER BY ORDINAL_POSITION").unwrap() {
        //     let result = result.unwrap();
        //     let table_name_result = result.index(0);

        //     if let Value::Bytes(table_name) = table_name_result {
        //         let name = from_utf8(table_name).unwrap();
        //         let name = RString::new_utf8(name);
        //         VM::yield_object(name);
        //         // println!("{:#?}", from_utf8(table_name).unwrap());
        //     }
        // }

        RString::new_utf8("Done")

        // RString::new_utf8(
        //     &ruby_string.
        //     to_string().
        //     chars().
        //     rev().
        //     collect::<String>()
        // )
    }
);

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_binlog_events() {
    Class::new("BinlogEventsInterface", None).define(|klass| {
        klass.def_self("listen", pub_listen);
    });
}
