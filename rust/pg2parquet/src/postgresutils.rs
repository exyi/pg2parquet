use postgres::{self, Row, types::Kind};
use uuid::Uuid;

pub fn identify_row(row: &Row) -> String {

	// first row with simple data type
	for (i, column) in row.columns().iter().enumerate() {
		let t = column.type_();
		if t.kind().clone() == Kind::Simple {
			match t.name() {
				"text" => {
					match row.get::<usize, Option<String>>(i) {
						Some(v) =>
							return format!("{}={:?}", column.name(), v),
						None => ()
					}
				},
				"oid" => {
					match row.get::<usize, Option<u32>>(i) {
						Some(v) =>
							return format!("{}={:?}", column.name(), v),
						None => ()
					}
				},
				"int4" => {
					match row.get::<usize, Option<i32>>(i) {
						Some(v) =>
							return format!("{}={:?}", column.name(), v),
						None => ()
					}
				},
				"int8" => {
					match row.get::<usize, Option<i64>>(i) {
						Some(v) =>
							return format!("{}={:?}", column.name(), v),
						None => ()
					}
				},
				"uuid" => {
					match row.get::<usize, Option<Uuid>>(i) {
						Some(v) =>
							return format!("{}={:?}", column.name(), v),
						None => ()
					}
				},
				_ => ()
			}
		}
	}
	"Row ¯\\_(ツ)_/¯".to_owned()
}
