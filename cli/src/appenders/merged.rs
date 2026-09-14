use std::{sync::Arc, marker::PhantomData};
use super::{ColumnAppenderBase, ColumnAppender, DynColumnAppender};
use arrow::array::{Array, ArrayRef, NullBufferBuilder, StructArray};
use arrow::datatypes::{Field, Fields};

pub struct DynamicMergedAppender<T> {
	columns: Vec<DynColumnAppender<T>>,
	// Child names and nullability are fixed at construction; types come from their builders.
	fields: Vec<(String, bool)>,

	// Track parent-level presence per row so we can set Struct nulls correctly in Arrow
	present: NullBufferBuilder,
}

impl<T> DynamicMergedAppender<T> {
	pub fn new(columns: Vec<DynColumnAppender<T>>, fields: Vec<(String, bool)>) -> Self {
		assert_eq!(columns.len(), fields.len());
		DynamicMergedAppender { columns, fields, present: NullBufferBuilder::new(1024) }
	}
}

impl<T> ColumnAppenderBase for DynamicMergedAppender<T> {
	fn write_null(&mut self) -> Result<usize, String> {
		self.present.append_null();
		let mut total = 0;
		for x in &mut self.columns {
		    total += x.write_null()?;
		}
		Ok(total.max(1))
	}

	fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
		let rows = self.present.len();
		let mut child_arrays = Vec::with_capacity(self.columns.len());
		let mut fields = Vec::with_capacity(self.columns.len());
		for (col, (name, nullable)) in self.columns.iter_mut().zip(&self.fields) {
			let arr = col.to_arrow_array()?;
			fields.push(Field::new(name, arr.data_type().clone(), *nullable));
			child_arrays.push(arr);
		}
		let validity = self.present.finish();

		let arrow_fields = fields.into();
		let sa = StructArray::try_new_with_length(arrow_fields, child_arrays, validity, rows)
			.map_err(|e| format!("Failed to create StructArray: {e}"))?;
		Ok(Arc::new(sa) as ArrayRef)
	}
}

impl<T> ColumnAppender<T> for DynamicMergedAppender<T> {
	fn copy_value(&mut self, reader: &T) -> Result<usize, String> {
		// Record a non-null struct row
		self.present.append_non_null();
		let mut total = 0;
		for c in self.columns.iter_mut() {
			total += c.copy_value(reader)?;
		}
		Ok(total.max(1))
	}
}

pub fn new_static_merged_appender<T>() -> impl StaticMergedAppender<T> {
    StaticMergedAppenderNil { null_buffer: NullBufferBuilder::new(1024) }
}

pub trait StaticMergedAppender<T>: ColumnAppender<T> {
    fn to_struct_array(&mut self) -> Result<StructArray, String>;

    fn add_appender<A: ColumnAppender<T>>(self, name: impl Into<String>, nullable: bool, appender: A) -> StaticMergedAppenderImpl<T, A, Self>
        where Self: Sized {
        StaticMergedAppenderImpl { appender, next: self, name: name.into(), nullable, _dummy: PhantomData }
    }
}

pub struct StaticMergedAppenderImpl<T, TAppender: ColumnAppender<T>, Next: StaticMergedAppender<T>> {
    name: String,
    nullable: bool,
    pub appender: TAppender,
    pub next: Next,
    pub _dummy: PhantomData<T>
}

impl<T, TAppender: ColumnAppender<T>, Next: StaticMergedAppender<T>> ColumnAppenderBase for StaticMergedAppenderImpl<T, TAppender, Next> {
	fn write_null(&mut self) -> Result<usize, String> {
		let x = self.next.write_null()?;
		let y = self.appender.write_null()?;
		Ok((x + y).max(1))
    }

    fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
	Ok(Arc::new(StaticMergedAppender::to_struct_array(self)?))
    }
}

impl<T, TAppender: ColumnAppender<T>, Next: StaticMergedAppender<T>> ColumnAppender<T> for StaticMergedAppenderImpl<T, TAppender, Next> {
    fn copy_value(&mut self, reader: &T) -> Result<usize, String> {
		let x = self.next.copy_value(reader)?;
        let y = self.appender.copy_value(reader)?;
		Ok((x + y).max(1))
    }
}

impl<T, TAppender: ColumnAppender<T>, Next: StaticMergedAppender<T>> StaticMergedAppender<T> for StaticMergedAppenderImpl<T, TAppender, Next> {
    fn to_struct_array(&mut self) -> Result<StructArray, String> {
	let next_struct = self.next.to_struct_array()?;
	let len = next_struct.len();
	let (fields, mut arrays, nulls) = next_struct.into_parts();
	let mut fields = fields.to_vec();
        let array = self.appender.to_arrow_array()?;
	fields.push(Arc::new(Field::new(&self.name, array.data_type().clone(), self.nullable)));
	arrays.push(array);
	Ok(StructArray::try_new_with_length(
		fields.into(),
		arrays,
		nulls,
		len).unwrap())
    }
}

pub struct StaticMergedAppenderNil {
    pub null_buffer: NullBufferBuilder
}

impl StaticMergedAppenderNil {
    fn to_struct_array(&mut self) -> StructArray {
	let len = self.null_buffer.len();
	let nulls = self.null_buffer.finish();
        StructArray::try_new_with_length(Fields::empty(), vec![], nulls, len).unwrap()
    }
}

impl<T> StaticMergedAppender<T> for StaticMergedAppenderNil {
    fn to_struct_array(&mut self) -> Result<StructArray, String> {
	Ok(self.to_struct_array())
    }
}

impl ColumnAppenderBase for StaticMergedAppenderNil {
    fn write_null(&mut self) -> Result<usize, String> {
	self.null_buffer.append_null();
        Ok(1)
    }

    fn to_arrow_array(&mut self) -> Result<ArrayRef, String> {
	Ok(Arc::new(self.to_struct_array()))
    }
}

impl<T> ColumnAppender<T> for StaticMergedAppenderNil {
	fn copy_value(&mut self, _reader: &T) -> Result<usize, String> {
	self.null_buffer.append_non_null();
		Ok(1)
    }
}
