use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, format, Write};
use std::process::Output;
use std::str::FromStr;
use bigdecimal::BigDecimal;
use chrono::format;
use clap::Error;
use parquet::data_type::Decimal;
use postgres::Client;
use postgres::types::{Format, Kind, ToSql, Type as PgType};
use serde_json as json;
use serde::{Deserialize, Serialize};

use crate::appenders::ArrayColumnAppender;
use crate::pg_utils::explain_parser::{ExplainJson, ExplainJsonPlan};
use crate::pg_utils::explain_resolver::ResolvingData;
use crate::pg_utils::schema_queries::{self, ColumnInformation};
use crate::pg_utils::type_shenanigans::ParsedType;
use crate::rustutils::{ArrayDeconstructor, AsStrRef};


/// Collect all columns from tables touched by the query - including numeric columns
fn get_queries_columns(client: &mut Client, query: &ExplainJsonPlan) -> Result<HashMap<(String, String), HashMap<String, ColumnInformation>>, String> {
	let relations = query.get_all_plans()
		.iter()
		.filter_map(|p|
			(p.schema.as_str_ref().unwrap_or("public").to_owned(), p.relation_name.as_ref()?.to_owned()).into()
		)
		.collect::<HashSet<(String, String)>>();
	let all_columns = schema_queries::get_table_columns(
		client,
		&relations.iter().map(|(s, r)| (&s[..], &r[..])).collect::<Vec<_>>()
	)?;
	let mut relation_columns = HashMap::new();
	for (s, t) in relations {
		relation_columns.insert((s, t), HashMap::new());
	}
	for col in all_columns.into_iter() {
		relation_columns.get_mut(&(col.schema.clone(), col.relname.clone())).unwrap()
			.insert(col.name.to_owned(), col);
	}

	Ok(relation_columns)
}


pub fn get_query_info(client: &mut Client, query: &str, out_columns: &[postgres::Column]) -> Result<(), String> {

	// TODO: composite types

	let explain_row = client.query(&format!("EXPLAIN (FORMAT json, VERBOSE true) {}", query), &[])
		.map_err(|e| format!("Failed to get EXPLAIN of the query: {}", e))?;
	if explain_row.len() != 1 {
		return Err(format!("Unexpected number of rows returned from EXPLAIN statement: {}", explain_row.len()));
	}
	let explain_str: String = explain_row[0].get(0);
	let explain: Vec<ExplainJson> = json::from_str(&explain_str)
		.map_err(|e| format!("Cannot deserialize EXPLAIN (FORMAT json): {}", e))?;
	let explain: ExplainJson = explain.try_single().or_else(|explain| {
		if explain.is_empty() {
			Err("PostgreSQL returned empty plan from EXPLAIN (FORMAT json, VERBOSE)".to_owned())
		} else {
			eprintln!("Warning: EXPLAIN returned multiple plans, using the first one");
			Ok(explain.into_1())
		}
	})?;

	let table_schemas = get_queries_columns(client, &explain.plan)?;
	let mut table_schemas2 = HashMap::new();
	for ((k1, k2), v) in table_schemas.iter() {
		table_schemas2.insert((&k1[..], &k2[..]), v.clone());
	}
	let mut aliases: HashMap<&str, &ExplainJsonPlan> = HashMap::new();
	let mut resolved_aliases: HashMap<&str, &HashMap<String, ColumnInformation>> = HashMap::new();
	for plan in explain.plan.get_all_plans() {
		if let Some(alias) = &plan.alias {
			aliases.insert(&alias, plan);

			if let Some(relname) = &plan.relation_name {
				let schema = plan.schema.as_str_ref().unwrap_or("public").to_owned();
				resolved_aliases.insert(&alias, &table_schemas[&(schema, relname.to_owned())]);
			}
		}
	}

	let data = ResolvingData {
		aliases: &aliases,
		resolved_aliases: &resolved_aliases,
		table_schemas: &table_schemas2,
		row_count_expected: explain.plan.plan_rows
	};

	Ok(())
}

