use std::{borrow::Cow, f32::consts::E, fmt::{self, format, Write}, result, str::FromStr};

use bigdecimal::BigDecimal;
use serde::de::value;
use uuid::fmt::Simple;

use crate::rustutils::ArrayDeconstructor;

use super::type_shenanigans::ParsedType;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainJson {
	#[serde(rename = "Plan")]
	pub plan: ExplainJsonPlan
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainJsonPlan {
	/// Sort, Seq Scan, etc.
	#[serde(rename = "Node Type")]
	pub node_type: String,
	/// Expected row count
	#[serde(rename = "Plan Rows")]
	pub plan_rows: u64,
	#[serde(rename = "Plan Width")]
	pub plan_width: u64,
	/// List of output columns (SQL expression strings)
	#[serde(rename = "Output")]
	pub output: Vec<String>,
	#[serde(rename = "Plans")]
	pub plans: Option<Vec<ExplainJsonPlan>>,

	#[serde(rename = "Alias")]
	pub alias: Option<String>,
	#[serde(rename = "Relation Name")]
	pub relation_name: Option<String>,
	#[serde(rename = "Schema")]
	pub schema: Option<String>,

	#[serde(rename = "CTE Name")]
	pub cte_name: Option<String>,

	#[serde(rename = "Subplan Name")]
	pub subplan_name: Option<String>,

	#[serde(rename = "Parent Relationship")]
	pub parent_relationship: Option<String>,

	#[serde(rename = "Filter")]
	pub filter: Option<String>,
	#[serde(rename = "Index Cond")]
	pub index_cond: Option<String>,
}

impl ExplainJsonPlan {
	pub fn get_all_plans<'a>(&'a self) -> Vec<&'a ExplainJsonPlan> {
		let mut plans = vec![self];
		if let Some(subplans) = &self.plans {
			for plan in subplans {
				plans.extend(plan.get_all_plans());
			}
		}
		plans
	}

	pub fn find_plan<'a>(&'a self, mut f: impl FnMut(&ExplainJsonPlan) -> bool) -> Option<&'a ExplainJsonPlan> {
		let mut result = None;
		if f(self) {
			result = Some(self);
		}
		if let Some(subplans) = &self.plans {
			for plan in subplans {
				if let Some(p) = plan.find_plan(|p| f(p)) {
					if result.is_some() {
						debug_assert!(false);
						eprintln!("WARNING: Likely internal bug, multiple matching plans found");
						return None;
					}
					result = Some(p);
				}
			}
		}
		return result
	}

	/// "SubPlan NUMBER", or "CTE cte_name"
	pub fn find_subplan<'a>(&'a self, name: &str, ignore_case: bool) -> Option<&'a ExplainJsonPlan> {
		self.find_plan(|p| p.subplan_name.as_ref().map(|n| ignore_case && n.eq_ignore_ascii_case(name) || n.eq(name)).unwrap_or(false))
	}

	/// For finding $1, $2, ...
	pub fn find_initplan<'a>(&'a self, variable_name: &str) -> Option<&'a ExplainJsonPlan> {
		self.find_plan(|p| p.parent_relationship.as_ref().map(|p| p == "InitPlan").unwrap_or(false) &&
			p.subplan_name.as_ref().map(|n| n.ends_with(&format!("(returns {})", variable_name))).unwrap_or(false)
		)
	}

	fn resolve_alias<'a>(&'a self, alias: &str) -> Option<&'a ExplainJsonPlan> {
		self.find_plan(|p| p.alias.as_ref().map(|a| a == alias).unwrap_or(false))
	}
}
