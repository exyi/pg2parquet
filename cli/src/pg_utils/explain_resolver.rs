use std::cmp::{max, min};
use std::collections::HashMap;
use std::process::Output;
use std::ptr::null;
use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive, Zero};
use chrono::format::Parsed;
use pg_bigdecimal::BigInt;
use postgres::types::Type as PgType;
use postgres_protocol::Oid;

use crate::datatypes::numeric as pgnumeric;
use crate::rustutils::ArrayDeconstructor;

use super::explain_parser::{ExplainJsonPlan, ExplainJson};
use super::expr_parser::Expr;
use super::schema_queries::ColumnInformation;
use super::type_shenanigans::ParsedType;


struct OutputTypeInformation {
    /// schema.table.column
    pub original_column: Option<(String, String, String)>,
    pub nullable: bool,
    /// always returns null
    pub only_null: bool,
    pub literal: Option<String>,
    pub out_type: ParsedType,
    pub out_type_oid: Option<u32>,
    pub out_type_mod: i32,
    // pub expected_row_count: Option<usize>
}

impl OutputTypeInformation {
    fn null() -> OutputTypeInformation {
        let mut x = OutputTypeInformation::constant_type(None, -1, ParsedType::Null, None);
        x.only_null = true;
        x
    }
    fn bool(nullable: bool) -> OutputTypeInformation {
        OutputTypeInformation::constant_pgtype(PgType::BOOL, None).with_nullable(nullable)
    }
    fn unk(nullable:bool) -> OutputTypeInformation {
        OutputTypeInformation::constant_pgtype(PgType::UNKNOWN, None).with_nullable(nullable)
    }
    #[inline]
    fn is_unk(&self) -> bool {
        self.out_type_oid == Some(PgType::UNKNOWN.oid())
    }
    #[inline]
    fn is_type(&self, t: PgType) -> bool {
        self.out_type_oid == Some(t.oid())
    }
    fn pgtype(t: PgType) -> OutputTypeInformation {
        OutputTypeInformation::constant_pgtype(t, None)
    }
    fn constant_pgtype(t: PgType, literal: Option<String>) -> OutputTypeInformation {
        let oid = t.oid();
        let name = t.name();
        OutputTypeInformation::constant_type(Some(oid), -1, ParsedType::parse(name, Some(oid), -1), literal)
    }
    fn constant_type(oid: Option<u32>, typemod: i32, name: ParsedType, literal: Option<String>) -> OutputTypeInformation {
        OutputTypeInformation {
            original_column: None,
            nullable: false,
            only_null: false,
            out_type: name,
            out_type_oid: oid,
            out_type_mod: typemod,
            literal
        }
    }
    #[inline]
    fn has_valid_mod(&self) -> bool {
        self.out_type_mod >= 0
    }

    #[must_use]
    fn widen_type(mut self, other: &OutputTypeInformation) -> Self {
        self.nullable = self.nullable || other.nullable;
        self.only_null = self.only_null && other.only_null;
        self.literal = None;
        if (self.is_unk(), self.out_type_oid.unwrap_or(0), self.out_type_mod as u32) < (self.is_unk(), other.out_type_oid.unwrap_or(0), other.out_type_mod as u32) {
            self.out_type = other.out_type.clone();
            self.out_type_oid = other.out_type_oid;
            self.out_type_mod = other.out_type_mod;
        }
        self
    }

    #[must_use]
    pub fn union(mut vs: impl Iterator<Item = OutputTypeInformation>) -> Result<OutputTypeInformation, String> {
        let mut current = vs.next().ok_or_else(|| format!("Cannot union empty list"))?;
        for v in vs {
            current.nullable = current.nullable || v.nullable;
            if current.only_null {
                current = v;
            } else if v.only_null {
            } else {
                // check that types are equal, otherwise error
                if current.out_type_oid.is_some() &&
                    current.out_type_oid.is_some() {
                    let l = current.out_type_oid.unwrap();
                    let r = v.out_type_oid.unwrap();
                    
                    if current.out_type_oid == v.out_type_oid {
                    } else if is_float(l) && is_float(r) {
                        current = current.widen_type(&v)
                    } else if is_int(l) && is_int(r) {
                        current = current.widen_type(&v)
                    } else {
                        return Err(format!("Cannot union types with different OIDs: {}[{}] and {}[{}]", current.out_type, current.out_type_oid.unwrap(), v.out_type, v.out_type_oid.unwrap()));
                    }
                }
                // check that mods are equal, otherwise clear it
                if v.out_type_mod >= 0 && current.out_type_mod >= 0 && current.out_type_mod != v.out_type_mod {
                    current.out_type_mod = -1
                }
            }
        }
        return Ok(current);


        #[inline]
        fn is_float(oid: u32) -> bool {
            oid == PgType::FLOAT4.oid() || oid == PgType::FLOAT8.oid()
        }
        fn is_int(oid: u32) -> bool {
            oid == PgType::INT2.oid() || oid == PgType::INT4.oid() || oid == PgType::INT8.oid()
        }
    }

    fn with_type_pg(mut self, t: PgType, typemod: i32) -> Self {
        self.out_type = ParsedType::parse("?", Some(t.oid()), typemod);
        self.out_type_mod = typemod;
        self.out_type_oid = Some(t.oid());
        self
    }

    fn with_type(mut self, p: ParsedType) -> Self {
        self.out_type_mod = p.typemod();
        self.out_type_oid = p.oid();
        self.out_type = p;
        self
    }

    #[inline]
    fn map_type(mut self, f: impl FnOnce(ParsedType) -> ParsedType) -> Self {
        let t = f(std::mem::replace(&mut self.out_type, ParsedType::Null));
        self.with_type(t)
    }

    fn with_nullable(mut self, nullable: bool) -> Self {
        if self.only_null {
            self
        } else {
            self.nullable = nullable;
            self
        }
    }
    fn with_onlynull(mut self, only_null: bool) -> Self {
        self.only_null = only_null;
        self
    }
    fn or_nullable(self, nullable: bool) -> Self {
        let nullable = self.nullable | nullable;
        self.with_nullable(nullable)
    }
}

impl From<&ColumnInformation> for OutputTypeInformation {
    fn from(value: &ColumnInformation) -> Self {
        OutputTypeInformation {
            original_column: Some((value.schema.clone(), value.relname.clone(), value.name.clone())),
            nullable: !value.notnull,
            only_null: false,
            out_type: value.parse_type(),
            out_type_oid: Some(value.type_oid),
            out_type_mod: value.type_mod,
            literal: None
        }
    }
}

fn resolve_column_type<'a>(
    expr: &Expr,
    aliases: HashMap<&str, &ExplainJsonPlan>,
    resolved_aliases: HashMap<&str, Vec<ColumnInformation>>,
    table_schemas: HashMap<(&str, &str), Vec<ColumnInformation>>) -> Result<OutputTypeInformation, String> {
    todo!()
}

fn transpose_r_v<T, E>(x: Vec<Result<T, E>>) -> Result<Vec<T>, E> {
    x.into_iter().collect::<Result<Vec<_>, _>>()
}

#[derive(Debug, Clone)]
pub struct ResolvingData<'a> {
    pub aliases: &'a HashMap<&'a str, &'a ExplainJsonPlan>,
    pub resolved_aliases: &'a HashMap<&'a str, &'a HashMap<String, ColumnInformation>>,
    pub table_schemas: &'a HashMap<(&'a str, &'a str), HashMap<String, ColumnInformation>>,
    pub row_count_expected: u64
}

fn resolve_expr_type_errmask(expr: &Expr, d: &ResolvingData) -> OutputTypeInformation {
    resolve_expr_type(expr, d).unwrap_or_else(|e| {
        eprintln!("Error resolving expression: {}", e);
        eprintln!("Unsupported expression: {:?}", expr);
        OutputTypeInformation::unk(true)
    })
}

fn resolve_expr_type<'a>(
    expr: &Expr,
    d: &ResolvingData) -> Result<OutputTypeInformation, String> {

    let get_type = |plan: &ExplainJsonPlan, col: &str| -> Result<OutputTypeInformation, String> {
        if plan.relation_name.is_some() {
            let schema = plan.schema.as_ref().map(|s| &s[..]).unwrap_or("public");
            let relname = plan.relation_name.as_ref().unwrap();
            let table =
                d.table_schemas.get(&(schema, &relname[..]))
                    .ok_or_else(|| format!("Could not find table {}.{}", schema, relname))?;
            let column = table.get(col)
                .ok_or_else(|| format!("Could not find column {}.{}.{}", schema, relname, col))?;

            Ok(OutputTypeInformation::from(column))
        // } else if plan.node_type == "Append" {
        // 	let members = plan.plans.unwrap_or(vec![]).iter().filter(|p| p.parent_relationship.is_some_and(|x| x == "Member")).collect::<Vec<_>>();
        // 	let types = members.iter().map(|m| get_type(m, col)).collect::<Result<Vec<_>, _>>()?;

        // 	Err(format!("NIE"))
        } else {
            Err(format!("NIE: {}", plan.node_type))
        }
    };

    fn out_nullable_type(t: PgType, nullable: bool) -> Result<OutputTypeInformation, String> {
        Ok(OutputTypeInformation::pgtype(t).with_nullable(nullable))
    }

    return match expr {
        // TODO: identifiers reference other plans
        Expr::Identifier(None, name, _, _quoted) => {
            let matching_column: Vec<_> = d.table_schemas.values().flat_map(|x| x.get(name).into_iter()).collect();
            if matching_column.len() == 1 {
                Ok(OutputTypeInformation::from(matching_column[0]))
            } else if matching_column.len() == 0 {
                Err(format!("Could not find column {}", name))
            } else {
                Err(format!("Ambiguous column name: {}", name))
            }
        },
        Expr::Identifier(Some(alias), name, _a_quoted, _n_quoted) => {
            let column =
                d.resolved_aliases.get(&alias[..]).and_then(|x| x.get(name));
            let Some(column) = column else {
                return Err(format!("Could not resolve column {}.{} to a specific relation", alias, name));
            };
            Ok(OutputTypeInformation::from(column))
        },
        Expr::Unknown(expr) => Err(format!("Unknown expression: {}", expr)),
        Expr::ConstantNull => Ok(OutputTypeInformation::null()),
        Expr::ConstantBool(bool) => Ok(OutputTypeInformation::constant_pgtype(PgType::BOOL, Some(bool.to_string()))),
        Expr::ConstantNum(n) => Ok(if let Ok(int) = n.parse::<i32>() {
            OutputTypeInformation::constant_pgtype(PgType::INT4, Some(int.to_string()))
        } else if let Ok(bigint) = n.parse::<i64>() {
            OutputTypeInformation::constant_pgtype(PgType::INT8, Some(bigint.to_string()))
        } else {
            let n: BigDecimal = n.parse().map_err(|e| format!("Could not parse number '{}': {}", n, e))?;
            let digits = n.digits();
            let (int, scale) = n.clone().as_bigint_and_exponent();
            OutputTypeInformation::constant_type(
                Some(PgType::NUMERIC.oid()),
                1,
                ParsedType::Numeric(digits as u32, scale as u32, Some((int.clone(), int))),
                Some(n.to_string())
            )
        }),
        Expr::ConstantStr(s) => Ok(OutputTypeInformation::constant_type(Some(PgType::TEXT.oid()), s.len() as i32, ParsedType::Char(s.len() as u32), Some(s.to_owned()))),
        Expr::And(exprs) | Expr::Or(exprs) => {
            let nullable =
                exprs.iter()
                    .any(|e| resolve_expr_type_errmask(e, d).nullable);
            Ok(OutputTypeInformation::bool(nullable))
        },
        Expr::Not(expr) => {
            let inner = resolve_expr_type_errmask(&expr, d);
            Ok(OutputTypeInformation::bool(inner.nullable))
        },
        Expr::TypeConversion(expr, parsed_type) => {
            let inner = resolve_expr_type_errmask(&expr, d);
            Ok(inner.with_type(parsed_type.clone()))
        },
        Expr::UnaryOp(op, expr) => {
            let inner = resolve_expr_type_errmask(&expr, d);
            match &op[..] {
                "-" => Ok(inner.map_type(|t| match t {
                    ParsedType::Numeric(s, p, Some((min, max))) =>
                        ParsedType::Numeric(s, p, Some((-max, -min))),
                    _ => t
                })),
                "@" => match inner.out_type {
                    ParsedType::Numeric(_, _, _) => {
                        let (minv, maxv) = inner.out_type.number_dec_range().unwrap();
                        let minv = if minv <= BigDecimal::zero() && maxv >= BigDecimal::zero() {
                            BigDecimal::zero()
                        } else {
                            min(minv.abs(), maxv.abs())
                        };
                        let maxv = max(minv.abs(), maxv.abs());
                        Ok(inner.with_type(ParsedType::from_dec_range(minv, maxv)))
                    },
                    _ => Ok(inner)
                },
                "|/" | "||/" | "+" | "~" | "!!" => Ok(inner),
                _ => Err(format!("Unknown unary operator: {}", op))
            }
        },
        Expr::OverExpression(x, _) => resolve_expr_type(x, d),
        Expr::SubPlan(_) => {
            panic!("not implemented")
        },
        Expr::BinaryOp(op, expr1, expr2) => {
            let left = resolve_expr_type(&expr1, d)?;
            let right = resolve_expr_type(&expr2, d)?;
            let wider_type = if (left.out_type_oid, left.out_type_mod) > (right.out_type_oid, right.out_type_mod) {
                &left.out_type
            } else {
                &right.out_type
            };
            let o_tpassthrough = Ok(OutputTypeInformation::pgtype(left.out_type.pgtype()).with_nullable(left.nullable || right.nullable).with_onlynull(left.only_null || right.only_null));
            let o_bool = Ok(OutputTypeInformation::pgtype(PgType::BOOL).with_nullable(left.nullable || right.nullable).with_onlynull(left.only_null || right.only_null));

            match (&op.to_lowercase()[..], left.out_type.pgtype(), left.out_type.pgtype(), left.out_type_oid == right.out_type_oid) {
                ("+" | "-" | "*" | "/" | "^", _, _, _) if left.out_type.is_number() && right.out_type.is_number() => {
                    match (&left.out_type, &right.out_type) {
                        (ParsedType::Numeric(lprec, lscale, _), ParsedType::Numeric(rprec, rscale, _)) => {
                            let t = match &op[..] {
                                "+" => {
                                    let (lmin, lmax) = left.out_type.number_dec_range().unwrap();
                                    let (rmin, rmax) = right.out_type.number_dec_range().unwrap();
                                    ParsedType::from_dec_range(lmin + rmin, lmax + rmax)
                                },
                                "-" => {
                                    let (lmin, lmax) = left.out_type.number_dec_range().unwrap();
                                    let (rmin, rmax) = right.out_type.number_dec_range().unwrap();
                                    ParsedType::from_dec_range(lmin - rmax, lmax - rmin)
                                }
                                "*" => {
                                    let scale = (lscale + rscale) as i64;
                                    let (lmin, lmax) = left.out_type.number_dec_range().unwrap();
                                    let (rmin, rmax) = right.out_type.number_dec_range().unwrap();
                                    let vals = vec![&lmin * &rmin, lmin * &rmax, &lmax * rmin, lmax * rmax];
                                    ParsedType::from_dec_range(vals.iter().min().unwrap().with_scale(scale), vals.iter().max().unwrap().with_scale(scale))
                                },
                                "/" => {
                                    let scale = lscale + rprec + 1;
                                    let precision = lprec - lscale + rscale + scale;
                                    let (rmin, rmax) = right.out_type.number_dec_range().unwrap();
                                    if rmin.sign() == rmax.sign() {
                                        // zero not in range, we can gain some nice info from doing to division properly
                                        let (lmin, lmax) = left.out_type.number_dec_range().unwrap();
                                        let vals = vec![&lmin / &rmin, lmin / &rmax, &lmax / rmin, lmax / rmax];
                                        ParsedType::from_dec_range(vals.iter().min().unwrap().with_scale(scale as i64), vals.iter().max().unwrap().with_scale(scale as i64))
                                    } else {
                                        // max / 0.0000...0001 is just going to lead to ridiculous ranges, so let's just count digits from now on
                                        ParsedType::Numeric(precision, scale, None)
                                    }
                                },
                                "%" => {
                                    let scale = max(*lscale, *rscale);
                                    let (_rmin, rmax) = right.out_type.number_dec_range().unwrap();
                                    ParsedType::from_dec_range(-rmax.with_scale(scale as i64), rmax)
                                },
                                "^" => {
                                    ParsedType::Plain(Some(PgType::NUMERIC.oid()), "numeric".to_owned())
                                },
                                _ => unreachable!()
                            };
                            Ok(OutputTypeInformation::constant_type(t.oid(), t.typemod(), t, None).with_nullable(left.nullable || right.nullable))
                        },
                        _ if left.out_type.is_number() && right.out_type.is_number() => {
                            Ok(left.widen_type(&right))
                        }
                        _ => Ok(OutputTypeInformation::unk(left.nullable || right.nullable))
                    }
                },
                // keeps argument types
                ("&" | "|" | "#", _, _, true) if left.out_type.is_number() && right.out_type.is_number() => { // # is XOR
                    Ok(left.or_nullable(right.nullable))
                },
                // keeps left type
                ("<<" | ">>", _, _, _) if left.out_type.is_number() && right.out_type.is_number() => {
                    Ok(left.or_nullable(right.nullable))
                },
                // used as concatenation, assume that type is kept, or it becomes text
                ("||", _, _, true) => Ok(left.or_nullable(right.nullable)),
                ("||", _, _, _) if left.out_type.is_texty() || right.out_type.is_texty() =>
                    out_nullable_type(PgType::TEXT, left.nullable || right.nullable),
                ("&&" | "||" | "<->", PgType::TS_VECTOR, PgType::TS_VECTOR, _) => o_tpassthrough,
                ("@@", _, PgType::TS_VECTOR, _) => o_bool,

                // always return bool:
                ("&&" | "<@" | "@>" | "@@@" | "@@" | "<<" | ">>" | "<<=" | ">>=" | "<<|" | "|>>" | "&<|" | "|&>" | "<^" | ">^" | "?#" | "?-" | "?|" | "?-|" | "?||" | "~=" | "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "like" | "ilike" | "similar" | "similar to" | "?&" | "?" | "@?" | "~" | "~*" | "!~" | "!~*" | "^@", _, _, _) =>
                    o_bool,

                // _ => Ok(OutputTypeInformation::unk(left.nullable || right.nullable)),
                _ => Err(format!("Unknown binary operator: {}", op))
            }
        },
        Expr::FunctionCall(function, args) => {
            let res_args = args.iter().map(|a| resolve_expr_type_errmask(a, d)).collect::<Vec<_>>();
            match (&function.to_lowercase()[..], res_args.len()) {
                // https://www.postgresql.org/docs/current/functions-aggregate.html
                ("coalesce", 1) => Ok(res_args.into_1()),
                ("coalesce" | "greatest" | "least", _) => {
                    // greatest/least: NULL values in the argument list are ignored. The result will be NULL only if all the expressions evaluate to NULL. (This is a deviation from the SQL standard. According to the standard, the return value is NULL if any argument is NULL. Some other databases behave this way.)
                    let nullable = res_args.iter().all(|a| a.nullable);
                    let mut core = OutputTypeInformation::union(res_args.into_iter())?;
                    core.nullable = nullable;
                    Ok(core)
                },
                ("nullif", 2) =>
                    Ok(res_args.into_1().with_nullable(true)),
                ("count", 1) => Ok(OutputTypeInformation::constant_pgtype(PgType::INT8, None)),
                ("min" | "max" | "first" | "every" | "bit_and" | "bit_or" | "bool_and" | "bool_or" | "bit_xor" | "any_value" | "range_agg" | "range_intersect_agg" | "mode", 1) => Ok(res_args.into_1()),
                ("last_value" | "first_value", 1) => Ok(res_args.into_1()),
                ("array_agg", 1) => if let ParsedType::Array(_, _) = res_args[0].out_type {
                    Ok(res_args.into_1())
                } else {
                    Ok(res_args.into_1().map_type(|t| ParsedType::Array(None, Box::new(t))))
                },
                ("row_number", 0) | ("rank", 0) | ("dense_rank", 0) | ("ntile", 1) =>
                    Ok(OutputTypeInformation::constant_pgtype(PgType::INT8, None)),
                ("percent_rank", 0) | ("cume_dist", 0) =>
                    Ok(OutputTypeInformation::constant_pgtype(PgType::FLOAT8, None)),
                ("lag" | "lead", 1 | 2 | 3) => {
                    let mut res_args = res_args.into_iter();
                    let t = res_args.next().unwrap();
                    let _offset = res_args.next();
                    let default = res_args.next();
                    Ok(t.with_nullable(default.map(|d| d.nullable).unwrap_or(true)))
                },
                ("nth_value", 2) => Ok(res_args.into_1().with_nullable(true)),
                ("sum", 1) => {
                    // sum ( smallint ) → bigint
                    // sum ( integer ) → bigint
                    // sum ( bigint ) → numeric
                    // sum ( numeric ) → numeric
                    // sum ( real ) → real
                    // sum ( double precision ) → double precision
                    // sum ( interval ) → interval
                    // sum ( money ) → money
                    let t = res_args.into_1();
                    if t.is_type(PgType::NUMERIC) || t.is_type(PgType::INT8) {
                        let (minv, maxv) = t.out_type.number_dec_range().unwrap();
                        let rows = max(1_000_000, d.row_count_expected * 10) as i64;
                        let (minv, maxv) = (minv * rows, maxv * rows);
                        Ok(t.with_type(ParsedType::from_dec_range(minv, maxv)))
                    } else if t.out_type_oid == Some(PgType::INT8.oid()) {
                        // numeric with larger than int64 scale
                        Ok(t.with_type(ParsedType::Numeric(30, 0, None)))
                    } else if t.out_type_oid == Some(PgType::INT4.oid()) {
                        Ok(t.with_type_pg(PgType::INT8, -1))
                    } else if t.out_type_oid == Some(PgType::INT2.oid()) {
                        Ok(t.with_type_pg(PgType::INT4, -1))
                    } else {
                        Ok(t)
                    }
                },
                ("ceil" | "ceiling", 1) => Ok(res_args.into_1().map_type(|t| t.round(0, RoundingMode::Ceiling))),
                    ("floor", 1) => Ok(res_args.into_1().map_type(|t| t.round(0, RoundingMode::Floor))),
                ("round", 1) => Ok(res_args.into_1().map_type(|t| t.round(0, RoundingMode::HalfUp))),
                (fname @ ("round" | "trunc"), 2) => {
                    let (t, digits) = res_args.into_2();
                    match digits.literal.and_then(|x| x.parse::<i64>().ok()) {
                        Some(round_digits) => {
                            Ok(t.map_type(|t| t.round(round_digits, match fname {
                                "round" => RoundingMode::HalfUp,
                                "trunc" => RoundingMode::Down,
                                _ => unreachable!()
                            })))
                        },
                        _ => Ok(t)
                    }
                },
                _ => Err(format!("Unknown function: {}[{}]", function, res_args.len()))
            }
        },
        Expr::ArrayIndex(_, _) => todo!(),
        Expr::ConstantTyped(_, _) => todo!(),
        Expr::Column(_, _, _) => todo!(),
        // _ => Err(format!("Unknown expression"))
    }
}
