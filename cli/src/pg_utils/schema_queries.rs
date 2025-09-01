use std::{collections::HashMap, result::Result, num::NonZeroU32};

use postgres::{types::ToSql, Client};
use postgres_protocol::Oid;

use crate::pg_utils::type_shenanigans::{ParsedType};

#[derive(Debug, Clone)]
pub struct ColumnInformation {
    pub container_oid: u32,
    pub schema: String,
    pub relname: String,
    pub number: i32,
    pub name: String,
    pub notnull: bool,
    pub type_name: String,
    pub type_oid: u32,
    pub type_mod: i32,
    pub contypes: Vec<String>, // pg_constraint types - c = check constraint, f = foreign key constraint, p = primary key constraint, u = unique constraint, t = constraint trigger, x = exclusion constraint
}

impl ColumnInformation {
    pub fn parse_type(&self) -> ParsedType {
        ParsedType::parse(&self.type_name, Some(self.type_oid), self.type_mod)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OperatorKind { Prefix, Infix, Unknown }

#[derive(Debug, Clone)]
pub struct OperatorInfo {
    pub operator_oid: u32,
    pub left_type_oid: u32,
    // pub left_type_name: String,
    pub right_type_oid: u32,
    // pub right_type_name: String,
    pub operator_symbol: String,
    pub operator_kind: OperatorKind
}

pub fn get_table_columns_p(client: &mut Client, table: &str) -> Result<Vec<ColumnInformation>, String> {
    let split: Vec<&str> = table.split('.').collect();
    let (schema, table) = match &split[..] {
        &[sc, tb] => (sc, tb),
        [_] => ("public", table),
        _ => panic!("Unexpected table name: {}", table)
    };
    get_table_columns(client, &[(schema, table)])
}

pub fn get_table_columns(client: &mut Client, tables: &[(&str, &str)]) -> Result<Vec<ColumnInformation>, String> {
    if tables.is_empty() {
        return Ok(vec![]);
    }

    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    let mut condition = String::new();
    for (i, (schema, table)) in tables.iter().enumerate() {
        if i > 0 {
            condition.push_str(" OR ");
        }
        params.push(schema);
        params.push(table);
        condition.push_str(&format!("(n.nspname = ${} AND c.relname = ${})", params.len() - 1, params.len()));
    }
    let data = client.query(&format!("
    SELECT
        c.oid as container_oid,
        coalesce(n.nspname, 'public') AS schema,
        c.relname AS relname,
        f.attnum AS number,
        f.attname AS name,
        f.attnotnull AS notnull,
        pg_catalog.format_type(f.atttypid,f.atttypmod) AS type_name,
        f.atttypid AS type_oid,
        COALESCE(f.atttypmod, -1) AS type_mod,
        (SELECT array_agg(pp.contype)
            FROM pg_constraint pp
            WHERE pp.conrelid = c.oid AND f.attnum = ANY (pp.conkey))
            AS contypes
    FROM pg_attribute f  
        JOIN pg_class c ON c.oid = f.attrelid
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace  
        LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)  
    WHERE (c.relkind = 'r'::char OR c.relkind = 'v'::char)
        AND {};", condition), &params)
        .map_err(|e| format!("Error getting table {:?} schema: {}", tables, e))?;

    Ok(data.iter().map(|r| {
        ColumnInformation {
            container_oid: r.get(0),
            schema: r.get(1),
            relname: r.get(2),
            number: r.get(3),
            name: r.get(4),
            notnull: r.get(5),
            type_name: r.get(6),
            type_oid: r.get(7),
            type_mod: r.get(8),
            contypes: r.get(9)
        }
    }).collect())
}


pub fn get_composite_type_columns(client: &mut Client, oids: &[u32]) -> Result<Vec<ColumnInformation>, String> {
    if oids.is_empty() {
        return Ok(vec![]);
    }

    let data = client.query(&format!("
    SELECT
        t.oid as container_oid,
        coalesce(n.nspname, 'public') AS schema,
        t.typname AS relname,
        f.attnum AS number,
        f.attname AS name,
        f.attnotnull AS notnull,
        pg_catalog.format_type(f.atttypid,f.atttypmod) AS type_name,
        f.atttypid AS type_oid,
        COALESCE(f.atttypmod, -1) AS type_mod
    FROM pg_type t
    LEFT JOIN pg_attribute f ON f.attrelid = t.typrelid
    LEFT JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE t.oid = ANY($1)
    "), &[&oids])
        .map_err(|e| format!("Error getting composite type schema: {}", e))?;

    Ok(data.iter().map(|r| {
        ColumnInformation {
            container_oid: r.get(0),
            schema: r.get(1),
            relname: r.get(2),
            number: r.get(3),
            name: r.get(4),
            notnull: r.get(5),
            type_name: r.get(6),
            type_oid: r.get(7),
            type_mod: r.get(8),
            contypes: vec![]
        }
    }).collect())
}

pub fn get_operators(client: &mut Client, names: Option<&[&str]>) -> Result<Vec<OperatorInfo>, String> {
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];

    let condition = match names {
        Some(names) => {
            if names.is_empty() {
                return Ok(Vec::new());
            }

            let mut condition = "oprname in (".to_owned();
            for (i, name) in names.iter().enumerate() {
                if i > 0 {
                    condition.push_str(", ");
                }
                params.push(name);
                condition.push_str(&format!("${}", params.len()));
            }
            condition.push_str(")");
            condition
        },
        None => "true".to_owned()
    };

    let data = client.query(&format!("
        SELECT oid, oprname, oprkind, oprleft, oprright, oprresult
            -- , pg_catalog.format_type(oprleft, NULL) AS oprleft_name
            -- , pg_catalog.format_type(oprright, NULL) AS oprright_name
        FROM pg_operator
        WHERE {}
    ", condition), &params);

    Ok(data.map_err(|e| format!("Error getting PostgreSQL operators: {}", e))?.iter().map(|r| {
        let kind = match r.get::<_, i8>(2) as u8 as char {
            'l' | 'L' => OperatorKind::Prefix,
            'b' | 'B' => OperatorKind::Infix,
            _ => OperatorKind::Unknown
        };
        OperatorInfo {
            operator_oid: r.get(0),
            operator_symbol: r.get(1),
            operator_kind: kind,
            left_type_oid: r.get(3),
            right_type_oid: r.get(4),
            // left_type_name: r.get(5),
            // right_type_name: r.get(6),
        }
    }).collect())
}

pub fn get_types(client: &mut Client, oids: Option<&[u32]>, names_to_parse: &[&str]) -> Result<Vec<PgTypeInfo>, String> {
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    let tmp = names_to_parse.iter().map(|x| *x).collect::<Vec<_>>();
    params.push(&tmp);
    let condition = match &oids {
        Some(oids) => {
            if oids.is_empty() {
                return Ok(Vec::new());
            }

            params.push(oids);
            format!("t.oid = ANY(${})", params.len())
        },
        None => "true".to_owned()
    };

    let data = client.query(&format!("
        WITH MATERIALIZED (
            SELECT name, to_regtypemod(name) as type_mod, to_regtype(name)::oid as type_oid
            FROM unnest($1::text[]) x(name)
        ) AS types_to_parse

        SELECT
            oid,
            typname,
            typtype,
            typrelid, -- references pg_class.oid for composite types
            typelem, -- array element type (or zero)
            typnotnull, -- domain is defined not NULL
            typbasetype, -- domain's base type
            typtypmod, -- domain's type modifier
            typndims, -- domain's number of array dimensions
            t.typmodin as has_typemod
        FROM pg_type t
        LEFT JOIN types_to_parse p ON t.oid = p.type_oid
        WHERE ({} OR t.oid in (SELECT type_oid FROM types_to_parse)) AND t.typisdefined
    ", condition), &params)
        .map_err(|e| format!("Error getting PostgreSQL types: {}", e))?;


    Ok(data.iter().map(|r| {
        PgTypeInfo {
            oid: r.get(0),
            name: r.get(1),
            typtype: match (r.get::<_, i8>(2) as u8 as char).to_ascii_lowercase() {
                'b' => PgTypeInfoType::Base,
                'c' => PgTypeInfoType::Composite,
                'd' => PgTypeInfoType::Domain,
                'p' => PgTypeInfoType::Pseudo,
                _ => PgTypeInfoType::Base
            },
            comp_relid: NonZeroU32::new(r.get(3)),
            elem: NonZeroU32::new(r.get(4)),
            dom_notnull: r.get(5),
            dom_basetype: NonZeroU32::new(r.get(6)),
            dom_typmod: r.get(7),
            dom_ndims: r.get(8),
            has_typemod: r.get(9),
            category: match (r.get::<_, i8>(2) as u8 as char).to_ascii_lowercase() {
                'a' => PgTypeInfoCategory::Array,
                'b' => PgTypeInfoCategory::Boolean,
                'c' => PgTypeInfoCategory::Composite,
                'd' => PgTypeInfoCategory::DateTime,
                'e' => PgTypeInfoCategory::Enum,
                'g' => PgTypeInfoCategory::Geometric,
                'i' => PgTypeInfoCategory::NetworkAddr,
                'n' => PgTypeInfoCategory::Numeric,
                'p' => PgTypeInfoCategory::Pseudo,
                'r' => PgTypeInfoCategory::Range,
                's' => PgTypeInfoCategory::String,
                't' => PgTypeInfoCategory::TimeSpan,
                'u' => PgTypeInfoCategory::UsedDefined,
                'v' => PgTypeInfoCategory::BitString,
                _ => PgTypeInfoCategory::Unknown
            }
        }
    }).collect())
    
}


#[derive(Debug, Clone)]
pub struct PgTypeInfo {
    pub oid: Oid,
    pub name: String, // typname
    pub comp_relid: Option<NonZeroU32>, // composite type pg_class.oid
    pub elem: Option<NonZeroU32>, // array (or similar thing like int4vector, lseg, ...) element type
    pub dom_notnull: bool,
    pub dom_basetype: Option<NonZeroU32>, // domain's base type
    pub dom_typmod: i32,
    pub dom_ndims: i32,
    pub has_typemod: bool,
    pub category: PgTypeInfoCategory,
    pub typtype: PgTypeInfoType
}

#[derive(Debug, Clone, Copy)]
pub enum PgTypeInfoCategory {
    Array, Boolean, Composite, DateTime, Enum, Geometric, NetworkAddr, Numeric, Pseudo, Range, String, TimeSpan, UsedDefined, BitString, Unknown, InternalUseOnly
}

#[derive(Debug, Clone, Copy)]
pub enum PgTypeInfoType { Base, Composite, Domain, Pseudo }
