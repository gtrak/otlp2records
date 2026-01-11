//! Schema definitions parsed from VRL @schema annotations.

use once_cell::sync::Lazy;

// Include compiled VRL schemas and schema definitions from build.rs.
include!(concat!(env!("OUT_DIR"), "/compiled_vrl.rs"));

/// A single schema field definition.
#[derive(Clone, Copy, Debug)]
pub struct SchemaField {
    pub name: &'static str,
    pub field_type: &'static str,
    pub required: bool,
}

/// A schema definition parsed from VRL annotations.
#[derive(Clone, Copy, Debug)]
pub struct SchemaDef {
    pub name: &'static str,
    pub fields: &'static [SchemaField],
}

/// Return all schema definitions parsed from VRL.
pub fn schema_defs() -> &'static [SchemaDef] {
    ALL_SCHEMA_DEFS
}

/// Find a schema definition by name.
pub fn schema_def(name: &str) -> Option<&'static SchemaDef> {
    ALL_SCHEMA_DEFS.iter().find(|schema| schema.name == name)
}
