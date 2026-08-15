//! Strict language-neutral schema input and deterministic typed Go generation.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::ffi::OsString;
use std::fmt::{self, Write as _};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use netbadb_schema::{ColumnDef, Schema, SchemaError, TableDef, TypeSpec};
use netbadb_types::{ColumnId, PhysicalType, TableId};
use serde::Deserialize;

/// Version of the language-neutral SDK code-generation input.
pub const SDK_SCHEMA_SPEC_VERSION: u32 = 1;

const HELP: &str = "Usage: netbadb-codegen go --schema <path> --package <name> --output <path> [--table-id <u64>]... [--check]";
const GO_IMPORT: &str = "github.com/sskycn/netbadb/sdk/go";

#[derive(Debug, Deserialize)]
struct SpecVersion {
    version: u32,
}

/// Strict JSON input for SDK generation. It is not Canonical Schema encoding.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSpec {
    version: u32,
    tables: Vec<TableSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSpec {
    id: u64,
    name: String,
    columns: Vec<ColumnSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnSpec {
    id: u32,
    name: String,
    physical_type: PhysicalTypeSpec,
    semantic_type: Option<String>,
    nullable: bool,
    primary_key: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PhysicalTypeSpec {
    Bool,
    Int64,
    Uint64,
    Text,
}

impl From<PhysicalTypeSpec> for PhysicalType {
    fn from(value: PhysicalTypeSpec) -> Self {
        match value {
            PhysicalTypeSpec::Bool => Self::Bool,
            PhysicalTypeSpec::Int64 => Self::Int64,
            PhysicalTypeSpec::Uint64 => Self::UInt64,
            PhysicalTypeSpec::Text => Self::Text,
        }
    }
}

impl SchemaSpec {
    fn into_schema(self) -> Result<Schema, CodegenError> {
        debug_assert_eq!(self.version, SDK_SCHEMA_SPEC_VERSION);
        let tables = self
            .tables
            .into_iter()
            .map(TableSpec::into_table)
            .collect::<Vec<_>>();
        Schema::new(tables).map_err(CodegenError::Schema)
    }
}

impl TableSpec {
    fn into_table(self) -> TableDef {
        let columns = self
            .columns
            .into_iter()
            .map(ColumnSpec::into_column)
            .collect();
        TableDef::new(TableId(self.id), self.name, columns)
    }
}

impl ColumnSpec {
    fn into_column(self) -> ColumnDef {
        let physical = self.physical_type.into();
        let type_spec = self
            .semantic_type
            .map_or(TypeSpec::Physical(physical), |name| TypeSpec::Semantic {
                name,
                physical,
            });
        ColumnDef::new(ColumnId(self.id), self.name, type_spec)
            .nullable(self.nullable)
            .primary_key(self.primary_key)
    }
}

/// Complete deterministic Go-generation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoGenerationRequest {
    pub schema_path: String,
    pub package: String,
    pub output_path: String,
    pub table_ids: Vec<TableId>,
}

/// Parses and validates a Schema Spec using the canonical Rust schema API.
pub fn parse_schema_spec(source: &str) -> Result<Schema, CodegenError> {
    let version: SpecVersion = serde_json::from_str(source).map_err(CodegenError::Json)?;
    if version.version != SDK_SCHEMA_SPEC_VERSION {
        return Err(CodegenError::UnsupportedSchemaSpecVersion(version.version));
    }
    let spec: SchemaSpec = serde_json::from_str(source).map_err(CodegenError::Json)?;
    spec.into_schema()
}

/// Generates one gofmt-compatible source file without accessing the filesystem.
pub fn generate_go(source: &str, request: &GoGenerationRequest) -> Result<String, CodegenError> {
    validate_package(&request.package)?;
    validate_command_path("schema", &request.schema_path)?;
    validate_command_path("output", &request.output_path)?;
    let schema = parse_schema_spec(source)?;
    let tables = select_tables(&schema, &request.table_ids)?;
    let names = GoNames::build(&tables)?;
    render_go(request, &tables, &names)
}

/// Reads, generates, and either writes or byte-checks one output file.
pub fn generate_go_file(request: &GoGenerationRequest, check: bool) -> Result<(), CodegenError> {
    let schema_path = Path::new(&request.schema_path);
    let source = std::fs::read_to_string(schema_path).map_err(|source| CodegenError::Read {
        path: schema_path.to_path_buf(),
        source,
    })?;
    let generated = generate_go(&source, request)?;
    let output_path = Path::new(&request.output_path);
    if check {
        let existing = std::fs::read(output_path).map_err(|source| CodegenError::OutputRead {
            path: output_path.to_path_buf(),
            source,
        })?;
        if existing != generated.as_bytes() {
            return Err(CodegenError::OutputStale(output_path.to_path_buf()));
        }
        return Ok(());
    }
    replace_output(output_path, generated.as_bytes())
}

fn replace_output(output_path: &Path, contents: &[u8]) -> Result<(), CodegenError> {
    let parent = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = output_path
        .file_name()
        .ok_or_else(|| CodegenError::OutputWrite {
            path: output_path.to_path_buf(),
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "output path has no file name",
            ),
        })?;

    let mut temporary_name = OsString::from(".");
    temporary_name.push(file_name);
    temporary_name.push(format!(".netbadb-codegen-{}.tmp", std::process::id()));
    let temporary_path = parent.join(temporary_name);
    let mut temporary = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(|source| CodegenError::OutputWrite {
            path: output_path.to_path_buf(),
            source,
        })?;
    if let Err(source) = temporary
        .write_all(contents)
        .and_then(|()| temporary.sync_all())
    {
        drop(temporary);
        let _ = std::fs::remove_file(&temporary_path);
        return Err(CodegenError::OutputWrite {
            path: output_path.to_path_buf(),
            source,
        });
    }
    drop(temporary);
    if let Err(source) = std::fs::rename(&temporary_path, output_path) {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(CodegenError::OutputWrite {
            path: output_path.to_path_buf(),
            source,
        });
    }
    Ok(())
}

/// Runs the deliberately small single-target command line interface.
pub fn run_cli(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<Option<&'static str>, CodegenError> {
    let mut arguments = arguments.into_iter();
    let Some(target) = arguments.next() else {
        return Err(CodegenError::Cli(HELP.into()));
    };
    if target == "--help" || target == "-h" {
        return Ok(Some(HELP));
    }
    if target == "--version" || target == "-V" {
        return Ok(Some(env!("CARGO_PKG_VERSION")));
    }
    if target != "go" {
        return Err(CodegenError::Cli(format!(
            "unsupported target `{}`; only `go` is available",
            target.to_string_lossy()
        )));
    }

    let mut schema_path = None;
    let mut package = None;
    let mut output_path = None;
    let mut table_ids = Vec::new();
    let mut check = false;
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--schema") => schema_path = Some(next_utf8(&mut arguments, "--schema")?),
            Some("--package") => package = Some(next_utf8(&mut arguments, "--package")?),
            Some("--output") => output_path = Some(next_utf8(&mut arguments, "--output")?),
            Some("--table-id") => {
                let value = next_utf8(&mut arguments, "--table-id")?;
                let id = value.parse::<u64>().map_err(|_| {
                    CodegenError::Cli(format!("--table-id requires a u64, got `{value}`"))
                })?;
                table_ids.push(TableId(id));
            }
            Some("--check") if !check => check = true,
            Some("--check") => return Err(CodegenError::Cli("--check was repeated".into())),
            _ => {
                return Err(CodegenError::Cli(format!(
                    "unknown argument `{}`",
                    argument.to_string_lossy()
                )));
            }
        }
    }
    let request = GoGenerationRequest {
        schema_path: required(schema_path, "--schema")?,
        package: required(package, "--package")?,
        output_path: required(output_path, "--output")?,
        table_ids,
    };
    generate_go_file(&request, check)?;
    Ok(None)
}

fn next_utf8(
    arguments: &mut impl Iterator<Item = OsString>,
    option: &'static str,
) -> Result<String, CodegenError> {
    let value = arguments
        .next()
        .ok_or_else(|| CodegenError::Cli(format!("{option} requires a value")))?;
    value
        .into_string()
        .map_err(|_| CodegenError::Cli(format!("{option} requires UTF-8")))
}

fn required(value: Option<String>, option: &'static str) -> Result<String, CodegenError> {
    value.ok_or_else(|| CodegenError::Cli(format!("{option} is required")))
}

fn select_tables<'a>(
    schema: &'a Schema,
    selected: &[TableId],
) -> Result<Vec<&'a TableDef>, CodegenError> {
    if selected.is_empty() {
        return Ok(schema.tables().iter().collect());
    }
    let mut ids = BTreeSet::new();
    for id in selected {
        if !ids.insert(*id) {
            return Err(CodegenError::DuplicateSelectedTable(*id));
        }
        if !schema.tables().iter().any(|table| table.id == *id) {
            return Err(CodegenError::UnknownSelectedTable(*id));
        }
    }
    Ok(schema
        .tables()
        .iter()
        .filter(|table| ids.contains(&table.id))
        .collect())
}

#[derive(Debug)]
struct TableNames {
    base: String,
    fields: Vec<String>,
}

#[derive(Debug)]
struct GoNames {
    tables: Vec<TableNames>,
    semantic: Vec<(String, PhysicalType)>,
}

impl GoNames {
    fn build(tables: &[&TableDef]) -> Result<Self, CodegenError> {
        let mut declarations = BTreeMap::<String, String>::new();
        for helper in ["Queryer", "RequiredSchemas", "Dial"] {
            reserve(&mut declarations, helper, "generated helper")?;
        }

        let mut semantic_by_source = BTreeMap::<String, (String, PhysicalType)>::new();
        let mut semantic = Vec::new();
        for table in tables {
            for column in &table.columns {
                if let TypeSpec::Semantic { name, physical } = &column.type_spec {
                    let go_name = go_exported_name(name, "semantic type")?;
                    if let Some((_, existing)) = semantic_by_source.get(name) {
                        if existing != physical {
                            return Err(CodegenError::SemanticTypeConflict {
                                name: name.clone(),
                                first: *existing,
                                second: *physical,
                            });
                        }
                    } else {
                        reserve(
                            &mut declarations,
                            &go_name,
                            &format!("semantic type `{name}`"),
                        )?;
                        semantic.push((go_name.clone(), *physical));
                        semantic_by_source.insert(name.clone(), (go_name, *physical));
                    }
                }
            }
        }

        let mut table_names = Vec::with_capacity(tables.len());
        for table in tables {
            let base = go_exported_name(&table.name, "table")?;
            for (name, purpose) in [
                (format!("{base}Row"), "row type"),
                (format!("{base}TableId"), "table ID"),
                (format!("{base}Identity"), "table identity function"),
                (format!("Validate{base}Columns"), "shape validator"),
                (format!("Decode{base}Row"), "row decoder"),
                (format!("{base}Rows"), "typed rows"),
                (format!("Query{base}"), "query function"),
            ] {
                reserve(
                    &mut declarations,
                    &name,
                    &format!("{purpose} for table `{}`", table.name),
                )?;
            }
            let mut fields = Vec::with_capacity(table.columns.len());
            let mut field_names = BTreeMap::new();
            for column in &table.columns {
                let field = go_exported_name(&column.name, "column")?;
                reserve(
                    &mut field_names,
                    &field,
                    &format!("column `{}.{}`", table.name, column.name),
                )?;
                reserve(
                    &mut declarations,
                    &format!("{base}{field}ColumnId"),
                    &format!("column ID for `{}.{}`", table.name, column.name),
                )?;
                fields.push(field);
            }
            table_names.push(TableNames { base, fields });
        }
        Ok(Self {
            tables: table_names,
            semantic,
        })
    }
}

fn reserve(
    declarations: &mut BTreeMap<String, String>,
    name: &str,
    purpose: &str,
) -> Result<(), CodegenError> {
    if let Some(first) = declarations.get(name) {
        return Err(CodegenError::GoNameCollision {
            name: name.into(),
            first: first.clone(),
            second: purpose.into(),
        });
    }
    declarations.insert(name.into(), purpose.into());
    Ok(())
}

fn go_exported_name(value: &str, kind: &'static str) -> Result<String, CodegenError> {
    if !is_go_source_name(value) {
        return Err(CodegenError::UnsupportedGoIdentifier {
            kind,
            name: value.into(),
        });
    }
    let mut result = String::new();
    for word in value.split('_') {
        if word.is_empty() {
            return Err(CodegenError::UnsupportedGoIdentifier {
                kind,
                name: value.into(),
            });
        }
        let mut characters = word.chars();
        if let Some(first) = characters.next() {
            result.push(first.to_ascii_uppercase());
        }
        result.extend(characters);
    }
    Ok(result)
}

fn is_go_source_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn validate_package(package: &str) -> Result<(), CodegenError> {
    let mut bytes = package.bytes();
    let valid = matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid || package == "_" || GO_KEYWORDS.contains(&package) {
        return Err(CodegenError::InvalidPackage(package.into()));
    }
    Ok(())
}

fn validate_command_path(kind: &'static str, path: &str) -> Result<(), CodegenError> {
    if path.is_empty() || path.contains(['\r', '\n']) {
        return Err(CodegenError::InvalidCommandPath {
            kind,
            path: path.into(),
        });
    }
    Ok(())
}

const GO_KEYWORDS: &[&str] = &[
    "break",
    "default",
    "func",
    "interface",
    "select",
    "case",
    "defer",
    "go",
    "map",
    "struct",
    "chan",
    "else",
    "goto",
    "package",
    "switch",
    "const",
    "fallthrough",
    "if",
    "range",
    "type",
    "continue",
    "for",
    "import",
    "return",
    "var",
];

fn render_go(
    request: &GoGenerationRequest,
    tables: &[&TableDef],
    names: &GoNames,
) -> Result<String, CodegenError> {
    let mut output = String::new();
    writeln!(output, "// Code generated by netbadb-codegen; DO NOT EDIT.")?;
    writeln!(output, "// Source: {}", request.schema_path)?;
    write!(
        output,
        "// Regenerate: cargo run -p netbadb-codegen -- go --schema {} --package {} --output {}",
        shell_quote(&request.schema_path),
        request.package,
        shell_quote(&request.output_path)
    )?;
    for id in &request.table_ids {
        write!(output, " --table-id {}", id.0)?;
    }
    writeln!(output, "\n\npackage {}\n", request.package)?;
    writeln!(
        output,
        "import (\n\t\"context\"\n\n\tnetbadb \"{GO_IMPORT}\"\n)\n"
    )?;

    for (name, physical) in &names.semantic {
        writeln!(output, "type {name} {}\n", go_physical_type(*physical))?;
    }
    writeln!(
        output,
        "type Queryer interface {{\n\tQuery(context.Context, string) (*netbadb.Rows, error)\n}}\n"
    )?;

    for (table, table_names) in tables.iter().zip(&names.tables) {
        render_table(&mut output, table, table_names)?;
    }
    render_schema_helpers(&mut output, tables, &names.tables)?;
    Ok(output)
}

fn shell_quote(value: &str) -> String {
    if value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':' | b'+')
    }) {
        return value.into();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn render_table(
    output: &mut String,
    table: &TableDef,
    names: &TableNames,
) -> Result<(), CodegenError> {
    let base = &names.base;
    writeln!(
        output,
        "const {base}TableId netbadb.TableID = {}",
        table.id.0
    )?;
    for (column, field) in table.columns.iter().zip(&names.fields) {
        writeln!(
            output,
            "const {base}{field}ColumnId netbadb.ColumnID = {}",
            column.id.0
        )?;
    }
    writeln!(output)?;
    writeln!(output, "func {base}Identity() netbadb.TableIdentity {{")?;
    write!(
        output,
        "\treturn netbadb.TableIdentity{{TableID: {base}TableId, Fingerprint: netbadb.SchemaFingerprint{{"
    )?;
    let fingerprint = table.fingerprint().map_err(CodegenError::Schema)?;
    for (index, byte) in fingerprint.as_bytes().iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        write!(output, "0x{byte:02x}")?;
    }
    writeln!(output, "}}}}\n}}\n")?;

    writeln!(output, "type {base}Row struct {{")?;
    let longest_field = names.fields.iter().map(String::len).max().unwrap_or(0);
    for (column, field) in table.columns.iter().zip(&names.fields) {
        writeln!(
            output,
            "\t{field}{}{}",
            " ".repeat(longest_field - field.len() + 1),
            go_column_type(column)
        )?;
    }
    writeln!(output, "}}\n")?;

    writeln!(
        output,
        "func Validate{base}Columns(columns []netbadb.ResultColumn) error {{"
    )?;
    writeln!(
        output,
        "\tif len(columns) != {} {{\n\t\treturn &netbadb.ResultShapeError{{Reason: \"{table_name}: expected {} columns\"}}\n\t}}",
        table.columns.len(),
        table.columns.len(),
        table_name = table.name
    )?;
    for (index, column) in table.columns.iter().enumerate() {
        let (named, semantic_name) = match &column.type_spec {
            TypeSpec::Physical(_) => (false, ""),
            TypeSpec::Semantic { name, .. } => (true, name.as_str()),
        };
        writeln!(
            output,
            "\tif columns[{index}].Name != \"{}\" || columns[{index}].Type.Physical != netbadb.{} || columns[{index}].Type.Named != {named} || columns[{index}].Type.Name != \"{semantic_name}\" || columns[{index}].Nullable != {} {{\n\t\treturn &netbadb.ResultShapeError{{Reason: \"{}: column {} does not match the canonical full-row shape\"}}\n\t}}",
            column.name,
            go_physical_constant(column.semantic_type().physical),
            column.nullable,
            table.name,
            index + 1
        )?;
    }
    writeln!(output, "\treturn nil\n}}\n")?;

    writeln!(
        output,
        "func Decode{base}Row(values []netbadb.Value) ({base}Row, error) {{"
    )?;
    writeln!(output, "\tvar row {base}Row")?;
    writeln!(
        output,
        "\tif len(values) != {} {{\n\t\treturn row, &netbadb.ResultShapeError{{Reason: \"{}: expected {} row values\"}}\n\t}}",
        table.columns.len(),
        table.name,
        table.columns.len()
    )?;
    for (index, (column, field)) in table.columns.iter().zip(&names.fields).enumerate() {
        render_decode_column(output, table, column, field, index)?;
    }
    writeln!(output, "\treturn row, nil\n}}\n")?;

    writeln!(
        output,
        "type {base}Rows struct {{\n\trows    *netbadb.Rows\n\tcurrent {base}Row\n\terr     error\n}}\n"
    )?;
    writeln!(output, "func (rows *{base}Rows) Next() bool {{")?;
    writeln!(
        output,
        "\tif rows == nil || rows.err != nil || !rows.rows.Next() {{\n\t\treturn false\n\t}}"
    )?;
    writeln!(output, "\trow, err := Decode{base}Row(rows.rows.Values())")?;
    writeln!(
        output,
        "\tif err != nil {{\n\t\tif closeErr := rows.rows.Close(); closeErr != nil {{\n\t\t\trows.err = closeErr\n\t\t}} else {{\n\t\t\trows.err = err\n\t\t}}\n\t\treturn false\n\t}}"
    )?;
    writeln!(output, "\trows.current = row\n\treturn true\n}}\n")?;
    writeln!(
        output,
        "func (rows *{base}Rows) Row() {base}Row {{\n\tif rows == nil {{\n\t\treturn {base}Row{{}}\n\t}}\n\treturn rows.current\n}}\n"
    )?;
    writeln!(
        output,
        "func (rows *{base}Rows) Err() error {{\n\tif rows == nil {{\n\t\treturn nil\n\t}}\n\tif rows.err != nil {{\n\t\treturn rows.err\n\t}}\n\treturn rows.rows.Err()\n}}\n"
    )?;
    writeln!(
        output,
        "func (rows *{base}Rows) Close() error {{\n\tif rows == nil {{\n\t\treturn nil\n\t}}\n\tif rows.err != nil {{\n\t\treturn rows.err\n\t}}\n\treturn rows.rows.Close()\n}}\n"
    )?;
    writeln!(
        output,
        "func Query{base}(ctx context.Context, queryer Queryer, sql string) (*{base}Rows, error) {{"
    )?;
    writeln!(
        output,
        "\trows, err := queryer.Query(ctx, sql)\n\tif err != nil {{\n\t\treturn nil, err\n\t}}"
    )?;
    writeln!(
        output,
        "\tif err := Validate{base}Columns(rows.Columns()); err != nil {{\n\t\tif closeErr := rows.Close(); closeErr != nil {{\n\t\t\treturn nil, closeErr\n\t\t}}\n\t\treturn nil, err\n\t}}"
    )?;
    writeln!(output, "\treturn &{base}Rows{{rows: rows}}, nil\n}}\n")?;
    Ok(())
}

fn render_decode_column(
    output: &mut String,
    table: &TableDef,
    column: &ColumnDef,
    field: &str,
    index: usize,
) -> Result<(), CodegenError> {
    let physical = column.semantic_type().physical;
    let accessor = go_value_accessor(physical);
    let conversion = match &column.type_spec {
        TypeSpec::Physical(_) => "raw".to_string(),
        TypeSpec::Semantic { name, .. } => {
            format!("{}(raw)", go_exported_name(name, "semantic type")?)
        }
    };
    if column.nullable {
        writeln!(output, "\tif !values[{index}].IsNull() {{")?;
        writeln!(output, "\t\traw, ok := values[{index}].{accessor}()")?;
        writeln!(
            output,
            "\t\tif !ok {{\n\t\t\treturn row, &netbadb.ResultShapeError{{Reason: \"{}.{} has the wrong physical value\"}}\n\t\t}}",
            table.name, column.name
        )?;
        writeln!(output, "\t\trow.{field} = netbadb.Some({conversion})\n\t}}")?;
    } else {
        writeln!(output, "\traw, ok := values[{index}].{accessor}()")?;
        writeln!(
            output,
            "\tif !ok {{\n\t\treturn row, &netbadb.ResultShapeError{{Reason: \"{}.{} is NULL or has the wrong physical value\"}}\n\t}}",
            table.name, column.name
        )?;
        writeln!(output, "\trow.{field} = {conversion}")?;
    }
    Ok(())
}

fn render_schema_helpers(
    output: &mut String,
    tables: &[&TableDef],
    names: &[TableNames],
) -> Result<(), CodegenError> {
    writeln!(output, "func RequiredSchemas() []netbadb.TableIdentity {{")?;
    if tables.is_empty() {
        writeln!(output, "\treturn nil")?;
    } else {
        writeln!(output, "\treturn []netbadb.TableIdentity{{")?;
        for name in names {
            writeln!(output, "\t\t{}Identity(),", name.base)?;
        }
        writeln!(output, "\t}}")?;
    }
    writeln!(output, "}}\n")?;
    writeln!(
        output,
        "func Dial(ctx context.Context, config netbadb.Config) (*netbadb.Client, error) {{"
    )?;
    writeln!(
        output,
        "\trequired := make([]netbadb.TableIdentity, 0, len(config.RequiredSchemas)+{})",
        tables.len()
    )?;
    writeln!(
        output,
        "\trequired = append(required, config.RequiredSchemas...)"
    )?;
    writeln!(
        output,
        "\trequired = append(required, RequiredSchemas()...)"
    )?;
    writeln!(output, "\tconfig.RequiredSchemas = required")?;
    writeln!(output, "\treturn netbadb.Dial(ctx, config)\n}}")?;
    Ok(())
}

fn go_column_type(column: &ColumnDef) -> String {
    let base = match &column.type_spec {
        TypeSpec::Physical(physical) => go_physical_type(*physical).into(),
        TypeSpec::Semantic { name, .. } => {
            go_exported_name(name, "semantic type").unwrap_or_else(|_| name.clone())
        }
    };
    if column.nullable {
        format!("netbadb.Nullable[{base}]")
    } else {
        base
    }
}

const fn go_physical_type(physical: PhysicalType) -> &'static str {
    match physical {
        PhysicalType::Bool => "bool",
        PhysicalType::Int64 => "int64",
        PhysicalType::UInt64 => "uint64",
        PhysicalType::Text => "string",
    }
}

const fn go_physical_constant(physical: PhysicalType) -> &'static str {
    match physical {
        PhysicalType::Bool => "PhysicalTypeBool",
        PhysicalType::Int64 => "PhysicalTypeInt64",
        PhysicalType::UInt64 => "PhysicalTypeUInt64",
        PhysicalType::Text => "PhysicalTypeText",
    }
}

const fn go_value_accessor(physical: PhysicalType) -> &'static str {
    match physical {
        PhysicalType::Bool => "Bool",
        PhysicalType::Int64 => "Int64",
        PhysicalType::UInt64 => "UInt64",
        PhysicalType::Text => "Text",
    }
}

#[derive(Debug)]
pub enum CodegenError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Json(serde_json::Error),
    UnsupportedSchemaSpecVersion(u32),
    Schema(SchemaError),
    InvalidPackage(String),
    InvalidCommandPath {
        kind: &'static str,
        path: String,
    },
    UnsupportedGoIdentifier {
        kind: &'static str,
        name: String,
    },
    GoNameCollision {
        name: String,
        first: String,
        second: String,
    },
    SemanticTypeConflict {
        name: String,
        first: PhysicalType,
        second: PhysicalType,
    },
    UnknownSelectedTable(TableId),
    DuplicateSelectedTable(TableId),
    OutputRead {
        path: PathBuf,
        source: std::io::Error,
    },
    OutputWrite {
        path: PathBuf,
        source: std::io::Error,
    },
    OutputStale(PathBuf),
    Cli(String),
    Format(fmt::Error),
}

impl fmt::Display for CodegenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => write!(
                formatter,
                "failed to read schema spec `{}`: {source}",
                path.display()
            ),
            Self::Json(error) => write!(formatter, "invalid SDK Schema Spec JSON: {error}"),
            Self::UnsupportedSchemaSpecVersion(version) => write!(
                formatter,
                "unsupported SDK Schema Spec version {version}; expected {SDK_SCHEMA_SPEC_VERSION}"
            ),
            Self::Schema(error) => write!(formatter, "invalid canonical schema: {error}"),
            Self::InvalidPackage(package) => {
                write!(formatter, "invalid Go package identifier `{package}`")
            }
            Self::InvalidCommandPath { kind, path } => write!(
                formatter,
                "invalid {kind} path for generated command `{path}`"
            ),
            Self::UnsupportedGoIdentifier { kind, name } => write!(
                formatter,
                "{kind} name `{name}` cannot be represented by the Go v1 naming rules"
            ),
            Self::GoNameCollision {
                name,
                first,
                second,
            } => write!(
                formatter,
                "generated Go name `{name}` collides between {first} and {second}"
            ),
            Self::SemanticTypeConflict {
                name,
                first,
                second,
            } => write!(
                formatter,
                "semantic Go type `{name}` has conflicting physical types {first} and {second}"
            ),
            Self::UnknownSelectedTable(id) => write!(
                formatter,
                "selected table ID {} is not present in the schema spec",
                id.0
            ),
            Self::DuplicateSelectedTable(id) => write!(
                formatter,
                "selected table ID {} was provided more than once",
                id.0
            ),
            Self::OutputRead { path, source } => write!(
                formatter,
                "failed to read generated output `{}`: {source}",
                path.display()
            ),
            Self::OutputWrite { path, source } => write!(
                formatter,
                "failed to write generated output `{}`: {source}",
                path.display()
            ),
            Self::OutputStale(path) => write!(
                formatter,
                "generated output `{}` is stale; regenerate it",
                path.display()
            ),
            Self::Cli(message) => formatter.write_str(message),
            Self::Format(error) => error.fmt(formatter),
        }
    }
}

impl Error for CodegenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. }
            | Self::OutputRead { source, .. }
            | Self::OutputWrite { source, .. } => Some(source),
            Self::Json(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Format(error) => Some(error),
            _ => None,
        }
    }
}

impl From<fmt::Error> for CodegenError {
    fn from(value: fmt::Error) -> Self {
        Self::Format(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPEC: &str = r#"{
      "version": 1,
      "tables": [
        {"id": 1, "name": "users", "columns": [
          {"id": 1, "name": "id", "physical_type": "int64", "semantic_type": "UserId", "nullable": false, "primary_key": true},
          {"id": 2, "name": "name", "physical_type": "text", "semantic_type": null, "nullable": true, "primary_key": false}
        ]},
        {"id": 2, "name": "teams", "columns": [
          {"id": 1, "name": "id", "physical_type": "uint64", "semantic_type": "TeamId", "nullable": false, "primary_key": true}
        ]}
      ]
    }"#;

    fn request() -> GoGenerationRequest {
        GoGenerationRequest {
            schema_path: "schema.json".into(),
            package: "appdb".into(),
            output_path: "generated.go".into(),
            table_ids: Vec::new(),
        }
    }

    #[test]
    fn parses_strict_spec_through_canonical_schema() {
        let schema = parse_schema_spec(SPEC).expect("valid spec");
        assert_eq!(schema.tables().len(), 2);
        assert_eq!(
            schema.tables()[0].columns[0]
                .semantic_type()
                .name
                .as_deref(),
            Some("UserId")
        );
        assert!(schema.tables()[0].columns[1].nullable);

        let unknown = SPEC.replacen("\"version\": 1", "\"version\": 1, \"listen\": \"x\"", 1);
        assert!(matches!(
            parse_schema_spec(&unknown),
            Err(CodegenError::Json(_))
        ));
        let unsupported = SPEC.replacen("\"version\": 1", "\"version\": 2", 1);
        assert!(matches!(
            parse_schema_spec(&unsupported),
            Err(CodegenError::UnsupportedSchemaSpecVersion(2))
        ));
        let missing_identity_field = r#"{"version":1,"tables":[{"id":1,"name":"users","columns":[{"id":1,"name":"id","physical_type":"int64","semantic_type":null,"nullable":false}]}]}"#;
        assert!(matches!(
            parse_schema_spec(missing_identity_field),
            Err(CodegenError::Json(_))
        ));
    }

    #[test]
    fn propagates_canonical_schema_errors_and_rejects_type_aliases() {
        let duplicate = SPEC.replacen(
            "\"id\": 2, \"name\": \"teams\"",
            "\"id\": 1, \"name\": \"teams\"",
            1,
        );
        assert!(matches!(
            parse_schema_spec(&duplicate),
            Err(CodegenError::Schema(SchemaError::DuplicateTableId { .. }))
        ));
        let alias = SPEC.replacen(
            "\"physical_type\": \"int64\"",
            "\"physical_type\": \"i64\"",
            1,
        );
        assert!(matches!(
            parse_schema_spec(&alias),
            Err(CodegenError::Json(_))
        ));
    }

    #[test]
    fn output_is_deterministic_and_contains_typed_bindings() {
        let first = generate_go(SPEC, &request()).expect("generate");
        let second = generate_go(SPEC, &request()).expect("generate again");
        assert_eq!(first, second);
        for expected in [
            "type UserId int64",
            "type TeamId uint64",
            "Name netbadb.Nullable[string]",
            "UsersIdColumnId",
            "func UsersIdentity()",
            "func QueryUsers(",
            "columns[0].Type.Name != \"UserId\"",
        ] {
            assert!(first.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn maps_every_physical_type_without_target_runtime_helpers() {
        let spec = r#"{"version":1,"tables":[{"id":1,"name":"values","columns":[
            {"id":1,"name":"enabled","physical_type":"bool","semantic_type":null,"nullable":false,"primary_key":false},
            {"id":2,"name":"signed","physical_type":"int64","semantic_type":null,"nullable":false,"primary_key":false},
            {"id":3,"name":"unsigned","physical_type":"uint64","semantic_type":null,"nullable":false,"primary_key":false},
            {"id":4,"name":"label","physical_type":"text","semantic_type":null,"nullable":false,"primary_key":false}
        ]}]}"#;
        let output = generate_go(spec, &request()).expect("physical mappings");
        for expected in [
            "Enabled  bool",
            "Signed   int64",
            "Unsigned uint64",
            "Label    string",
        ] {
            assert!(output.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn selection_preserves_schema_order_and_rejects_bad_selection() {
        let mut selected = request();
        selected.table_ids = vec![TableId(2)];
        let output = generate_go(SPEC, &selected).expect("selected output");
        assert!(output.contains("TeamsRow"));
        assert!(!output.contains("UsersRow"));
        assert!(!output.contains("type UserId"));

        selected.table_ids = vec![TableId(9)];
        assert!(matches!(
            generate_go(SPEC, &selected),
            Err(CodegenError::UnknownSelectedTable(TableId(9)))
        ));
        selected.table_ids = vec![TableId(1), TableId(1)];
        assert!(matches!(
            generate_go(SPEC, &selected),
            Err(CodegenError::DuplicateSelectedTable(TableId(1)))
        ));
    }

    #[test]
    fn validates_go_names_packages_and_semantic_physical_consistency() {
        assert_eq!(go_exported_name("user_id", "test").unwrap(), "UserId");
        assert_eq!(go_exported_name("id", "test").unwrap(), "Id");
        assert!(matches!(
            go_exported_name("用户", "test"),
            Err(CodegenError::UnsupportedGoIdentifier { .. })
        ));
        let mut invalid = request();
        invalid.package = "type".into();
        assert!(matches!(
            generate_go(SPEC, &invalid),
            Err(CodegenError::InvalidPackage(_))
        ));

        let collision = SPEC.replacen("\"name\": \"name\"", "\"name\": \"Id\"", 1);
        assert!(matches!(
            generate_go(&collision, &request()),
            Err(CodegenError::GoNameCollision { .. })
        ));
        let conflict = SPEC.replacen(
            "\"semantic_type\": null",
            "\"semantic_type\": \"UserId\"",
            1,
        );
        assert!(matches!(
            generate_go(&conflict, &request()),
            Err(CodegenError::SemanticTypeConflict { .. })
        ));
        let reserved = SPEC.replacen("\"UserId\"", "\"Dial\"", 1);
        assert!(matches!(
            generate_go(&reserved, &request()),
            Err(CodegenError::GoNameCollision { .. })
        ));
    }

    #[test]
    fn fingerprint_and_nullability_changes_affect_generated_output() {
        let baseline = generate_go(SPEC, &request()).expect("baseline");
        let semantic = generate_go(&SPEC.replacen("\"UserId\"", "\"MemberId\"", 1), &request())
            .expect("semantic change");
        assert_ne!(identity_line(&baseline), identity_line(&semantic));
        let nullable_spec = SPEC.replacen("\"nullable\": false", "\"nullable\": true", 1);
        let nullable = generate_go(&nullable_spec, &request()).expect("nullable change");
        assert_ne!(identity_line(&baseline), identity_line(&nullable));
        assert!(nullable.lines().any(|line| {
            line.split_whitespace().collect::<Vec<_>>() == ["Id", "netbadb.Nullable[UserId]"]
        }));
    }

    fn identity_line(output: &str) -> &str {
        output
            .lines()
            .find(|line| line.contains("return netbadb.TableIdentity"))
            .expect("identity line")
    }

    #[test]
    fn file_generation_is_late_and_check_mode_never_writes() {
        let directory =
            std::env::temp_dir().join(format!("netbadb-codegen-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir(&directory).expect("create test directory");
        let schema_path = directory.join("schema.json");
        let output_path = directory.join("generated.go");
        std::fs::write(&schema_path, SPEC).expect("write schema");
        std::fs::write(&output_path, "existing").expect("write existing output");
        let mut file_request = request();
        file_request.schema_path = schema_path.to_string_lossy().into_owned();
        file_request.output_path = output_path.to_string_lossy().into_owned();

        file_request.package = "type".into();
        assert!(matches!(
            generate_go_file(&file_request, false),
            Err(CodegenError::InvalidPackage(_))
        ));
        assert_eq!(
            std::fs::read_to_string(&output_path).expect("read preserved output"),
            "existing"
        );

        file_request.package = "appdb".into();
        assert!(matches!(
            generate_go_file(&file_request, true),
            Err(CodegenError::OutputStale(_))
        ));
        assert_eq!(
            std::fs::read_to_string(&output_path).expect("read stale output"),
            "existing"
        );

        let temporary_path = directory.join(format!(
            ".generated.go.netbadb-codegen-{}.tmp",
            std::process::id()
        ));
        std::fs::write(&temporary_path, "occupied").expect("occupy temporary output");
        assert!(matches!(
            generate_go_file(&file_request, false),
            Err(CodegenError::OutputWrite { .. })
        ));
        assert_eq!(
            std::fs::read_to_string(&output_path).expect("read output after write failure"),
            "existing"
        );
        std::fs::remove_file(temporary_path).expect("remove occupied temporary output");

        generate_go_file(&file_request, false).expect("regenerate output");
        generate_go_file(&file_request, true).expect("fresh output passes check");
        std::fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn regeneration_command_quotes_paths_without_losing_the_source_path() {
        let mut quoted = request();
        quoted.schema_path = "schemas/user's schema.json".into();
        quoted.output_path = "generated files/appdb.go".into();
        let output = generate_go(SPEC, &quoted).expect("quoted regeneration command");
        assert!(output.contains("// Source: schemas/user's schema.json"));
        assert!(output.contains("--schema 'schemas/user'\"'\"'s schema.json'"));
        assert!(output.contains("--output 'generated files/appdb.go'"));
    }
}
