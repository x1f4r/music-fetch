//! Generates `extension/src/engine/types.gen.ts`: the core's records and enums
//! as the facade passes them to JS (serde's default representation through
//! serde-wasm-bindgen: snake_case fields, externally tagged enums, `null` for
//! `None`, plain objects for maps, numbers for integers).
//!
//! The shapes are traced from the types' own serde implementations
//! (serde-reflection), so they cannot drift from what crosses the boundary.
//! `cargo test -p mfcore-wasm --test types` fails when the checked-in file is
//! out of date; `MF_WRITE_TYPES=1` rewrites it.

#![cfg(not(target_family = "wasm"))]

use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use mfcore::config::{AcrCredentials, Credentials};
use mfcore::model::*;
use mfcore::pipeline::capture::{PageChapter, PageMedia};
use mfcore_wasm::config::WebEngineConfig;
use serde_reflection::{ContainerFormat, Format, Named, Registry, Samples, Tracer, TracerConfig, VariantFormat};

/// Every type the extension sees, in the order they are written.
fn trace() -> Registry {
    let mut tracer = Tracer::new(TracerConfig::default());
    let samples = Samples::new();
    macro_rules! trace {
        ($($t:ty),* $(,)?) => {
            $( tracer.trace_type::<$t>(&samples).unwrap_or_else(|e| panic!("{}: {e}", stringify!($t))); )*
        };
    }
    trace!(
        Service,
        ServiceLink,
        Track,
        SessionKind,
        SessionStatus,
        SegmentKind,
        Tier,
        EvidenceSource,
        Evidence,
        Segment,
        CandidateTier,
        CandidateSourceKind,
        CandidateSource,
        MelodyAlignment,
        Candidate,
        SourceInfo,
        PitchFrame,
        Phase,
        Notice,
        NoMatchReason,
        Outcome,
        ErrorCode,
        SessionEvent,
        CaptureSource,
        ListenMode,
        ListenOptions,
        SpeechTranscript,
        AnalyzeMode,
        LibraryQuery,
        LibraryEntry,
        SessionDetail,
        ExportFormat,
        MelodyIndexStatus,
        StorageUsage,
        LyricLine,
        Lyrics,
        Credentials,
        AcrCredentials,
        PageChapter,
        PageMedia,
        WebEngineConfig,
    );
    tracer.registry().expect("every type traced")
}

/// Written first, in this order; anything else the tracer found follows.
const ORDER: &[&str] = &[
    "Service",
    "ServiceLink",
    "Track",
    "SessionKind",
    "SessionStatus",
    "SegmentKind",
    "Tier",
    "EvidenceSource",
    "Evidence",
    "Segment",
    "CandidateTier",
    "CandidateSourceKind",
    "CandidateSource",
    "MelodyAlignment",
    "Candidate",
    "SourceInfo",
    "PitchFrame",
    "Phase",
    "Notice",
    "NoMatchReason",
    "Outcome",
    "ErrorCode",
    "SessionEvent",
    "CaptureSource",
    "ListenMode",
    "ListenOptions",
    "SpeechTranscript",
    "AnalyzeMode",
    "LibraryQuery",
    "LibraryEntry",
    "SessionDetail",
    "ExportFormat",
    "MelodyIndexStatus",
    "StorageUsage",
    "LyricLine",
    "Lyrics",
    "Credentials",
    "AcrCredentials",
    "PageChapter",
    "PageMedia",
    "WebEngineConfig",
];

/// Sources whose `#[serde(default)]` attributes make fields optional in TS
/// (serde-reflection cannot see defaults).
const SOURCES: &[&str] =
    &["../mfcore/src/model.rs", "../mfcore/src/config.rs", "../mfcore/src/pipeline/capture.rs", "src/config.rs"];

/// (type, field) pairs that may be left out, and types whose fields all may:
/// read from the `#[serde(default)]` attributes in [`SOURCES`].
fn defaults() -> (BTreeSet<(String, String)>, BTreeSet<String>) {
    let mut fields = BTreeSet::new();
    let mut types = BTreeSet::new();
    for source in SOURCES {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(source);
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let mut owner: Option<String> = None;
        let mut pending_default = false;
        for line in text.lines().map(str::trim) {
            if line.starts_with("#[serde(default") {
                pending_default = true;
                continue;
            }
            if let Some(rest) = line.strip_prefix("pub struct ") {
                let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
                if pending_default {
                    types.insert(name.clone());
                }
                owner = Some(name);
                pending_default = false;
                continue;
            }
            if line.starts_with("pub enum ") || line == "}" {
                owner = None;
                pending_default = false;
                continue;
            }
            if let (Some(owner), Some(rest)) = (&owner, line.strip_prefix("pub "))
                && let Some((field, _)) = rest.split_once(':')
            {
                if pending_default {
                    fields.insert((owner.clone(), field.trim().to_string()));
                }
                pending_default = false;
                continue;
            }
            // Doc comments and other attributes between `#[serde(default)]` and the field keep it.
            if !(line.starts_with("///") || line.starts_with("#[") || line.is_empty()) {
                pending_default = false;
            }
        }
    }
    (fields, types)
}
/// Fields whose TS type is wider than the trace says: JSON transports (the mock
/// engine, the CLI) carry NaN as null.
const WIDER: &[(&str, &str, &str)] = &[("PitchFrame", "midi", "number | null")];

fn ts(format: &Format) -> String {
    match format {
        Format::TypeName(name) => name.clone(),
        Format::Unit => "null".into(),
        Format::Bool => "boolean".into(),
        Format::I8
        | Format::I16
        | Format::I32
        | Format::I64
        | Format::I128
        | Format::U8
        | Format::U16
        | Format::U32
        | Format::U64
        | Format::U128
        | Format::F32
        | Format::F64 => "number".into(),
        Format::Char | Format::Str => "string".into(),
        Format::Bytes => "number[]".into(),
        Format::Option(inner) => format!("{} | null", ts(inner)),
        Format::Seq(inner) => {
            let inner = ts(inner);
            if inner.contains(' ') { format!("({inner})[]") } else { format!("{inner}[]") }
        }
        Format::Map { key, value } => format!("Record<{}, {}>", ts(key), ts(value)),
        Format::Tuple(items) => format!("[{}]", items.iter().map(ts).collect::<Vec<_>>().join(", ")),
        Format::TupleArray { content, size } => format!("[{}]", vec![ts(content); *size].join(", ")),
        Format::Variable(_) => panic!("untraced format"),
    }
}

fn fields(owner: &str, fields: &[Named<Format>]) -> String {
    let (optional_fields, optional_types) = defaults();
    let mut out = String::new();
    for field in fields {
        let optional =
            optional_types.contains(owner) || optional_fields.contains(&(owner.to_string(), field.name.clone()));
        let ty = WIDER
            .iter()
            .find(|(t, f, _)| *t == owner && *f == field.name)
            .map(|(_, _, ty)| ty.to_string())
            .unwrap_or_else(|| ts(&field.value));
        let _ = writeln!(out, "  {}{}: {};", field.name, if optional { "?" } else { "" }, ty);
    }
    out
}

fn inline_fields(owner: &str, list: &[Named<Format>]) -> String {
    let body: Vec<String> = fields(owner, list).lines().map(|l| l.trim().trim_end_matches(';').to_string()).collect();
    if body.is_empty() { "{}".into() } else { format!("{{ {} }}", body.join("; ")) }
}

fn container(name: &str, format: &ContainerFormat) -> String {
    match format {
        ContainerFormat::UnitStruct => format!("export type {name} = null;\n"),
        ContainerFormat::NewTypeStruct(inner) => format!("export type {name} = {};\n", ts(inner)),
        ContainerFormat::TupleStruct(items) => {
            format!("export type {name} = [{}];\n", items.iter().map(ts).collect::<Vec<_>>().join(", "))
        }
        ContainerFormat::Struct(list) => format!("export interface {name} {{\n{}}}\n", fields(name, list)),
        ContainerFormat::Enum(variants) => {
            let arms: Vec<String> = variants
                .values()
                .map(|v| match &v.value {
                    VariantFormat::Unit => format!("'{}'", v.name),
                    VariantFormat::NewType(inner) => format!("{{ {}: {} }}", v.name, ts(inner)),
                    VariantFormat::Tuple(items) => {
                        format!("{{ {}: [{}] }}", v.name, items.iter().map(ts).collect::<Vec<_>>().join(", "))
                    }
                    VariantFormat::Struct(list) => format!("{{ {}: {} }}", v.name, inline_fields(name, list)),
                    VariantFormat::Variable(_) => panic!("untraced variant of {name}"),
                })
                .collect();
            let one_line = format!("export type {name} = {};\n", arms.join(" | "));
            if one_line.len() <= 120 {
                one_line
            } else {
                let mut out = format!("export type {name} =\n");
                for arm in arms {
                    let _ = writeln!(out, "  | {arm}");
                }
                out.pop();
                out.push_str(";\n");
                out
            }
        }
    }
}

fn generate() -> String {
    let registry = trace();
    let mut out = String::from(
        "// Generated by core/crates/mfcore-wasm/tests/types.rs from the engine's serde types. Do not edit:\n\
         // MF_WRITE_TYPES=1 cargo test -p mfcore-wasm --test types\n\
         //\n\
         // Field names are snake_case, enums externally tagged (unit variants are strings, data\n\
         // variants single-key objects), `Option<T>` is `T | null` and integers are numbers.\n",
    );
    let mut written = BTreeSet::new();
    let names = ORDER.iter().map(|s| s.to_string()).chain(registry.keys().cloned()).collect::<Vec<_>>();
    for name in names {
        if !written.insert(name.clone()) {
            continue;
        }
        let format = registry.get(&name).unwrap_or_else(|| panic!("{name} was not traced"));
        out.push('\n');
        out.push_str(&container(&name, format));
    }
    out
}

#[test]
fn typescript_types_are_current() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../extension/src/engine/types.gen.ts");
    let generated = generate();
    if std::env::var_os("MF_WRITE_TYPES").is_some() {
        std::fs::write(&path, &generated).expect("write types.gen.ts");
        return;
    }
    let current = std::fs::read_to_string(&path).unwrap_or_default();
    assert!(
        current == generated,
        "{} is out of date: run MF_WRITE_TYPES=1 cargo test -p mfcore-wasm --test types",
        path.display()
    );
}
