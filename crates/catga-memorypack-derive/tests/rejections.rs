//! Compile-time rejection coverage for the MemoryPack derive.
//!
//! The derive refuses layouts that would defeat Catga's bounded decoding, and those refusals
//! are compile errors: ordinary test code can never execute them. This suite therefore re-runs
//! `rustc` on minimal snippets against the freshly built derive dylib and asserts the emitted
//! diagnostic, which also executes the proc-macro's rejection branches. The snippets stop at
//! the derive error, so they never need the codec trait imports.

use std::path::{Path, PathBuf};
use std::process::Command;

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

/// The workspace target directory that holds the freshly built derive dylib.
///
/// Under `cargo llvm-cov` the profile file lives inside the instrumented target directory, so
/// its path names that directory; otherwise the plain workspace target directory is used.
fn target_dir() -> PathBuf {
    let workspace_target = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the crate lives two levels below the workspace root")
        .join("target");
    if let Ok(profile) = std::env::var("LLVM_PROFILE_FILE") {
        let profile_dir = Path::new(&profile);
        if let Some(parent) = profile_dir.parent()
            && parent.starts_with(&workspace_target)
        {
            return parent.to_path_buf();
        }
    }
    workspace_target
}

/// Picks the most recently built `catga_memorypack_derive` proc-macro dylib.
fn derive_dylib() -> PathBuf {
    let deps = target_dir().join("debug").join("deps");
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&deps)
        .expect("the deps directory exists once the crate has been built")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            name.starts_with("catga_memorypack_derive-")
                && name.ends_with(std::env::consts::DLL_SUFFIX)
        })
        .collect();
    assert!(
        !candidates.is_empty(),
        "the derive dylib must be built before the tests run"
    );
    candidates.sort_by_key(|path| {
        std::fs::metadata(path)
            .and_then(|metadata| metadata.modified())
            .expect("dylib metadata is readable")
    });
    candidates.pop().expect("candidates is non-empty")
}

/// Compiles `source` against the freshly built derive dylib and reports success plus stderr.
fn compile_with_derive(name: &str, source: &str) -> (bool, String) {
    let dir = target_dir();
    let snippet = dir.join(format!("memorypack_rejections_{name}.rs"));
    let metadata = dir.join(format!("memorypack_rejections_{name}.rmeta"));
    std::fs::write(&snippet, source).expect("the snippet file is writable");
    let output = Command::new("rustc")
        .arg("--edition=2024")
        .arg("--crate-type=lib")
        .arg("--emit=metadata")
        .arg("-o")
        .arg(&metadata)
        .arg("--extern")
        .arg(format!(
            "catga_memorypack_derive={}",
            derive_dylib().display()
        ))
        .arg(&snippet)
        .output()
        .expect("rustc is available on PATH");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// Asserts that `source` fails to compile with a diagnostic containing `message`.
fn expect_rejection(name: &str, source: &str, message: &str) {
    let (success, stderr) = compile_with_derive(name, source);
    assert!(
        !success,
        "snippet `{name}` must not compile; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains(message),
        "snippet `{name}` should fail with `{message}`, got:\n{stderr}"
    );
}

fn derive_snippet(attrs: &str, body: &str) -> String {
    format!(
        "use catga_memorypack_derive::MemoryPackable;\n\n#[derive(MemoryPackable)]\n{attrs}{body}\n"
    )
}

/// A named struct with `fields` serialized `u8` fields.
fn many_field_struct(fields: usize) -> String {
    let mut body = String::from("struct ManyFields {\n");
    for index in 0..fields {
        body.push_str(&format!("    f{index:03}: u8,\n"));
    }
    body.push('}');
    derive_snippet("", &body)
}

/// A `#[memorypack(union)]` enum with `variants` single-`u8` payload variants and no tags.
fn many_variant_union(variants: usize) -> String {
    let mut body = String::from("enum ManyVariants {\n");
    for index in 0..variants {
        body.push_str(&format!("    V{index:03}(u8),\n"));
    }
    body.push('}');
    derive_snippet("#[memorypack(union)]\n", &body)
}

#[test]
fn derive_rejects_more_than_255_serialized_fields() {
    expect_rejection(
        "field_overflow",
        &many_field_struct(256),
        "more than 255 serialized fields",
    );
}

#[test]
fn derive_rejects_circular_layouts() {
    expect_rejection(
        "circular",
        &derive_snippet("#[memorypack(circular)]\n", "struct Node { next: i32 }"),
        "does not support circular or version_tolerant",
    );
}

#[test]
fn derive_rejects_version_tolerant_layouts() {
    expect_rejection(
        "version_tolerant",
        &derive_snippet(
            "#[memorypack(version_tolerant)]\n",
            "struct Account { balance: i64 }",
        ),
        "does not support circular or version_tolerant",
    );
}

#[test]
fn derive_rejects_c_like_enums_without_repr_or_discriminants() {
    expect_rejection(
        "enum_without_repr",
        &derive_snippet("", "enum Status { Open, Closed }"),
        "must have either #[repr(i32)] or explicit discriminants",
    );
}

#[test]
fn derive_rejects_rust_unions() {
    expect_rejection(
        "rust_union",
        &derive_snippet("", "union Value { int: i32, float: f32 }"),
        "cannot be derived for Rust unions",
    );
}

#[test]
fn union_rejects_payload_shapes_other_than_one_unnamed_field() {
    expect_rejection(
        "union_payload_shape",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { Text(String), Pair(i32, i32) }",
        ),
        "Union variants must have exactly one unnamed field",
    );
}

#[test]
fn union_rejects_multiple_tag_attributes() {
    expect_rejection(
        "union_double_tag",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag = 1] #[tag = 2] Text(String) }",
        ),
        "union variants may specify only one tag",
    );
}

#[test]
fn union_rejects_non_assignment_tag_syntax() {
    expect_rejection(
        "union_tag_list_syntax",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag(1)] Text(String) }",
        ),
        "union tags must use #[tag = N]",
    );
}

#[test]
fn union_rejects_non_literal_tag_expressions() {
    expect_rejection(
        "union_tag_path_expr",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag = TEXT_TAG] Text(String) }",
        ),
        "union tags must be integer literals",
    );
}

#[test]
fn union_rejects_string_literal_tags() {
    expect_rejection(
        "union_tag_string_literal",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag = \"1\"] Text(String) }",
        ),
        "union tags must be integer literals",
    );
}

#[test]
fn union_rejects_fractional_tags() {
    expect_rejection(
        "union_tag_fraction",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag = 1.5] Text(String) }",
        ),
        "union tags must be integer literals",
    );
}

#[test]
fn union_rejects_integer_tags_that_do_not_fit_a_u16() {
    expect_rejection(
        "union_tag_huge_integer",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag = 99999999999999999999] Text(String) }",
        ),
        "union tags must be integer literals in 0..=255",
    );
}

#[test]
fn union_rejects_tags_above_255() {
    expect_rejection(
        "union_tag_above_255",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Message { #[tag = 256] Text(String) }",
        ),
        "union tags must be in 0..=255",
    );
}

#[test]
fn union_rejects_declaration_ordinals_above_255() {
    expect_rejection(
        "union_ordinal_overflow",
        &many_variant_union(257),
        "union variants without an explicit tag must have an ordinal in 0..=255",
    );
}

#[test]
fn union_rejects_duplicate_tags() {
    expect_rejection(
        "union_duplicate_tag",
        &derive_snippet(
            "#[memorypack(union)]\n",
            "enum Shape { #[tag = 0] Point(f64), #[tag = 0] Circle(f64) }",
        ),
        "duplicate union tag 0",
    );
}

// A bare `#[memorypack]` is a path attribute, not a list; the attribute parser skips it and
// the derive falls back to the regular struct frame. (A bare `#[repr]` hits the same parser
// fallback, but rustc rejects it as malformed before any round-trip could exist.)
#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack]
struct BareMemorypackAttribute {
    value: u8,
}

#[test]
fn bare_memorypack_attribute_falls_back_to_the_regular_frame() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&BareMemorypackAttribute { value: 6 })?;
    assert_eq!(bytes, [1, 6]);
    let decoded: BareMemorypackAttribute = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, BareMemorypackAttribute { value: 6 });
    Ok(())
}

#[test]
fn name_value_repr_is_skipped_by_the_parser_and_rejected_by_rustc() {
    // The derive's repr parser skips non-list `repr` attributes, then rustc itself rejects the
    // malformed form; the snippet must not compile either way.
    expect_rejection(
        "repr_name_value",
        &derive_snippet("#[repr = \"C\"]\n", "struct Pod { a: u8 }"),
        "malformed `repr` attribute input",
    );
}
