use std::{fs, path::Path};

use catga_codec_memorypack::{
    DeadLetterMessageRecord, FlowStateRecord, ForEachProgressRecord, InboxMessageRecord,
    MemoryPackLimits, MemoryPackReader, MemoryPackValueCodec, MemoryPackWriter,
    NatsStoredSnapshotRecord, OutboxMessageRecord, StoredSnapshotMetadataRecord,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const FIXTURE_ROOT: &str = "fixtures/memorypack/v1";

#[derive(Deserialize)]
struct FixtureManifest {
    schema_version: u32,
    serializer: SerializerManifest,
    fixtures: Vec<FixtureEntry>,
}

#[derive(Deserialize)]
struct SerializerManifest {
    name: String,
    version: String,
}

#[derive(Deserialize)]
struct FixtureEntry {
    file: String,
    byte_length: usize,
    sha256: String,
}

#[derive(Debug, Eq, PartialEq)]
struct Order {
    id: i64,
    reference: Box<str>,
    description: Box<str>,
}

struct OrderCodec;

impl MemoryPackValueCodec<Order> for OrderCodec {
    fn encode(&self, value: &Order, writer: &mut MemoryPackWriter) -> CatgaResult<()> {
        writer.write_object_header(3)?;
        writer.write_i64(value.id)?;
        writer.write_string(Some(&value.reference))?;
        writer.write_string(Some(&value.description))?;
        writer.finish_object()
    }

    fn decode(&self, reader: &mut MemoryPackReader<'_>) -> CatgaResult<Order> {
        if !reader.read_object_header(3)? {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Order payload must not be null",
            ));
        }
        let value = Order {
            id: reader.read_i64()?,
            reference: reader
                .read_string()?
                .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "Order reference is null"))?,
            description: reader.read_string()?.ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "Order description is null")
            })?,
        };
        reader.finish_object()?;
        Ok(value)
    }
}

#[test]
fn explicit_value_codecs_round_trip_and_reject_invalid_frames_and_budgets() {
    let codec = OrderCodec;
    let order = Order {
        id: 42,
        reference: Box::from("A1"),
        description: Box::from("ok"),
    };
    let bytes = codec
        .encode_value(&order, MemoryPackLimits::default())
        .expect("encode explicit value");

    assert_eq!(
        codec
            .decode_value(&bytes, MemoryPackLimits::default())
            .expect("decode explicit value"),
        order
    );

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        codec
            .decode_value(&trailing, MemoryPackLimits::default())
            .expect_err("trailing bytes are rejected")
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        codec
            .decode_value(&[3], MemoryPackLimits::default())
            .expect_err("truncated object is rejected")
            .code(),
        ErrorCode::Validation
    );

    let allocation_limited = MemoryPackLimits::new(64, 2, 2, 64, 4).expect("valid limits");
    assert_eq!(
        codec
            .decode_value(&bytes, allocation_limited)
            .expect_err("cumulative string allocation is bounded")
            .code(),
        ErrorCode::Validation
    );
}

#[test]
fn immutable_memorypack_fixture_manifest_matches_every_payload() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT);
    let manifest: FixtureManifest = serde_json::from_slice(
        &fs::read(root.join("manifest.json")).expect("read fixture manifest"),
    )
    .expect("parse fixture manifest");

    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.serializer.name, "MemoryPack");
    assert_eq!(manifest.serializer.version, "1.21.3");
    assert_eq!(manifest.fixtures.len(), 27);
    for fixture in manifest.fixtures {
        let bytes = fs::read(root.join(&fixture.file)).expect("read immutable fixture");
        let digest = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(bytes.len(), fixture.byte_length, "{}", fixture.file);
        assert_eq!(digest, fixture.sha256, "{}", fixture.file);
    }
}

#[test]
fn flow_state_fixtures_decode_semantically_and_reencode_exactly() {
    for sample in ["null", "empty", "nonempty", "unicode"] {
        let bytes = fixture(&format!("flow-state-{sample}.bin"));
        let decoded = FlowStateRecord::decode(&bytes, MemoryPackLimits::default())
            .expect("decode flow state fixture");
        assert_eq!(
            FlowStateRecord::encode(decoded.as_ref(), MemoryPackLimits::default())
                .expect("encode flow state fixture"),
            bytes
        );
    }

    let bytes = fixture("flow-state-nonempty.bin");
    let record = FlowStateRecord::decode(&bytes, MemoryPackLimits::default())
        .expect("decode nonempty flow state")
        .expect("non-null flow state");
    assert_eq!(record.id.as_deref(), Some("flow-42"));
    assert_eq!(record.flow_type.as_deref(), Some("Acme.Workflows.Invoice"));
    assert_eq!(record.status, 1);
    assert_eq!(record.current_step, 17);
    assert_eq!(record.version, 99);
    assert_eq!(record.owner.as_deref(), Some("node-a"));
    assert_eq!(record.data.as_deref(), Some(&[0, 127, 255][..]));
    assert_eq!(record.error.as_deref(), Some("retry later"));

    let unicode = FlowStateRecord::decode(
        &fixture("flow-state-unicode.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode unicode flow state")
    .expect("non-null flow state");
    assert_eq!(unicode.id.as_deref(), Some("流程-雪"));
}

#[test]
fn outbox_and_inbox_fixtures_decode_semantically_and_reencode_exactly() {
    for sample in ["null", "empty", "nonempty", "unicode"] {
        let outbox_bytes = fixture(&format!("outbox-message-{sample}.bin"));
        let outbox = OutboxMessageRecord::decode(&outbox_bytes, MemoryPackLimits::default())
            .expect("decode outbox fixture");
        assert_eq!(
            OutboxMessageRecord::encode(outbox.as_ref(), MemoryPackLimits::default())
                .expect("encode outbox fixture"),
            outbox_bytes
        );

        let inbox_bytes = fixture(&format!("inbox-message-{sample}.bin"));
        let inbox = InboxMessageRecord::decode(&inbox_bytes, MemoryPackLimits::default())
            .expect("decode inbox fixture");
        assert_eq!(
            InboxMessageRecord::encode(inbox.as_ref(), MemoryPackLimits::default())
                .expect("encode inbox fixture"),
            inbox_bytes
        );
    }

    let outbox = OutboxMessageRecord::decode(
        &fixture("outbox-message-nonempty.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode outbox")
    .expect("non-null outbox");
    assert_eq!(
        outbox.message_type.as_deref(),
        Some("Acme.Contracts.InvoiceCreated")
    );
    assert_eq!(outbox.payload.as_deref(), Some(&[1, 2, 254][..]));
    assert_eq!(outbox.retry_count, 2);
    assert_eq!(outbox.max_retries, 5);
    assert!(outbox.flag);
    assert_eq!(outbox.correlation_id, 42);

    let inbox = InboxMessageRecord::decode(
        &fixture("inbox-message-nonempty.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode inbox")
    .expect("non-null inbox");
    assert_eq!(inbox.processing_result.as_deref(), Some(&[0x10, 0x20][..]));
    assert_eq!(inbox.mode, 2);
    assert!(inbox.flag);
    assert!(inbox.secondary_flag);
    assert_eq!(inbox.correlation_id, 2718);
}

#[test]
fn dead_letter_and_snapshot_fixtures_decode_and_reencode_exactly() {
    for sample in ["empty", "nonempty", "unicode"] {
        let bytes = fixture(&format!("dead-letter-message-{sample}.bin"));
        let decoded = DeadLetterMessageRecord::decode(&bytes, MemoryPackLimits::default())
            .expect("decode dead-letter fixture");
        assert_eq!(
            DeadLetterMessageRecord::encode(decoded.as_ref(), MemoryPackLimits::default())
                .expect("encode dead-letter fixture"),
            bytes
        );
    }
    for sample in ["null", "empty", "nonempty", "unicode"] {
        let metadata_bytes = fixture(&format!("stored-snapshot-metadata-{sample}.bin"));
        let metadata =
            StoredSnapshotMetadataRecord::decode(&metadata_bytes, MemoryPackLimits::default())
                .expect("decode snapshot metadata fixture");
        assert_eq!(
            StoredSnapshotMetadataRecord::encode(metadata.as_ref(), MemoryPackLimits::default(),)
                .expect("encode snapshot metadata fixture"),
            metadata_bytes
        );

        let nats_bytes = fixture(&format!("nats-stored-snapshot-{sample}.bin"));
        let nats = NatsStoredSnapshotRecord::decode(&nats_bytes, MemoryPackLimits::default())
            .expect("decode NATS snapshot fixture");
        assert_eq!(
            NatsStoredSnapshotRecord::encode(nats.as_ref(), MemoryPackLimits::default())
                .expect("encode NATS snapshot fixture"),
            nats_bytes
        );
    }

    let dead_letter = DeadLetterMessageRecord::decode(
        &fixture("dead-letter-message-nonempty.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode dead letter")
    .expect("non-null dead letter");
    assert_eq!(dead_letter.retry_count, 3);
    assert_eq!(
        dead_letter.exception_type.as_deref(),
        Some("System.TimeoutException")
    );

    let metadata = StoredSnapshotMetadataRecord::decode(
        &fixture("stored-snapshot-metadata-nonempty.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode metadata")
    .expect("non-null metadata");
    assert_eq!(metadata.flow_id.as_deref(), Some("flow-77"));
    assert_eq!(metadata.format, 2);
    assert_eq!(metadata.payload_length, 12);

    let nats = NatsStoredSnapshotRecord::decode(
        &fixture("nats-stored-snapshot-nonempty.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode NATS snapshot")
    .expect("non-null NATS snapshot");
    assert_eq!(nats.key.as_deref(), Some("orders-42"));
    assert_eq!(nats.version, 42);
    assert_eq!(nats.payload.as_deref(), Some(&[1, 2, 3][..]));
}

#[test]
fn foreach_progress_fixtures_decode_semantically_and_reencode_exactly() {
    for sample in ["null", "empty", "nonempty", "unicode"] {
        let bytes = fixture(&format!("foreach-progress-{sample}.bin"));
        let decoded = ForEachProgressRecord::decode(&bytes, MemoryPackLimits::default())
            .expect("decode ForEach fixture");
        assert_eq!(
            ForEachProgressRecord::encode(decoded.as_ref(), MemoryPackLimits::default())
                .expect("encode ForEach fixture"),
            bytes
        );
    }

    let record = ForEachProgressRecord::decode(
        &fixture("foreach-progress-nonempty.bin"),
        MemoryPackLimits::default(),
    )
    .expect("decode ForEach")
    .expect("non-null ForEach");
    assert_eq!(record.current_index, 4);
    assert_eq!(record.total_items, 9);
    assert_eq!(record.completed.as_deref(), Some(&[0, 1, 3][..]));
    assert_eq!(record.failed.as_deref(), Some(&[2][..]));
}

#[test]
fn every_record_rejects_an_incorrect_member_count_and_trailing_input() {
    let limits = MemoryPackLimits::default();
    macro_rules! assert_wrong_header {
        ($record:ty, $fixture:literal, $members:literal) => {{
            let mut bytes = fixture($fixture);
            bytes[0] = $members;
            assert_eq!(
                <$record>::decode(&bytes, limits)
                    .expect_err("wrong member count")
                    .code(),
                ErrorCode::Validation
            );
        }};
    }
    assert_wrong_header!(FlowStateRecord, "flow-state-empty.bin", 8);
    assert_wrong_header!(OutboxMessageRecord, "outbox-message-empty.bin", 12);
    assert_wrong_header!(InboxMessageRecord, "inbox-message-empty.bin", 12);
    assert_wrong_header!(DeadLetterMessageRecord, "dead-letter-message-empty.bin", 7);
    assert_wrong_header!(
        StoredSnapshotMetadataRecord,
        "stored-snapshot-metadata-empty.bin",
        5
    );
    assert_wrong_header!(
        NatsStoredSnapshotRecord,
        "nats-stored-snapshot-empty.bin",
        4
    );
    assert_wrong_header!(ForEachProgressRecord, "foreach-progress-empty.bin", 3);

    let mut trailing = fixture("foreach-progress-empty.bin");
    trailing.push(0);
    assert_eq!(
        ForEachProgressRecord::decode(&trailing, limits)
            .expect_err("trailing input")
            .code(),
        ErrorCode::Validation
    );
}

fn fixture(name: &str) -> Vec<u8> {
    fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(FIXTURE_ROOT)
            .join(name),
    )
    .expect("read fixture")
}
