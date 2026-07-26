use super::flow::{flow_key, type_index_key};

#[test]
fn plain_flow_keys_are_hashed_and_type_claim_indexes_are_partitioned() {
    let first = flow_key("catga:test", "payment-42");
    let second = flow_key("catga:test", "payment-42");
    let payment = type_index_key("catga:test", "payment");
    let invoice = type_index_key("catga:test", "invoice");

    assert_eq!(first, second);
    assert_ne!(payment, invoice);
    assert!(first.starts_with("catga:test:flow:"));
    assert!(payment.starts_with("catga:test:flow-type:"));
}
