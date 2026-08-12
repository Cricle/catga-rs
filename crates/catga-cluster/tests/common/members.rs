use catga_cluster::RaftMember;

pub(crate) fn single_member() -> Vec<RaftMember> {
    vec![RaftMember::new(1, "http://node-1")]
}
