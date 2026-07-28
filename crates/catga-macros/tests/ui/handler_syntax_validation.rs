use catga_macros::catga_handlers;

catga_handlers! {
    command RebuildIndex => rebuild_index;
    command RebuildIndex => rebuild_index;
}

catga_handlers! {
    service RebuildIndex => rebuild_index;
}

catga_handlers! {
    request RebuildIndex -> rebuild_index;
}

fn main() {}
