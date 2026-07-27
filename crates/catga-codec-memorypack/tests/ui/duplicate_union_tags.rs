use catga_codec_memorypack::MemoryPackable;

#[derive(MemoryPackable)]
#[memorypack(union)]
enum DuplicateUnionTags {
    #[tag = 4]
    First(u8),
    #[tag = 4]
    Second(u8),
}

fn main() {}
