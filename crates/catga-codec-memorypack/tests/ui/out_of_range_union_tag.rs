use catga_codec_memorypack::MemoryPackable;

#[derive(MemoryPackable)]
#[memorypack(union)]
enum OutOfRangeUnionTag {
    #[tag = 256]
    First(u8),
}

fn main() {}
