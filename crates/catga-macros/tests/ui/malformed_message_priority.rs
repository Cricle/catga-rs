use catga_macros::Message;

#[derive(Message)]
#[catga(priority = urgent)]
struct OrderCreated;

fn main() {}
