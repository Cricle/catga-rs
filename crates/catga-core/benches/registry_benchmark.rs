//! Memory benchmarks for the Registry - Large Scale

#![feature(test)]

extern crate test;

use catga_core::{CatgaResult, Handler, Message, MessageTypeId, Registry, Request};

// Generate types
macro_rules! gen_types {
    ($(($idx:expr, $name:ident, $tid:ident, $h:ident)),*) => {
        $(
            struct $name;
            impl Message for $name {}
            struct $tid;
            impl MessageTypeId for $tid {
                const NAME: &'static str = concat!("Msg", $idx);
            }
            impl Request for $name {
                type Response = String;
                type TypeId = $tid;
            }
            struct $h;
            #[async_trait::async_trait]
            impl Handler<$name> for $h {
                async fn handle(&self, _: $name) -> CatgaResult<String> { Ok("ok".to_string()) }
            }
        )*
    };
}

gen_types!(
    (0, Msg0, MsgTypeId0, Handler0),
    (1, Msg1, MsgTypeId1, Handler1),
    (2, Msg2, MsgTypeId2, Handler2),
    (3, Msg3, MsgTypeId3, Handler3),
    (4, Msg4, MsgTypeId4, Handler4),
    (5, Msg5, MsgTypeId5, Handler5),
    (6, Msg6, MsgTypeId6, Handler6),
    (7, Msg7, MsgTypeId7, Handler7),
    (8, Msg8, MsgTypeId8, Handler8),
    (9, Msg9, MsgTypeId9, Handler9),
    (10, Msg10, MsgTypeId10, Handler10),
    (11, Msg11, MsgTypeId11, Handler11),
    (12, Msg12, MsgTypeId12, Handler12),
    (13, Msg13, MsgTypeId13, Handler13),
    (14, Msg14, MsgTypeId14, Handler14),
    (15, Msg15, MsgTypeId15, Handler15),
    (16, Msg16, MsgTypeId16, Handler16),
    (17, Msg17, MsgTypeId17, Handler17),
    (18, Msg18, MsgTypeId18, Handler18),
    (19, Msg19, MsgTypeId19, Handler19),
    (20, Msg20, MsgTypeId20, Handler20),
    (21, Msg21, MsgTypeId21, Handler21),
    (22, Msg22, MsgTypeId22, Handler22),
    (23, Msg23, MsgTypeId23, Handler23),
    (24, Msg24, MsgTypeId24, Handler24),
    (25, Msg25, MsgTypeId25, Handler25),
    (26, Msg26, MsgTypeId26, Handler26),
    (27, Msg27, MsgTypeId27, Handler27),
    (28, Msg28, MsgTypeId28, Handler28),
    (29, Msg29, MsgTypeId29, Handler29),
    (30, Msg30, MsgTypeId30, Handler30),
    (31, Msg31, MsgTypeId31, Handler31),
    (32, Msg32, MsgTypeId32, Handler32),
    (33, Msg33, MsgTypeId33, Handler33),
    (34, Msg34, MsgTypeId34, Handler34),
    (35, Msg35, MsgTypeId35, Handler35),
    (36, Msg36, MsgTypeId36, Handler36),
    (37, Msg37, MsgTypeId37, Handler37),
    (38, Msg38, MsgTypeId38, Handler38),
    (39, Msg39, MsgTypeId39, Handler39),
    (40, Msg40, MsgTypeId40, Handler40),
    (41, Msg41, MsgTypeId41, Handler41),
    (42, Msg42, MsgTypeId42, Handler42),
    (43, Msg43, MsgTypeId43, Handler43),
    (44, Msg44, MsgTypeId44, Handler44),
    (45, Msg45, MsgTypeId45, Handler45),
    (46, Msg46, MsgTypeId46, Handler46),
    (47, Msg47, MsgTypeId47, Handler47),
    (48, Msg48, MsgTypeId48, Handler48),
    (49, Msg49, MsgTypeId49, Handler49),
    (50, Msg50, MsgTypeId50, Handler50),
    (51, Msg51, MsgTypeId51, Handler51),
    (52, Msg52, MsgTypeId52, Handler52),
    (53, Msg53, MsgTypeId53, Handler53),
    (54, Msg54, MsgTypeId54, Handler54),
    (55, Msg55, MsgTypeId55, Handler55),
    (56, Msg56, MsgTypeId56, Handler56),
    (57, Msg57, MsgTypeId57, Handler57),
    (58, Msg58, MsgTypeId58, Handler58),
    (59, Msg59, MsgTypeId59, Handler59),
    (60, Msg60, MsgTypeId60, Handler60),
    (61, Msg61, MsgTypeId61, Handler61),
    (62, Msg62, MsgTypeId62, Handler62),
    (63, Msg63, MsgTypeId63, Handler63),
    (64, Msg64, MsgTypeId64, Handler64),
    (65, Msg65, MsgTypeId65, Handler65),
    (66, Msg66, MsgTypeId66, Handler66),
    (67, Msg67, MsgTypeId67, Handler67),
    (68, Msg68, MsgTypeId68, Handler68),
    (69, Msg69, MsgTypeId69, Handler69),
    (70, Msg70, MsgTypeId70, Handler70),
    (71, Msg71, MsgTypeId71, Handler71),
    (72, Msg72, MsgTypeId72, Handler72),
    (73, Msg73, MsgTypeId73, Handler73),
    (74, Msg74, MsgTypeId74, Handler74),
    (75, Msg75, MsgTypeId75, Handler75),
    (76, Msg76, MsgTypeId76, Handler76),
    (77, Msg77, MsgTypeId77, Handler77),
    (78, Msg78, MsgTypeId78, Handler78),
    (79, Msg79, MsgTypeId79, Handler79),
    (80, Msg80, MsgTypeId80, Handler80),
    (81, Msg81, MsgTypeId81, Handler81),
    (82, Msg82, MsgTypeId82, Handler82),
    (83, Msg83, MsgTypeId83, Handler83),
    (84, Msg84, MsgTypeId84, Handler84),
    (85, Msg85, MsgTypeId85, Handler85),
    (86, Msg86, MsgTypeId86, Handler86),
    (87, Msg87, MsgTypeId87, Handler87),
    (88, Msg88, MsgTypeId88, Handler88),
    (89, Msg89, MsgTypeId89, Handler89),
    (90, Msg90, MsgTypeId90, Handler90),
    (91, Msg91, MsgTypeId91, Handler91),
    (92, Msg92, MsgTypeId92, Handler92),
    (93, Msg93, MsgTypeId93, Handler93),
    (94, Msg94, MsgTypeId94, Handler94),
    (95, Msg95, MsgTypeId95, Handler95),
    (96, Msg96, MsgTypeId96, Handler96),
    (97, Msg97, MsgTypeId97, Handler97),
    (98, Msg98, MsgTypeId98, Handler98),
    (99, Msg99, MsgTypeId99, Handler99)
);

// Benchmark: Registry creation (empty)
#[bench]
fn bench_registry_new_empty(b: &mut test::Bencher) {
    b.iter(|| {
        let _registry = Registry::new();
    });
}

// Benchmark: 10 handlers
#[bench]
fn bench_registry_10_handlers(b: &mut test::Bencher) {
    b.iter(|| {
        let mut registry = Registry::new();
        registry.register_request::<Msg0, _>(Handler0).unwrap();
        registry.register_request::<Msg1, _>(Handler1).unwrap();
        registry.register_request::<Msg2, _>(Handler2).unwrap();
        registry.register_request::<Msg3, _>(Handler3).unwrap();
        registry.register_request::<Msg4, _>(Handler4).unwrap();
        registry.register_request::<Msg5, _>(Handler5).unwrap();
        registry.register_request::<Msg6, _>(Handler6).unwrap();
        registry.register_request::<Msg7, _>(Handler7).unwrap();
        registry.register_request::<Msg8, _>(Handler8).unwrap();
        registry.register_request::<Msg9, _>(Handler9).unwrap();
    });
}

// Benchmark: 50 handlers
#[bench]
fn bench_registry_50_handlers(b: &mut test::Bencher) {
    b.iter(|| {
        let mut registry = Registry::new();
        registry.register_request::<Msg0, _>(Handler0).unwrap();
        registry.register_request::<Msg1, _>(Handler1).unwrap();
        registry.register_request::<Msg2, _>(Handler2).unwrap();
        registry.register_request::<Msg3, _>(Handler3).unwrap();
        registry.register_request::<Msg4, _>(Handler4).unwrap();
        registry.register_request::<Msg5, _>(Handler5).unwrap();
        registry.register_request::<Msg6, _>(Handler6).unwrap();
        registry.register_request::<Msg7, _>(Handler7).unwrap();
        registry.register_request::<Msg8, _>(Handler8).unwrap();
        registry.register_request::<Msg9, _>(Handler9).unwrap();
        registry.register_request::<Msg10, _>(Handler10).unwrap();
        registry.register_request::<Msg11, _>(Handler11).unwrap();
        registry.register_request::<Msg12, _>(Handler12).unwrap();
        registry.register_request::<Msg13, _>(Handler13).unwrap();
        registry.register_request::<Msg14, _>(Handler14).unwrap();
        registry.register_request::<Msg15, _>(Handler15).unwrap();
        registry.register_request::<Msg16, _>(Handler16).unwrap();
        registry.register_request::<Msg17, _>(Handler17).unwrap();
        registry.register_request::<Msg18, _>(Handler18).unwrap();
        registry.register_request::<Msg19, _>(Handler19).unwrap();
        registry.register_request::<Msg20, _>(Handler20).unwrap();
        registry.register_request::<Msg21, _>(Handler21).unwrap();
        registry.register_request::<Msg22, _>(Handler22).unwrap();
        registry.register_request::<Msg23, _>(Handler23).unwrap();
        registry.register_request::<Msg24, _>(Handler24).unwrap();
        registry.register_request::<Msg25, _>(Handler25).unwrap();
        registry.register_request::<Msg26, _>(Handler26).unwrap();
        registry.register_request::<Msg27, _>(Handler27).unwrap();
        registry.register_request::<Msg28, _>(Handler28).unwrap();
        registry.register_request::<Msg29, _>(Handler29).unwrap();
        registry.register_request::<Msg30, _>(Handler30).unwrap();
        registry.register_request::<Msg31, _>(Handler31).unwrap();
        registry.register_request::<Msg32, _>(Handler32).unwrap();
        registry.register_request::<Msg33, _>(Handler33).unwrap();
        registry.register_request::<Msg34, _>(Handler34).unwrap();
        registry.register_request::<Msg35, _>(Handler35).unwrap();
        registry.register_request::<Msg36, _>(Handler36).unwrap();
        registry.register_request::<Msg37, _>(Handler37).unwrap();
        registry.register_request::<Msg38, _>(Handler38).unwrap();
        registry.register_request::<Msg39, _>(Handler39).unwrap();
        registry.register_request::<Msg40, _>(Handler40).unwrap();
        registry.register_request::<Msg41, _>(Handler41).unwrap();
        registry.register_request::<Msg42, _>(Handler42).unwrap();
        registry.register_request::<Msg43, _>(Handler43).unwrap();
        registry.register_request::<Msg44, _>(Handler44).unwrap();
        registry.register_request::<Msg45, _>(Handler45).unwrap();
        registry.register_request::<Msg46, _>(Handler46).unwrap();
        registry.register_request::<Msg47, _>(Handler47).unwrap();
        registry.register_request::<Msg48, _>(Handler48).unwrap();
        registry.register_request::<Msg49, _>(Handler49).unwrap();
    });
}

// Benchmark: 100 handlers
#[bench]
fn bench_registry_100_handlers(b: &mut test::Bencher) {
    b.iter(|| {
        let mut registry = Registry::new();
        registry.register_request::<Msg0, _>(Handler0).unwrap();
        registry.register_request::<Msg1, _>(Handler1).unwrap();
        registry.register_request::<Msg2, _>(Handler2).unwrap();
        registry.register_request::<Msg3, _>(Handler3).unwrap();
        registry.register_request::<Msg4, _>(Handler4).unwrap();
        registry.register_request::<Msg5, _>(Handler5).unwrap();
        registry.register_request::<Msg6, _>(Handler6).unwrap();
        registry.register_request::<Msg7, _>(Handler7).unwrap();
        registry.register_request::<Msg8, _>(Handler8).unwrap();
        registry.register_request::<Msg9, _>(Handler9).unwrap();
        registry.register_request::<Msg10, _>(Handler10).unwrap();
        registry.register_request::<Msg11, _>(Handler11).unwrap();
        registry.register_request::<Msg12, _>(Handler12).unwrap();
        registry.register_request::<Msg13, _>(Handler13).unwrap();
        registry.register_request::<Msg14, _>(Handler14).unwrap();
        registry.register_request::<Msg15, _>(Handler15).unwrap();
        registry.register_request::<Msg16, _>(Handler16).unwrap();
        registry.register_request::<Msg17, _>(Handler17).unwrap();
        registry.register_request::<Msg18, _>(Handler18).unwrap();
        registry.register_request::<Msg19, _>(Handler19).unwrap();
        registry.register_request::<Msg20, _>(Handler20).unwrap();
        registry.register_request::<Msg21, _>(Handler21).unwrap();
        registry.register_request::<Msg22, _>(Handler22).unwrap();
        registry.register_request::<Msg23, _>(Handler23).unwrap();
        registry.register_request::<Msg24, _>(Handler24).unwrap();
        registry.register_request::<Msg25, _>(Handler25).unwrap();
        registry.register_request::<Msg26, _>(Handler26).unwrap();
        registry.register_request::<Msg27, _>(Handler27).unwrap();
        registry.register_request::<Msg28, _>(Handler28).unwrap();
        registry.register_request::<Msg29, _>(Handler29).unwrap();
        registry.register_request::<Msg30, _>(Handler30).unwrap();
        registry.register_request::<Msg31, _>(Handler31).unwrap();
        registry.register_request::<Msg32, _>(Handler32).unwrap();
        registry.register_request::<Msg33, _>(Handler33).unwrap();
        registry.register_request::<Msg34, _>(Handler34).unwrap();
        registry.register_request::<Msg35, _>(Handler35).unwrap();
        registry.register_request::<Msg36, _>(Handler36).unwrap();
        registry.register_request::<Msg37, _>(Handler37).unwrap();
        registry.register_request::<Msg38, _>(Handler38).unwrap();
        registry.register_request::<Msg39, _>(Handler39).unwrap();
        registry.register_request::<Msg40, _>(Handler40).unwrap();
        registry.register_request::<Msg41, _>(Handler41).unwrap();
        registry.register_request::<Msg42, _>(Handler42).unwrap();
        registry.register_request::<Msg43, _>(Handler43).unwrap();
        registry.register_request::<Msg44, _>(Handler44).unwrap();
        registry.register_request::<Msg45, _>(Handler45).unwrap();
        registry.register_request::<Msg46, _>(Handler46).unwrap();
        registry.register_request::<Msg47, _>(Handler47).unwrap();
        registry.register_request::<Msg48, _>(Handler48).unwrap();
        registry.register_request::<Msg49, _>(Handler49).unwrap();
        registry.register_request::<Msg50, _>(Handler50).unwrap();
        registry.register_request::<Msg51, _>(Handler51).unwrap();
        registry.register_request::<Msg52, _>(Handler52).unwrap();
        registry.register_request::<Msg53, _>(Handler53).unwrap();
        registry.register_request::<Msg54, _>(Handler54).unwrap();
        registry.register_request::<Msg55, _>(Handler55).unwrap();
        registry.register_request::<Msg56, _>(Handler56).unwrap();
        registry.register_request::<Msg57, _>(Handler57).unwrap();
        registry.register_request::<Msg58, _>(Handler58).unwrap();
        registry.register_request::<Msg59, _>(Handler59).unwrap();
        registry.register_request::<Msg60, _>(Handler60).unwrap();
        registry.register_request::<Msg61, _>(Handler61).unwrap();
        registry.register_request::<Msg62, _>(Handler62).unwrap();
        registry.register_request::<Msg63, _>(Handler63).unwrap();
        registry.register_request::<Msg64, _>(Handler64).unwrap();
        registry.register_request::<Msg65, _>(Handler65).unwrap();
        registry.register_request::<Msg66, _>(Handler66).unwrap();
        registry.register_request::<Msg67, _>(Handler67).unwrap();
        registry.register_request::<Msg68, _>(Handler68).unwrap();
        registry.register_request::<Msg69, _>(Handler69).unwrap();
        registry.register_request::<Msg70, _>(Handler70).unwrap();
        registry.register_request::<Msg71, _>(Handler71).unwrap();
        registry.register_request::<Msg72, _>(Handler72).unwrap();
        registry.register_request::<Msg73, _>(Handler73).unwrap();
        registry.register_request::<Msg74, _>(Handler74).unwrap();
        registry.register_request::<Msg75, _>(Handler75).unwrap();
        registry.register_request::<Msg76, _>(Handler76).unwrap();
        registry.register_request::<Msg77, _>(Handler77).unwrap();
        registry.register_request::<Msg78, _>(Handler78).unwrap();
        registry.register_request::<Msg79, _>(Handler79).unwrap();
        registry.register_request::<Msg80, _>(Handler80).unwrap();
        registry.register_request::<Msg81, _>(Handler81).unwrap();
        registry.register_request::<Msg82, _>(Handler82).unwrap();
        registry.register_request::<Msg83, _>(Handler83).unwrap();
        registry.register_request::<Msg84, _>(Handler84).unwrap();
        registry.register_request::<Msg85, _>(Handler85).unwrap();
        registry.register_request::<Msg86, _>(Handler86).unwrap();
        registry.register_request::<Msg87, _>(Handler87).unwrap();
        registry.register_request::<Msg88, _>(Handler88).unwrap();
        registry.register_request::<Msg89, _>(Handler89).unwrap();
        registry.register_request::<Msg90, _>(Handler90).unwrap();
        registry.register_request::<Msg91, _>(Handler91).unwrap();
        registry.register_request::<Msg92, _>(Handler92).unwrap();
        registry.register_request::<Msg93, _>(Handler93).unwrap();
        registry.register_request::<Msg94, _>(Handler94).unwrap();
        registry.register_request::<Msg95, _>(Handler95).unwrap();
        registry.register_request::<Msg96, _>(Handler96).unwrap();
        registry.register_request::<Msg97, _>(Handler97).unwrap();
        registry.register_request::<Msg98, _>(Handler98).unwrap();
        registry.register_request::<Msg99, _>(Handler99).unwrap();
    });
}

// Benchmark: Registry struct size
#[bench]
fn bench_registry_sizeof(b: &mut test::Bencher) {
    let registry = Registry::new();
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&registry));
    });
}
