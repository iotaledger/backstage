use chronicle_cql::murmur3::murmur3::murmur3_cassandra_x64_128;
use criterion::{
    criterion_group,
    criterion_main,
    Criterion,
};
use std::io::Cursor;

fn tx_murmur3_cassandra_x64_128() {
    let tx_vec = vec![
        "99AEBTBOKRUEWEPZMRIGKJC9EBFYNPCHJZVRERXFHEUPTAWGHT9ZMQFWQOSLKOD99PQUABGPFFPV99999",
        "999LXIEYQBVBAAZG9RWGQFROWHYCUKIZFAUGCBHKGGBZGZXCPD9JBQBNYBKCJDFNKWISXJLZEKPIA9999",
        "999AZLGQZVNTLKUZZYEXNUBIYCQZFMBUJRHLVCMYH9NRLPNMYHHDUHYRDXDSVTOSLRQ9PXBRXAAUA9999",
        "99ACDCZTOHNPAYYHIWQBXMPZNYNVKPRIQQOUOQLPYXHXTLAKCOSGJFPPTBECLSHA9NQAJQULNDKUA9999",
        "99BVHMTBMCL9OPQZSJYKCXFRTHDRYBFOGBSPIFSFPKOVWMOMGQIQMCLOJPNVQSOVTPPTIEZXDRJY99999",
        "99AAYMKXYMOXHOLWOGZURWEUBFCRMLTHMQHZZUALNMAXDBXHTSSTGTFNCIVKRDDKQLTLLTENSNSXA9999",
        "99AIGTZONAVK9UEBFBTODRL9BFSEOLBZNGFPGGILNCFATRQFBBQOAK9XVDSDRBQMVLICYDDLMCQMZ9999",
        "999LQGBBZHQIGYWT9JIUEOTHJWZMSFTIQZUQHFVIBXHDHSABUFPEZQXZCMFZS9MSHTGZNDUSZMED99999",
        "999CQJWCVDIPWJQZDOIENPPVGUQAHMENINRNIPAZZAXNAXZJDMTOGVDBLPGGI9UWUYUPANEWFZWUA9999",
        "99BAEMKXQUKTEPKGVVYCHNMAQUZUIJVYYZTKXILXNYBXBJVMURBPOVVFPXCRUVGAG9NCROCFHBEHA9999",
    ];
    for i in 0..tx_vec.len() {
        let _ = murmur3_cassandra_x64_128(&mut Cursor::new(tx_vec[i]), 0);
    }
}

fn address_murmur3_cassandra_x64_128() {
    let addr_vec = vec![
        "BENDER999BENDER999BENDER999BENDER999BENDER999BENDER999BENDER999BENDER999BENDER999QORTGLROX",
        "CU9NODE9B9T9999999999999999999999999999999999999999999999999999999999999999999999IMWWAOSDD",
        "JPYUAV9MBDZG9ZX9BAPBBMYFEVORNBIOOZCYPZDZNRGKQYT9HFEXXXBG9TULULJIOWJWQMXSPLILOJGJGTODXANZF9",
        "ZC9BLRFFCEBTMMOTZYLRRYYDZDIJZSHPJW9ADTLBCF9UURFMCXXXJJNLGEKSPUTYNEGAKRIRSSREULB9YX9PMYJIU9",
        "JGPPQ9VRYGEJBVQS9HZHCVTXVJKPKBMMRFP9HRTTQUKRJRCRLITGSRRYSGEOZYTUEVWMMJKIIYTTJADNAQMDTVZWGX",
        "KSNWNNLGLHIBVFMULCEXDQLXMGGVKLACPPDNKSQYLBPPHISXGCNDZHEGCTQII9PRMRB9TXTBZ9BZTOSGWEPSRG9QDW",
        "9FNJWLMBECSQDKHQAGDHDPXBMZFMQIMAFAUIQTDECJVGKJBKHLEBVU9TWCTPRJGYORFDSYENIQKBVSYKW9NSLGS9UW",
        "9CKHWMLWHAZYTHXXMIEXWYAHLXQIFSMWVGEDRBFWFFCVSXEORUHCNJOO9EQXGK9PZWVLSQMWFYJPPNNGCAYUQRKSOX",
        "LYRICZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA9JGZNBXTY",
        "BTFFJSMAGDA9S9NHWUOTQRYJKUWBYCFWNUITQRLQZI9ACPTGKZIUKHFJDEYWKHBRAFJYEQSU9OKH9YHRDQQHYJNTG9",
    ];

    for i in 0..addr_vec.len() {
        let _ = murmur3_cassandra_x64_128(&mut Cursor::new(addr_vec[i]), 0);
    }
}

fn bench_tx(c: &mut Criterion) {
    c.bench_function("tx_murmur3_cassandra_x64_128", |b| {
        b.iter(|| tx_murmur3_cassandra_x64_128())
    });
}

fn bench_address(c: &mut Criterion) {
    c.bench_function("address_murmur3_cassandra_x64_128", |b| {
        b.iter(|| address_murmur3_cassandra_x64_128())
    });
}

criterion_group!(benches, bench_tx, bench_address);
criterion_main!(benches);