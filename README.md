# snprs

instant rsid search across multiple AncestryDNA exports. local, lmdb-backed, single binary.

![main screen](screenshots/main_screen_1.webp)

![main screen](screenshots/main_screen_2.webp)

## setup

1. drop your AncestryDNA.txt files into `exports/`, named per person:

       exports/person-x.txt
       exports/person-y.txt

2. `cargo run --release`
3. open http://127.0.0.1:8080

first run imports each file into lmdb (`lmdb_snps/`). subsequent runs skip already-imported people (keyed on filename stem). delete `lmdb_snps/` to force reimport.

## search

paste any rsid prefix — e.g. `rs3131` matches `rs3131972`. tick "exact match only" for strict.

## config

| env             | default          |
| --------------- | ---------------- |
| `SNPRS_BIND`    | `127.0.0.1:8080` |
| `SNPRS_LMDB`    | `lmdb_snps`      |
| `SNPRS_EXPORTS` | `exports`        |
| `RUST_LOG`      | `info`           |