use bitcode::{Decode, Encode};
use serde::Serialize;

// one SNP row from an AncestryDNA.txt file.
// stored value in lmdb db `snps`; key is "{person}\0{rsid}".
#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct Snp {
    pub rsid: String,
    pub chromosome: String, // ancestry uses 1..22, X, Y, MT, 0
    pub position: u32,
    pub allele1: char,
    pub allele2: char,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnpHit {
    pub person: String,
    #[serde(flatten)]
    pub snp: Snp,
}

// ----- diff types -----

#[derive(Debug, Clone, Copy, Encode, Decode, Serialize, PartialEq, Eq)]
pub enum DiffStatus {
    Same,
    Different,
    NoCall,  // either side has a no-call allele (0 or -)
    Missing, // one side lacks this rsid entirely
}

// stored value in lmdb db `diff`; key is "{a}\0{b}\0{chr_padded}\0{pos_be}\0{rsid}".
// pair (a,b) is canonicalized: a < b lexicographically.
#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct DiffRow {
    pub rsid: String,
    pub chromosome: String, // human-readable, e.g. "1", "X", "MT"
    pub position: u32,
    pub a_allele1: char,
    pub a_allele2: char,
    pub b_allele1: char,
    pub b_allele2: char,
    pub status: DiffStatus,
}

// stored value in lmdb db `diff_stats`; key is "{a}\0{b}".
#[derive(Debug, Clone, Default, Encode, Decode, Serialize)]
pub struct DiffStats {
    pub total_compared: u64, // same + different (excludes nocall, missing)
    pub same: u64,
    pub different: u64,
    pub nocall: u64,
    pub missing: u64,
    pub by_chr: Vec<ChrStats>,
}

#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct ChrStats {
    pub chromosome: String,
    pub same: u64,
    pub different: u64,
    pub nocall: u64,
    pub missing: u64,
}
