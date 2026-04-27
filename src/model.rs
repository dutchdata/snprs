use bitcode::{Decode, Encode};
use serde::Serialize;

// one SNP row from an AncestryDNA.txt file
// stored value in lmdb_snps; key is "{person}\0{rsid}"
#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct Snp {
    pub rsid: String,
    pub chromosome: String, // keep as string, ancestry uses 1..22, X, Y, MT, 0
    pub position: u32,
    pub allele1: String, // usually 1 char but no-calls use "0" or "-"
    pub allele2: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnpHit {
    pub person: String,
    #[serde(flatten)]
    pub snp: Snp,
}