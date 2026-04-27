use bitcode::{Decode, Encode};
use serde::Serialize;

// one SNP row from an AncestryDNA.txt file.
#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct Snp {
    pub rsid: String,
    pub chromosome: String,
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
    NoCall,
    Missing,
}

#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct DiffRow {
    pub rsid: String,
    pub chromosome: String,
    pub position: u32,
    pub a_allele1: char,
    pub a_allele2: char,
    pub b_allele1: char,
    pub b_allele2: char,
    pub status: DiffStatus,
}

#[derive(Debug, Clone, Default, Encode, Decode, Serialize)]
pub struct DiffStats {
    pub total_compared: u64,
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

// ----- zygosity types -----

#[derive(Debug, Clone, Default, Encode, Decode, Serialize)]
pub struct ZygosityStats {
    pub person: String,
    pub total: u64, // hom + het (excludes nocall)
    pub homozygous: u64,
    pub heterozygous: u64,
    pub nocall: u64,
    pub by_chr: Vec<ChrZygosity>,
}

#[derive(Debug, Clone, Encode, Decode, Serialize)]
pub struct ChrZygosity {
    pub chromosome: String,
    pub homozygous: u64,
    pub heterozygous: u64,
    pub nocall: u64,
}
