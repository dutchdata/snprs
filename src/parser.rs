use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::model::Snp;

// parse one AncestryDNA.txt file into snps.
// skips '#' comments and the header row "rsid\tchromosome\tposition\tallele1\tallele2".
pub fn parse_file(path: &Path) -> Result<Vec<Snp>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let r = BufReader::new(f);

    let mut out = Vec::with_capacity(700_000);
    let mut saw_header = false;

    for (lineno, line) in r.lines().enumerate() {
        let line =
            line.with_context(|| format!("read line {} of {}", lineno + 1, path.display()))?;
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !saw_header && line.starts_with("rsid") {
            saw_header = true;
            continue;
        }

        let mut cols = line.split('\t');
        let rsid = match cols.next() {
            Some(s) => s,
            None => continue,
        };
        let chromosome = match cols.next() {
            Some(s) => s,
            None => continue,
        };
        let position = match cols.next() {
            Some(s) => s,
            None => continue,
        };
        let allele1 = match cols.next() {
            Some(s) => s,
            None => continue,
        };
        let allele2 = match cols.next() {
            Some(s) => s,
            None => continue,
        };

        let position: u32 = match position.parse() {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!(
                    "bad position at {}:{}: {}",
                    path.display(),
                    lineno + 1,
                    position
                );
                continue;
            }
        };

        out.push(Snp {
            rsid: rsid.to_string(),
            chromosome: chromosome.to_string(),
            position,
            allele1: parse_allele(allele1, path, lineno + 1)?,
            allele2: parse_allele(allele2, path, lineno + 1)?,
        });
    }

    Ok(out)
}

fn parse_allele(s: &str, path: &Path, lineno: usize) -> Result<char> {
    let mut chars = s.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) => Ok(c),
        _ => anyhow::bail!(
            "expected single-char allele at {}:{}, got {:?}",
            path.display(),
            lineno,
            s
        ),
    }
}
