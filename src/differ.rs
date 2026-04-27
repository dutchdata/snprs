use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::model::{ChrStats, DiffRow, DiffStats, DiffStatus, Snp};
use crate::store::Store;

const NOCALL_CHARS: &[char] = &['0', '-'];

fn is_nocall(c: char) -> bool {
    NOCALL_CHARS.contains(&c)
}

// canonicalize a genotype as a sorted (a, b) tuple for equality comparison.
fn sorted_geno(a: char, b: char) -> (char, char) {
    if a <= b { (a, b) } else { (b, a) }
}

fn classify(a1: char, a2: char, b1: char, b2: char) -> DiffStatus {
    if is_nocall(a1) || is_nocall(a2) || is_nocall(b1) || is_nocall(b2) {
        return DiffStatus::NoCall;
    }
    if sorted_geno(a1, a2) == sorted_geno(b1, b2) {
        DiffStatus::Same
    } else {
        DiffStatus::Different
    }
}

// recompute every pair from scratch. blows away the diff dbs first.
pub fn recompute_all(store: &Arc<Store>) -> Result<()> {
    let people = store.list_people()?;
    if people.len() < 2 {
        tracing::info!("differ: <2 people, nothing to diff");
        store.clear_diffs()?;
        return Ok(());
    }

    tracing::info!("differ: clearing existing diffs");
    store.clear_diffs()?;

    for i in 0..people.len() {
        for j in (i + 1)..people.len() {
            let a = &people[i];
            let b = &people[j];
            tracing::info!("differ: {} vs {}", a, b);
            diff_pair(store, a, b)?;
        }
    }

    Ok(())
}

fn diff_pair(store: &Arc<Store>, a: &str, b: &str) -> Result<()> {
    // load both into memory keyed by rsid. simple, fast enough at ~677k each.
    let mut a_map: BTreeMap<String, Snp> = BTreeMap::new();
    store.for_each_snp(a, |s| {
        a_map.insert(s.rsid.clone(), s.clone());
        Ok(())
    })?;

    let mut b_map: BTreeMap<String, Snp> = BTreeMap::new();
    store.for_each_snp(b, |s| {
        b_map.insert(s.rsid.clone(), s.clone());
        Ok(())
    })?;

    let mut rows: Vec<DiffRow> = Vec::with_capacity(a_map.len());
    let mut stats = DiffStats::default();
    let mut by_chr: BTreeMap<String, ChrStats> = BTreeMap::new();

    let bump = |by_chr: &mut BTreeMap<String, ChrStats>, chr: &str, status: DiffStatus| {
        let entry = by_chr.entry(chr.to_string()).or_insert_with(|| ChrStats {
            chromosome: chr.to_string(),
            same: 0,
            different: 0,
            nocall: 0,
            missing: 0,
        });
        match status {
            DiffStatus::Same => entry.same += 1,
            DiffStatus::Different => entry.different += 1,
            DiffStatus::NoCall => entry.nocall += 1,
            DiffStatus::Missing => entry.missing += 1,
        }
    };

    // walk a, find b
    for (rsid, sa) in &a_map {
        match b_map.get(rsid) {
            Some(sb) => {
                let status = classify(sa.allele1, sa.allele2, sb.allele1, sb.allele2);
                match status {
                    DiffStatus::Same => stats.same += 1,
                    DiffStatus::Different => stats.different += 1,
                    DiffStatus::NoCall => stats.nocall += 1,
                    DiffStatus::Missing => stats.missing += 1,
                }
                bump(&mut by_chr, &sa.chromosome, status);

                rows.push(DiffRow {
                    rsid: rsid.clone(),
                    chromosome: sa.chromosome.clone(),
                    position: sa.position,
                    a_allele1: sa.allele1,
                    a_allele2: sa.allele2,
                    b_allele1: sb.allele1,
                    b_allele2: sb.allele2,
                    status,
                });
            }
            None => {
                stats.missing += 1;
                bump(&mut by_chr, &sa.chromosome, DiffStatus::Missing);
                rows.push(DiffRow {
                    rsid: rsid.clone(),
                    chromosome: sa.chromosome.clone(),
                    position: sa.position,
                    a_allele1: sa.allele1,
                    a_allele2: sa.allele2,
                    b_allele1: '\0',
                    b_allele2: '\0',
                    status: DiffStatus::Missing,
                });
            }
        }
    }

    // catch rsids in b but not a
    for (rsid, sb) in &b_map {
        if a_map.contains_key(rsid) { continue; }
        stats.missing += 1;
        bump(&mut by_chr, &sb.chromosome, DiffStatus::Missing);
        rows.push(DiffRow {
            rsid: rsid.clone(),
            chromosome: sb.chromosome.clone(),
            position: sb.position,
            a_allele1: '\0',
            a_allele2: '\0',
            b_allele1: sb.allele1,
            b_allele2: sb.allele2,
            status: DiffStatus::Missing,
        });
    }

    stats.total_compared = stats.same + stats.different;
    stats.by_chr = by_chr.into_values().collect();

    tracing::info!(
        "differ: {} vs {}: same={} diff={} nocall={} missing={} -> {} rows",
        a, b, stats.same, stats.different, stats.nocall, stats.missing, rows.len()
    );

    store.put_diff_rows(a, b, &rows)?;
    store.put_diff_stats(a, b, &stats)?;
    Ok(())
}