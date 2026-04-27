use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::model::{ChrZygosity, ZygosityStats};
use crate::store::Store;

const NOCALL_CHARS: &[char] = &['0', '-'];

fn is_nocall(c: char) -> bool {
    NOCALL_CHARS.contains(&c)
}

pub fn recompute_all(store: &Arc<Store>) -> Result<()> {
    tracing::info!("zygosity: clearing existing stats");
    store.clear_zygosity()?;

    let people = store.list_people()?;
    for p in people {
        compute_one(store, &p)?;
    }
    Ok(())
}

fn compute_one(store: &Arc<Store>, person: &str) -> Result<()> {
    let mut stats = ZygosityStats {
        person: person.to_string(),
        ..Default::default()
    };
    let mut by_chr: BTreeMap<String, ChrZygosity> = BTreeMap::new();

    store.for_each_snp(person, |s| {
        let entry = by_chr
            .entry(s.chromosome.clone())
            .or_insert_with(|| ChrZygosity {
                chromosome: s.chromosome.clone(),
                homozygous: 0,
                heterozygous: 0,
                nocall: 0,
            });

        if is_nocall(s.allele1) || is_nocall(s.allele2) {
            stats.nocall += 1;
            entry.nocall += 1;
        } else if s.allele1 == s.allele2 {
            stats.homozygous += 1;
            entry.homozygous += 1;
        } else {
            stats.heterozygous += 1;
            entry.heterozygous += 1;
        }
        Ok(())
    })?;

    stats.total = stats.homozygous + stats.heterozygous;
    stats.by_chr = by_chr.into_values().collect();

    tracing::info!(
        "zygosity: {}: hom={} het={} nocall={}",
        person,
        stats.homozygous,
        stats.heterozygous,
        stats.nocall
    );

    store.put_zygosity_stats(person, &stats)?;
    Ok(())
}
