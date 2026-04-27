use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::differ;
use crate::parser;
use crate::store::Store;
use crate::zygosity;

pub fn import_dir(store: &Arc<Store>, dir: &Path) -> Result<()> {
    if !dir.exists() {
        tracing::warn!(
            "exports dir {} does not exist; skipping import",
            dir.display()
        );
        return Ok(());
    }

    let mut any_imported = false;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("txt") {
            continue;
        }

        let person = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        if store.person_imported(&person)? {
            tracing::info!("{}: already imported, skipping", person);
            continue;
        }

        tracing::info!("{}: parsing {}", person, path.display());
        let snps = parser::parse_file(&path)?;
        tracing::info!("{}: {} snps -> lmdb", person, snps.len());
        store.import_person(&person, &snps)?;
        tracing::info!("{}: import done", person);
        any_imported = true;
    }

    let people = store.list_people()?;

    if any_imported {
        tracing::info!("recomputing zygosity");
        zygosity::recompute_all(store)?;
        if people.len() >= 2 {
            tracing::info!("recomputing diffs");
            differ::recompute_all(store)?;
        }
    } else {
        // backfill if missing
        if !people.is_empty() && store.get_zygosity_stats(&people[0])?.is_none() {
            tracing::info!("zygosity missing — recomputing");
            zygosity::recompute_all(store)?;
        }
        if people.len() >= 2 && store.get_diff_stats(&people[0], &people[1])?.is_none() {
            tracing::info!("diffs missing — recomputing");
            differ::recompute_all(store)?;
        }
    }

    Ok(())
}
