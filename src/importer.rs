use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::parser;
use crate::store::Store;

pub fn import_dir(store: &Arc<Store>, dir: &Path) -> Result<()> {
    if !dir.exists() {
        tracing::warn!("exports dir {} does not exist; skipping import", dir.display());
        return Ok(());
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() { continue; }
        if path.extension().and_then(|e| e.to_str()) != Some("txt") { continue; }

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
    }

    Ok(())
}