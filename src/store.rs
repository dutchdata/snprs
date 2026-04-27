use anyhow::{Context, Result};
use lmdb::{Cursor, Database, DatabaseFlags, Environment, Transaction, WriteFlags};
use std::path::Path;

use crate::model::{DiffRow, DiffStats, Snp, SnpHit, ZygosityStats};

pub struct Store {
    env: Environment,
    snps: Database,
    rsid_idx: Database,
    meta: Database,
    diff: Database,
    diff_stats: Database,
    zygosity_stats: Database,
}

const ZERO: u8 = 0;

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path).ok();

        let env = Environment::new()
            .set_max_dbs(8)
            .set_map_size(64 * 1024 * 1024 * 1024)
            .open(path)
            .with_context(|| format!("open lmdb at {}", path.display()))?;

        let snps = env.create_db(Some("snps"), DatabaseFlags::empty())?;
        let rsid_idx = env.create_db(Some("rsid_idx"), DatabaseFlags::empty())?;
        let meta = env.create_db(Some("meta"), DatabaseFlags::empty())?;
        let diff = env.create_db(Some("diff"), DatabaseFlags::empty())?;
        let diff_stats = env.create_db(Some("diff_stats"), DatabaseFlags::empty())?;
        let zygosity_stats = env.create_db(Some("zygosity_stats"), DatabaseFlags::empty())?;

        Ok(Self {
            env,
            snps,
            rsid_idx,
            meta,
            diff,
            diff_stats,
            zygosity_stats,
        })
    }

    pub fn person_imported(&self, person: &str) -> Result<bool> {
        let txn = self.env.begin_ro_txn()?;
        let key = format!("person:{}", person);
        match txn.get(self.meta, &key.as_bytes()) {
            Ok(_) => Ok(true),
            Err(lmdb::Error::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_people(&self) -> Result<Vec<String>> {
        let txn = self.env.begin_ro_txn()?;
        let mut out = Vec::new();
        let mut cur = txn.open_ro_cursor(self.meta)?;
        let prefix = b"person:";
        let mut iter = cur.iter_from(prefix);
        while let Some(item) = iter.next() {
            let (k, _) = item;
            if !k.starts_with(prefix) {
                break;
            }
            let name = std::str::from_utf8(&k[prefix.len()..])?;
            out.push(name.to_string());
        }
        out.sort();
        Ok(out)
    }

    pub fn import_person(&self, person: &str, snps: &[Snp]) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;

        for s in snps {
            let mut k = Vec::with_capacity(person.len() + 1 + s.rsid.len());
            k.extend_from_slice(person.as_bytes());
            k.push(ZERO);
            k.extend_from_slice(s.rsid.as_bytes());

            let v = bitcode::encode(s);
            txn.put(self.snps, &k, &v, WriteFlags::empty())?;

            let mut ik = Vec::with_capacity(s.rsid.len() + 1 + person.len());
            ik.extend_from_slice(s.rsid.as_bytes());
            ik.push(ZERO);
            ik.extend_from_slice(person.as_bytes());
            txn.put(self.rsid_idx, &ik, b"", WriteFlags::empty())?;
        }

        let mkey = format!("person:{}", person);
        txn.put(
            self.meta,
            &mkey.as_bytes(),
            b"imported",
            WriteFlags::empty(),
        )?;
        txn.commit()?;
        Ok(())
    }

    pub fn for_each_snp<F: FnMut(&Snp) -> Result<()>>(&self, person: &str, mut f: F) -> Result<()> {
        let txn = self.env.begin_ro_txn()?;
        let mut prefix = Vec::with_capacity(person.len() + 1);
        prefix.extend_from_slice(person.as_bytes());
        prefix.push(ZERO);

        let mut cur = txn.open_ro_cursor(self.snps)?;
        let mut iter = cur.iter_from(&prefix);
        while let Some(item) = iter.next() {
            let (k, v) = item;
            if !k.starts_with(&prefix) {
                break;
            }
            let snp: Snp = bitcode::decode(v)?;
            f(&snp)?;
        }
        Ok(())
    }

    pub fn prefix_search(&self, prefix: &str, exact: bool, limit: usize) -> Result<Vec<SnpHit>> {
        let txn = self.env.begin_ro_txn()?;
        let mut hits = Vec::new();

        {
            let mut cur = txn.open_ro_cursor(self.rsid_idx)?;
            let start = prefix.as_bytes();
            let mut iter = cur.iter_from(start);
            while let Some(item) = iter.next() {
                let (k, _) = item;
                let zero_pos = match k.iter().position(|b| *b == ZERO) {
                    Some(p) => p,
                    None => continue,
                };
                let rsid = match std::str::from_utf8(&k[..zero_pos]) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let person = match std::str::from_utf8(&k[zero_pos + 1..]) {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                if exact {
                    if rsid != prefix {
                        break;
                    }
                } else {
                    if !rsid.starts_with(prefix) {
                        break;
                    }
                }

                let mut sk = Vec::with_capacity(person.len() + 1 + rsid.len());
                sk.extend_from_slice(person.as_bytes());
                sk.push(ZERO);
                sk.extend_from_slice(rsid.as_bytes());

                if let Ok(val) = txn.get(self.snps, &sk) {
                    let snp: Snp = bitcode::decode(val)?;
                    hits.push(SnpHit {
                        person: person.to_string(),
                        snp,
                    });
                    if hits.len() >= limit {
                        break;
                    }
                }
            }
        }

        Ok(hits)
    }

    // ----- diff -----

    pub fn clear_diffs(&self) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;
        txn.clear_db(self.diff)?;
        txn.clear_db(self.diff_stats)?;
        txn.commit()?;
        Ok(())
    }

    pub fn put_diff_rows(&self, a: &str, b: &str, rows: &[DiffRow]) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;
        for r in rows {
            let key = diff_key(a, b, &r.chromosome, r.position, &r.rsid);
            let val = bitcode::encode(r);
            txn.put(self.diff, &key, &val, WriteFlags::empty())?;
        }
        txn.commit()?;
        Ok(())
    }

    pub fn put_diff_stats(&self, a: &str, b: &str, stats: &DiffStats) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;
        let key = pair_key(a, b);
        let val = bitcode::encode(stats);
        txn.put(self.diff_stats, &key, &val, WriteFlags::empty())?;
        txn.commit()?;
        Ok(())
    }

    pub fn get_diff_stats(&self, a: &str, b: &str) -> Result<Option<DiffStats>> {
        let txn = self.env.begin_ro_txn()?;
        let key = pair_key(a, b);
        match txn.get(self.diff_stats, &key) {
            Ok(v) => Ok(Some(bitcode::decode(v)?)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_diff_rows(
        &self,
        a: &str,
        b: &str,
        chr: Option<&str>,
        status: Option<crate::model::DiffStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<DiffRow>> {
        let txn = self.env.begin_ro_txn()?;
        let (a, b) = canon_pair(a, b);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(a.as_bytes());
        prefix.push(ZERO);
        prefix.extend_from_slice(b.as_bytes());
        prefix.push(ZERO);
        if let Some(c) = chr {
            prefix.extend_from_slice(&chr_pad(c));
            prefix.push(ZERO);
        }

        let mut out = Vec::new();
        let mut skipped = 0usize;
        let mut cur = txn.open_ro_cursor(self.diff)?;
        let mut iter = cur.iter_from(&prefix);
        while let Some(item) = iter.next() {
            let (k, v) = item;
            if !k.starts_with(&prefix) {
                break;
            }
            let row: DiffRow = bitcode::decode(v)?;
            if let Some(want) = status {
                if row.status != want {
                    continue;
                }
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            out.push(row);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    pub fn for_each_diff_row<F: FnMut(&DiffRow) -> Result<()>>(
        &self,
        a: &str,
        b: &str,
        mut f: F,
    ) -> Result<()> {
        let txn = self.env.begin_ro_txn()?;
        let (a, b) = canon_pair(a, b);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(a.as_bytes());
        prefix.push(ZERO);
        prefix.extend_from_slice(b.as_bytes());
        prefix.push(ZERO);

        let mut cur = txn.open_ro_cursor(self.diff)?;
        let mut iter = cur.iter_from(&prefix);
        while let Some(item) = iter.next() {
            let (k, v) = item;
            if !k.starts_with(&prefix) {
                break;
            }
            let row: DiffRow = bitcode::decode(v)?;
            f(&row)?;
        }
        Ok(())
    }

    pub fn get_diff_at_index(&self, a: &str, b: &str, index: usize) -> Result<Option<DiffRow>> {
        let txn = self.env.begin_ro_txn()?;
        let (a, b) = canon_pair(a, b);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(a.as_bytes());
        prefix.push(ZERO);
        prefix.extend_from_slice(b.as_bytes());
        prefix.push(ZERO);

        let mut cur = txn.open_ro_cursor(self.diff)?;
        let mut iter = cur.iter_from(&prefix);
        let mut i = 0usize;
        while let Some(item) = iter.next() {
            let (k, v) = item;
            if !k.starts_with(&prefix) {
                return Ok(None);
            }
            if i == index {
                let row: DiffRow = bitcode::decode(v)?;
                return Ok(Some(row));
            }
            i += 1;
        }
        Ok(None)
    }

    // ----- zygosity -----

    pub fn clear_zygosity(&self) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;
        txn.clear_db(self.zygosity_stats)?;
        txn.commit()?;
        Ok(())
    }

    pub fn put_zygosity_stats(&self, person: &str, stats: &ZygosityStats) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;
        let val = bitcode::encode(stats);
        txn.put(
            self.zygosity_stats,
            &person.as_bytes(),
            &val,
            WriteFlags::empty(),
        )?;
        txn.commit()?;
        Ok(())
    }

    pub fn get_zygosity_stats(&self, person: &str) -> Result<Option<ZygosityStats>> {
        let txn = self.env.begin_ro_txn()?;
        match txn.get(self.zygosity_stats, &person.as_bytes()) {
            Ok(v) => Ok(Some(bitcode::decode(v)?)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_all_zygosity(&self) -> Result<Vec<ZygosityStats>> {
        let people = self.list_people()?;
        let mut out = Vec::with_capacity(people.len());
        for p in people {
            if let Some(s) = self.get_zygosity_stats(&p)? {
                out.push(s);
            }
        }
        Ok(out)
    }
}

pub fn canon_pair<'a>(a: &'a str, b: &'a str) -> (&'a str, &'a str) {
    if a <= b { (a, b) } else { (b, a) }
}

pub fn pair_key(a: &str, b: &str) -> Vec<u8> {
    let (a, b) = canon_pair(a, b);
    let mut k = Vec::with_capacity(a.len() + 1 + b.len());
    k.extend_from_slice(a.as_bytes());
    k.push(ZERO);
    k.extend_from_slice(b.as_bytes());
    k
}

pub fn diff_key(a: &str, b: &str, chr: &str, pos: u32, rsid: &str) -> Vec<u8> {
    let (a, b) = canon_pair(a, b);
    let chr_p = chr_pad(chr);
    let mut k = Vec::with_capacity(a.len() + b.len() + chr_p.len() + 4 + rsid.len() + 4);
    k.extend_from_slice(a.as_bytes());
    k.push(ZERO);
    k.extend_from_slice(b.as_bytes());
    k.push(ZERO);
    k.extend_from_slice(&chr_p);
    k.push(ZERO);
    k.extend_from_slice(&pos.to_be_bytes());
    k.push(ZERO);
    k.extend_from_slice(rsid.as_bytes());
    k
}

pub fn chr_pad(chr: &str) -> Vec<u8> {
    match chr {
        "X" => b"23".to_vec(),
        "Y" => b"24".to_vec(),
        "MT" | "M" => b"25".to_vec(),
        s => {
            if let Ok(n) = s.parse::<u8>() {
                format!("{:02}", n).into_bytes()
            } else {
                let mut v = b"99_".to_vec();
                v.extend_from_slice(s.as_bytes());
                v
            }
        }
    }
}
