use anyhow::{Context, Result};
use lmdb::{Cursor, Database, DatabaseFlags, Environment, Transaction, WriteFlags};
use std::path::Path;

use crate::model::{Snp, SnpHit};

pub struct Store {
    env: Environment,
    snps: Database,     // key: "{person}\0{rsid}"  val: bitcode(Snp)
    rsid_idx: Database, // key: "{rsid}\0{person}"  val: empty (used for prefix scan)
    meta: Database,     // key: "person:{name}"     val: "imported" / "skipped"
}

const ZERO: u8 = 0;

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path).ok();

        // ~64 GiB ceiling. real usage ~50MB per person, this is just headroom.
        let env = Environment::new()
            .set_max_dbs(8)
            .set_map_size(64 * 1024 * 1024 * 1024)
            .open(path)
            .with_context(|| format!("open lmdb at {}", path.display()))?;

        let snps = env.create_db(Some("snps"), DatabaseFlags::empty())?;
        let rsid_idx = env.create_db(Some("rsid_idx"), DatabaseFlags::empty())?;
        let meta = env.create_db(Some("meta"), DatabaseFlags::empty())?;

        Ok(Self { env, snps, rsid_idx, meta })
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

    pub fn import_person(&self, person: &str, snps: &[Snp]) -> Result<()> {
        let mut txn = self.env.begin_rw_txn()?;

        for s in snps {
            // snps: person\0rsid -> bitcode(Snp)
            let mut k = Vec::with_capacity(person.len() + 1 + s.rsid.len());
            k.extend_from_slice(person.as_bytes());
            k.push(ZERO);
            k.extend_from_slice(s.rsid.as_bytes());

            let v = bitcode::encode(s);
            txn.put(self.snps, &k, &v, WriteFlags::empty())?;

            // rsid_idx: rsid\0person -> empty
            let mut ik = Vec::with_capacity(s.rsid.len() + 1 + person.len());
            ik.extend_from_slice(s.rsid.as_bytes());
            ik.push(ZERO);
            ik.extend_from_slice(person.as_bytes());
            txn.put(self.rsid_idx, &ik, &b"", WriteFlags::empty())?;
        }

        let mkey = format!("person:{}", person);
        txn.put(self.meta, &mkey.as_bytes(), b"imported", WriteFlags::empty())?;
        txn.commit()?;
        Ok(())
    }

    // exact lookup: return all hits across people for one rsid.
    // pub fn get_exact(&self, rsid: &str) -> Result<Vec<SnpHit>> {
    //     self.prefix_search(rsid, true, 64)
    // }

    // prefix search on rsid. e.g. "rs3131" returns rs3131972 etc.
    // limit caps the number of hits.
    pub fn prefix_search(&self, prefix: &str, exact: bool, limit: usize) -> Result<Vec<SnpHit>> {
        let txn = self.env.begin_ro_txn()?;
        let mut hits = Vec::new();

        // walk rsid_idx starting at prefix
        {
            let mut cur = txn.open_ro_cursor(self.rsid_idx)?;
            let start = prefix.as_bytes();
            let mut iter = cur.iter_from(start);
            while let Some(item) = iter.next() {
                let (k, _) = item;
                // split rsid\0person
                let zero_pos = match k.iter().position(|b| *b == ZERO) {
                    Some(p) => p,
                    None => continue,
                };
                let rsid = match std::str::from_utf8(&k[..zero_pos]) { Ok(s) => s, Err(_) => continue };
                let person = match std::str::from_utf8(&k[zero_pos + 1..]) { Ok(s) => s, Err(_) => continue };

                if exact {
                    if rsid != prefix { break; }
                } else {
                    if !rsid.starts_with(prefix) { break; }
                }

                // fetch snp from snps db
                let mut sk = Vec::with_capacity(person.len() + 1 + rsid.len());
                sk.extend_from_slice(person.as_bytes());
                sk.push(ZERO);
                sk.extend_from_slice(rsid.as_bytes());

                if let Ok(val) = txn.get(self.snps, &sk) {
                    let snp: Snp = bitcode::decode(val)?;
                    hits.push(SnpHit { person: person.to_string(), snp });
                    if hits.len() >= limit { break; }
                }
            }
        }

        Ok(hits)
    }
}