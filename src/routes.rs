use actix_web::{HttpResponse, Responder, get, http::header, web};
use serde::Deserialize;
use std::sync::Arc;

use crate::model::{DiffRow, DiffStatus};
use crate::store::Store;

pub struct AppState {
    pub store: Arc<Store>,
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default)]
    pub exact: bool,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
}

fn default_search_limit() -> usize {
    64
}

#[get("/api/search")]
pub async fn search(state: web::Data<AppState>, q: web::Query<SearchQuery>) -> impl Responder {
    let prefix = q.q.trim();
    if prefix.is_empty() {
        return HttpResponse::Ok().json(serde_json::json!({ "hits": [] }));
    }

    let store = state.store.clone();
    let exact = q.exact;
    let limit = q.limit.min(1000);
    let prefix_owned = prefix.to_string();

    let res = web::block(move || store.prefix_search(&prefix_owned, exact, limit)).await;

    match res {
        Ok(Ok(hits)) => HttpResponse::Ok().json(serde_json::json!({ "hits": hits })),
        Ok(Err(e)) => {
            tracing::error!("search error: {:?}", e);
            HttpResponse::InternalServerError().body("search failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

#[get("/api/people")]
pub async fn people(state: web::Data<AppState>) -> impl Responder {
    let store = state.store.clone();
    let res = web::block(move || store.list_people()).await;
    match res {
        Ok(Ok(p)) => HttpResponse::Ok().json(serde_json::json!({ "people": p })),
        Ok(Err(e)) => {
            tracing::error!("list_people error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

#[derive(Deserialize)]
pub struct PairQuery {
    pub a: String,
    pub b: String,
}

#[get("/api/diff/stats")]
pub async fn diff_stats(state: web::Data<AppState>, q: web::Query<PairQuery>) -> impl Responder {
    let store = state.store.clone();
    let a = q.a.clone();
    let b = q.b.clone();
    let res = web::block(move || store.get_diff_stats(&a, &b)).await;
    match res {
        Ok(Ok(Some(s))) => HttpResponse::Ok().json(s),
        Ok(Ok(None)) => HttpResponse::NotFound().body("no diff for that pair"),
        Ok(Err(e)) => {
            tracing::error!("diff_stats error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

#[derive(Deserialize)]
pub struct DiffQuery {
    pub a: String,
    pub b: String,
    pub chr: Option<String>,
    pub status: Option<String>,
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_diff_limit")]
    pub limit: usize,
}

fn default_diff_limit() -> usize {
    200
}

fn parse_status(s: &str) -> Option<Option<DiffStatus>> {
    match s {
        "all" => Some(None),
        "same" => Some(Some(DiffStatus::Same)),
        "different" => Some(Some(DiffStatus::Different)),
        "nocall" => Some(Some(DiffStatus::NoCall)),
        "missing" => Some(Some(DiffStatus::Missing)),
        _ => None,
    }
}

#[get("/api/diff")]
pub async fn diff(state: web::Data<AppState>, q: web::Query<DiffQuery>) -> impl Responder {
    let store = state.store.clone();
    let a = q.a.clone();
    let b = q.b.clone();
    let chr = q.chr.clone();
    let status = match q.status.as_deref() {
        None => Some(DiffStatus::Different),
        Some(s) => match parse_status(s) {
            Some(opt) => opt,
            None => return HttpResponse::BadRequest().body("invalid status"),
        },
    };
    let offset = q.offset;
    let limit = q.limit.min(2000);

    let res =
        web::block(move || store.get_diff_rows(&a, &b, chr.as_deref(), status, offset, limit))
            .await;

    match res {
        Ok(Ok(rows)) => HttpResponse::Ok().json(serde_json::json!({ "rows": rows })),
        Ok(Err(e)) => {
            tracing::error!("diff error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

#[get("/api/diff/track")]
pub async fn diff_track(state: web::Data<AppState>, q: web::Query<PairQuery>) -> impl Responder {
    let store = state.store.clone();
    let a = q.a.clone();
    let b = q.b.clone();

    let res = web::block(
        move || -> anyhow::Result<(Vec<u8>, Vec<(String, usize, usize)>)> {
            let mut bytes = Vec::with_capacity(700_000);
            let mut boundaries: Vec<(String, usize, usize)> = Vec::new();
            let mut cur_chr = String::new();
            let mut cur_start = 0usize;

            store.for_each_diff_row(&a, &b, |row: &DiffRow| {
                let byte = match row.status {
                    DiffStatus::Same => 0u8,
                    DiffStatus::Different => 1u8,
                    DiffStatus::NoCall => 2u8,
                    DiffStatus::Missing => 3u8,
                };
                if row.chromosome != cur_chr {
                    if !cur_chr.is_empty() {
                        boundaries.push((cur_chr.clone(), cur_start, bytes.len() - cur_start));
                    }
                    cur_chr = row.chromosome.clone();
                    cur_start = bytes.len();
                }
                bytes.push(byte);
                Ok(())
            })?;

            if !cur_chr.is_empty() {
                boundaries.push((cur_chr.clone(), cur_start, bytes.len() - cur_start));
            }
            Ok((bytes, boundaries))
        },
    )
    .await;

    match res {
        Ok(Ok((bytes, boundaries))) => {
            let header_json =
                serde_json::to_string(&boundaries).unwrap_or_else(|_| "[]".to_string());
            HttpResponse::Ok()
                .insert_header((header::CONTENT_TYPE, "application/octet-stream"))
                .insert_header(("x-chr-boundaries", header_json))
                .insert_header(("access-control-expose-headers", "x-chr-boundaries"))
                .body(bytes)
        }
        Ok(Err(e)) => {
            tracing::error!("track error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

#[derive(Deserialize)]
pub struct AtQuery {
    pub a: String,
    pub b: String,
    pub index: usize,
}

#[get("/api/diff/at")]
pub async fn diff_at(state: web::Data<AppState>, q: web::Query<AtQuery>) -> impl Responder {
    let store = state.store.clone();
    let a = q.a.clone();
    let b = q.b.clone();
    let index = q.index;

    let res = web::block(move || store.get_diff_at_index(&a, &b, index)).await;
    match res {
        Ok(Ok(Some(row))) => HttpResponse::Ok().json(row),
        Ok(Ok(None)) => HttpResponse::NotFound().body("index out of range"),
        Ok(Err(e)) => {
            tracing::error!("diff_at error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

// ----- zygosity -----

#[derive(Deserialize)]
pub struct PersonQuery {
    pub person: String,
}

#[get("/api/zygosity")]
pub async fn zygosity(state: web::Data<AppState>, q: web::Query<PersonQuery>) -> impl Responder {
    let store = state.store.clone();
    let person = q.person.clone();
    let res = web::block(move || store.get_zygosity_stats(&person)).await;
    match res {
        Ok(Ok(Some(s))) => HttpResponse::Ok().json(s),
        Ok(Ok(None)) => HttpResponse::NotFound().body("no zygosity stats for that person"),
        Ok(Err(e)) => {
            tracing::error!("zygosity error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}

#[get("/api/zygosity/all")]
pub async fn zygosity_all(state: web::Data<AppState>) -> impl Responder {
    let store = state.store.clone();
    let res = web::block(move || store.get_all_zygosity()).await;
    match res {
        Ok(Ok(stats)) => HttpResponse::Ok().json(serde_json::json!({ "stats": stats })),
        Ok(Err(e)) => {
            tracing::error!("zygosity_all error: {:?}", e);
            HttpResponse::InternalServerError().body("failed")
        }
        Err(e) => {
            tracing::error!("blocking error: {:?}", e);
            HttpResponse::InternalServerError().body("internal error")
        }
    }
}
