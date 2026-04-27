use actix_web::{HttpResponse, Responder, get, web};
use serde::Deserialize;
use std::sync::Arc;

use crate::store::Store;

pub struct AppState {
    pub store: Arc<Store>,
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default)]
    pub exact: bool,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize { 64 }

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

    // lmdb is sync; offload so we don't block the actix worker.
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