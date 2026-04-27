use actix_cors::Cors;
use actix_files::Files;
use actix_web::{App, HttpServer, middleware, web};
use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

mod differ;
mod importer;
mod model;
mod parser;
mod routes;
mod store;

use routes::AppState;
use store::Store;

#[actix_web::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let bind = std::env::var("SNPRS_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let lmdb_path = std::env::var("SNPRS_LMDB").unwrap_or_else(|_| "lmdb_snps".to_string());
    let exports_dir = std::env::var("SNPRS_EXPORTS").unwrap_or_else(|_| "exports".to_string());

    let store = Arc::new(Store::open(Path::new(&lmdb_path))?);

    importer::import_dir(&store, Path::new(&exports_dir))?;

    let state = web::Data::new(AppState {
        store: store.clone(),
    });

    tracing::info!("listening on http://{}", bind);

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(middleware::Compress::default())
            .wrap(Cors::permissive())
            .service(routes::search)
            .service(routes::people)
            .service(routes::diff_stats)
            .service(routes::diff)
            .service(routes::diff_track)
            .service(routes::diff_at)
            .service(Files::new("/", "static").index_file("index.html"))
    })
    .bind(&bind)?
    .run()
    .await?;

    Ok(())
}
