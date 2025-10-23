use std::collections::HashMap;
use std::env;

use dotenv::dotenv;
use futures_util::TryStreamExt;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use tokio;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "Usage: {} <namespace> <table_name> <from_snapshot_id> <to_snapshot_id>",
            args[0]
        );
        return Err("Not enough arguments".into());
    }
    let namespace = &args[1];
    let table_name = &args[2];
    let from_snapshot_id: i64 = args[3].parse().unwrap();
    let to_snapshot_id: i64 = args[4].parse().unwrap();

    let bucket = env::var("S3_BUCKET").expect("S3_BUCKET must be set");

    // Create catalog
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([
                (
                    REST_CATALOG_PROP_URI.to_string(),
                    "http://localhost:8181".to_string(),
                ),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    format!("s3://{}", bucket),
                ),
                (
                    S3_ACCESS_KEY_ID.to_string(),
                    env::var("AWS_ACCESS_KEY_ID").unwrap_or("admin".to_string()),
                ),
                (
                    S3_SECRET_ACCESS_KEY.to_string(),
                    env::var("AWS_SECRET_ACCESS_KEY").unwrap_or("password".to_string()),
                ),
                (
                    S3_REGION.to_string(),
                    env::var("AWS_REGION").unwrap_or("us-east-1".to_string()),
                ),
                (
                    S3_ENDPOINT.to_string(),
                    env::var("AWS_ENDPOINT_URL").unwrap_or("http://localhost:9000".to_string()),
                ),
            ]),
        )
        .await?;

    let namespace = NamespaceIdent::new(namespace.clone());
    // Error if namespace doesn't exist
    if !catalog.namespace_exists(&namespace).await? {
        return Err("Namespace does not exist".into());
    }

    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            namespace.clone(),
            table_name.clone(),
        ))
        .await?;
    let mut stream = table
        .incremental_scan(from_snapshot_id, to_snapshot_id)
        .build()?
        .to_arrow()
        .await?;

    let mut rows = 0;
    while let Some((batch_type, batch)) = stream.try_next().await? {
        rows += batch.num_rows();
        println!("Batch: {:?}, Type: {:?}", batch, batch_type);
    }

    println!("Total rows: {:?}", rows);
    Ok(())
}
