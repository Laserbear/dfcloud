//Let's write a web server in Rust that accepts a GET and POST requests. When receiving a POST request, it will deterministically hash the data and store it in an in memory map. 
//When receiving a GET request, it will look up the hash and return the value. 
use warp::{http::Response, Filter, http::StatusCode, Rejection, reject::Reject, Reply, reply::{self, Json}};
use warp::cors::Cors;

use redis::{AsyncCommands, Client};
use serde_json::{Value, json};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let cors = warp::cors()
    .allow_any_origin()
    .allow_headers(vec!["Content-Type"])
    .allow_methods(vec!["GET", "POST"]);
    println!("Starting server on http://0.0.0.0:8008");
    let redis_client = Client::open("redis://0.0.0.0:6379").unwrap();
    let redis_client = Arc::new(redis_client);

    println!("Connected to Redis");

    let redis_filter = warp::any().map(move || redis_client.clone());

    let get_route = warp::get()
        .and(warp::path!("hash" / String))
        .and(redis_filter.clone())
        .and_then(handle_get);

    let post_route = warp::post()
        .and(warp::path("hash"))
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(redis_filter.clone())
        .and_then(handle_post);

    let routes = get_route.or(post_route).with(cors);

    warp::serve(routes).run(([0, 0, 0, 0], 8008)).await;
}

#[derive(Debug)]
struct JsonRejection {
    message: Value,
}

impl Reject for JsonRejection {}

async fn handle_get(
    hash: String,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    //println!("Handling GET request for hash: {}", hash);
    let augmented_hash = "df".to_owned() + &hash; //so clients can't write to ratelimiter... is this brittle?
    let mut conn = client.get_async_connection().await.unwrap();
    let result: Option<String> = conn.get(&augmented_hash).await.unwrap();

    if let Some(value) = result {
        Ok(warp::reply::json(&value))
    } else {
        let error_message = json!({ "error": "Hash not found" });
        Err(warp::reject::custom(JsonRejection { message: error_message }))
    }
}

async fn handle_post(
    data: Value,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    println!("Handling POST request for data: {:?}", data);
    let mut conn = client.get_async_connection().await.unwrap();
    let hash = hash_data(&data);
    let augmented_hash = "df".to_owned() + &hash;
    let _: () = conn.set(&augmented_hash, serde_json::to_string(&data).unwrap()).await.unwrap();
    let hash_response = json!({ "hash": augmented_hash });
    Ok(warp::reply::json(&hash_response))
}

fn hash_data(data: &Value) -> String {
    let data_string = serde_json::to_string(data).unwrap(); // Convert the JSON value to a string
    let mut hasher = DefaultHasher::new();
    data_string.hash(&mut hasher); // Hash the string representation
    format!("{:x}", hasher.finish())
}


