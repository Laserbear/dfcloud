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

    let ip_filter = warp::addr::remote().map(|addr: Option<std::net::SocketAddr>| {addr.map(|socket_addr| socket_addr.ip())});

    let get_route = warp::get()
        .and(warp::path!("hash" / String))
        .and(ip_filter)
        .and(redis_filter.clone())
        .and_then(handle_get);

    let post_route = warp::post()
        .and(warp::path("hash"))
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(ip_filter)
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
    _client_ip: Option<std::net::IpAddr>,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    //println!("Handling GET request for hash: {}", hash);
    let augmented_hash = "df".to_owned() + &hash; //so clients can't write to ratelimiter... is this brittle?
    let mut conn = client.get_async_connection().await.unwrap();
    let THRESHOLD = 10;
    if let Some(ip) = _client_ip {
        let ip_key: String  = "ip".to_owned() + &ip.to_string(); 
        let result: Option<String> = conn.get(ip_key.clone()).await.unwrap();
        if let Some(value) = result {
            if let Ok(mut int_value) = value.parse::<i32>() {
                if int_value > THRESHOLD {
                    //above threshold, deny request
                    let error_message = json!({"error": "Rate limit exceeded"});
                    return Err(warp::reject::custom(JsonRejection { message: error_message}))
                } else {
                    //below threshold, increment counter
                    int_value += 1;
                    let _: () = conn.set(ip_key, int_value).await.unwrap();
                }
            }
            else {
                //Somehow the value associated with their IP wasn't an integer, this indicates
                //we have collisions between keys or clients can manipulate our datastore
                let error_message = json!({"error": "Internal Server Error"});
                return Err(warp::reject::custom(JsonRejection { message: error_message}))
            }
        } else {
            let _: () = conn.set(ip_key.clone(), 0).await.unwrap();
            let _: () = conn.expire(ip_key.clone(), 60).await.unwrap();
        }
    } else {
        let error_message = json!({"error": "Failed to determine client IP"}); //failing closed here, we could consider failing open
        return Err(warp::reject::custom(JsonRejection { message: error_message}))
  
    }


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
    _client_ip: Option<std::net::IpAddr>,
    client: Arc<Client>,
) -> Result<impl warp::Reply, Rejection> {
    println!("Handling POST request for data: {:?}", data);
    let mut conn = client.get_async_connection().await.unwrap();
    let THRESHOLD = 10;
    if let Some(ip) = _client_ip {
        let ip_key: String = "ip".to_owned() + &ip.to_string();
        let result: Option<String> = conn.get(ip_key.clone()).await.unwrap();
        if let Some(value) = result {
            if let Ok(mut int_value) = value.parse::<i32>() {
                if int_value > THRESHOLD {
                    //above threshold, deny request
                    let error_message = json!({"error": "Rate limit exceeded"});
                    return Err(warp::reject::custom(JsonRejection { message: error_message}))
                } else {
                    //below threshold, increment counter
                    int_value += 1;
                    let _: () = conn.set(ip_key, int_value).await.unwrap();
                }
            }
            else {
                //Somehow the value associated with their IP wasn't an integer, this indicates
                //we have collisions between keys or clients can manipulate our datastore
                let error_message = json!({"error": "Internal Server Error"});
                return Err(warp::reject::custom(JsonRejection { message: error_message}))
            }
        }
        else {
            let _: () = conn.set(ip_key.clone(), 0).await.unwrap();
            let _: () = conn.expire(ip_key.clone(), 60).await.unwrap();
        }
    }
    else {
        let error_message = json!({"error": "Failed to determine client IP"}); //failing closed here, we could consider failing open
        return Err(warp::reject::custom(JsonRejection { message: error_message}))
    }

    let hash = hash_data(&data);
    let augmented_hash = "df".to_owned() + &hash;
    let _: () = conn.set(&augmented_hash, serde_json::to_string(&data).unwrap()).await.unwrap();
    let hash_response = json!({ "hash": hash });
    return  Ok(warp::reply::json(&hash_response))
}

fn hash_data(data: &Value) -> String {
    let data_string = serde_json::to_string(data).unwrap(); // Convert the JSON value to a string
    let mut hasher = DefaultHasher::new();
    data_string.hash(&mut hasher); // Hash the string representation
    format!("{:x}", hasher.finish())
}


