use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::time::Duration;
use tokio::time::interval;

#[derive(Serialize)]
struct Order {
    order_id: String,
    user_id: String,
    items: Vec<String>,
    total_price: u32,
}

fn random_order() -> Order {
    let order_id = format!("ord_{}", rand::random::<u32>());
    let user_id = format!("user_{}", rand::random::<u32>() % 1000);
    let items = vec![
        format!("item_{}", rand::random::<u32>() % 100),
        format!("item_{}", rand::random::<u32>() % 100),
    ];
    let total_price = (rand::random::<u32>() % 20000) + 500; // between 5 and 200

    Order {
        order_id,
        user_id,
        items,
        total_price,
    }
}

#[tokio::main]
async fn main() {
    let brokers = "localhost:9092";
    let topic = "orders.created";

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    println!("Starting order producer, sending to topic '{}'", topic);

    let mut tick = interval(Duration::from_secs(2));

    loop {
        tick.tick().await;

        let order = random_order();
        let payload = serde_json::to_string(&order).expect("Failed to serialize order");

        let result = producer
            .send(
                FutureRecord::to(topic)
                    .payload(&payload)
                    .key(&order.order_id),
                Duration::from_secs(0),
            )
            .await;

        match result {
            Ok(delivery) => {
                println!(
                    "Produced order {} ({} price) -> delivery: {:?}",
                    order.order_id, order.total_price, delivery
                );
            }
            Err((e, _)) => {
                eprintln!("Failed to produce order: {}", e);
            }
        }
    }
}
