use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize)]
struct Order {
    order_id: String,
    #[allow(dead_code)]
    user_id: String,
    #[allow(dead_code)]
    items: Vec<String>,
    total_price: u32,
}

#[derive(Serialize)]
struct OrderValidated {
    order_id: String,
    status: String, // "accepted" / "rejected"
}

#[tokio::main]
async fn main() {
    let brokers = "localhost:9092";

    // Consumer for orders.created
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "order-validator")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["orders.created"])
        .expect("Can't subscribe to orders.created");

    // Producer for orders.validated
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    println!("Starting order validator...");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload = std::str::from_utf8(msg.payload().unwrap_or(&[]))
                    .expect("Invalid UTF-8 in message");

                let order: Order =
                    serde_json::from_str(payload).expect("Failed to parse OrderCreated");

                let status = if order.total_price > 15_000 {
                    "rejected"
                } else {
                    "accepted"
                };

                let validated = OrderValidated {
                    order_id: order.order_id.clone(),
                    status: status.to_string(),
                };

                let validated_payload =
                    serde_json::to_string(&validated).expect("Failed to serialize validated order");

                let result = producer
                    .send(
                        FutureRecord::to("orders.validated")
                            .payload(&validated_payload)
                            .key(&order.order_id),
                        Duration::from_secs(5),
                    )
                    .await;

                match result {
                    Ok(delivery) => {
                        println!(
                            "Validated order {} as {} -> orders.validated, delivery: {:?}",
                            order.order_id, status, delivery
                        );
                    }
                    Err((e, _)) => {
                        eprintln!("Failed to send validated order: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Kafka error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validates_low_amount_as_accepted() {
        let order = Order {
            order_id: "ord_1".to_string(),
            user_id: "user_1".to_string(),
            items: vec!["item_a".to_string()],
            total_price: 10_000,
        };

        let status = if order.total_price > 15_000 {
            "rejected"
        } else {
            "accepted"
        };

        assert_eq!(status, "accepted");
    }

    #[test]
    fn test_validates_high_amount_as_rejected() {
        let order = Order {
            order_id: "ord_2".to_string(),
            user_id: "user_2".to_string(),
            items: vec!["item_b".to_string()],
            total_price: 20_000,
        };

        let status = if order.total_price > 15_000 {
            "rejected"
        } else {
            "accepted"
        };

        assert_eq!(status, "rejected");
    }

    #[test]
    fn test_validated_order_serializes() {
        let validated = OrderValidated {
            order_id: "ord_3".to_string(),
            status: "accepted".to_string(),
        };

        let json = serde_json::to_string(&validated).expect("Failed to serialize");

        assert!(json.contains("\"order_id\""));
        assert!(json.contains("\"status\""));
        assert!(json.contains("accepted"));

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Failed to parse JSON");

        assert_eq!(parsed["order_id"], "ord_3");
        assert_eq!(parsed["status"], "accepted");
    }
}
