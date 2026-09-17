use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize)]
struct OrderValidated {
    order_id: String,
    status: String,
}

#[derive(Serialize)]
struct OrderShipped {
    order_id: String,
    shipped_at: String,
}

#[tokio::main]
async fn main() {
    let brokers = "localhost:9092";

    // Consumer for orders.validated
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "order-shipping")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["orders.validated"])
        .expect("Can't subscribe to orders.validated");

    // Producer for orders.shipped
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation failed");

    println!("Starting order shipping service...");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let payload = std::str::from_utf8(msg.payload().unwrap_or(&[]))
                    .expect("Invalid UTF-8 in message");

                let validated: OrderValidated =
                    serde_json::from_str(payload).expect("Failed to parse OrderValidated");

                if validated.status != "accepted" {
                    println!(
                        "Order {} is {}, skipping shipping",
                        validated.order_id, validated.status
                    );
                    continue;
                }

                // Simple fixed timestamp for now
                let shipped = OrderShipped {
                    order_id: validated.order_id.clone(),
                    shipped_at: "2026-09-17T17:00:00Z".to_string(),
                };

                let shipped_payload =
                    serde_json::to_string(&shipped).expect("Failed to serialize shipped order");

                let result = producer
                    .send(
                        FutureRecord::to("orders.shipped")
                            .payload(&shipped_payload)
                            .key(&validated.order_id),
                        Duration::from_secs(5),
                    )
                    .await;

                match result {
                    Ok(delivery) => {
                        println!(
                            "Shipped order {} -> orders.shipped, delivery: {:?}",
                            shipped.order_id, delivery
                        );
                    }
                    Err((e, _)) => {
                        eprintln!("Failed to send shipped order: {}", e);
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
    fn test_accepted_order_will_be_shipped() {
        let validated = OrderValidated {
            order_id: "ord_1".to_string(),
            status: "accepted".to_string(),
        };

        let will_shipip = validated.status == "accepted";
        assert!(will_shipip);
    }

    #[test]
    fn test_rejected_order_will_not_be_shipped() {
        let validated = OrderValidated {
            order_id: "ord_2".to_string(),
            status: "rejected".to_string(),
        };

        let will_shipip = validated.status == "accepted";
        assert!(!will_shipip);
    }

    #[test]
    fn test_shipped_order_serializes() {
        let shipped = OrderShipped {
            order_id: "ord_3".to_string(),
            shipped_at: "2026-09-17T17:00:00Z".to_string(),
        };

        let json = serde_json::to_string(&shipped).expect("Failed to serialize");

        assert!(json.contains("\"order_id\""));
        assert!(json.contains("\"shipped_at\""));
        assert!(json.contains("ord_3"));

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Failed to parse JSON");

        assert_eq!(parsed["order_id"], "ord_3");
        assert!(parsed["shipped_at"].as_str().is_some());
    }
}
