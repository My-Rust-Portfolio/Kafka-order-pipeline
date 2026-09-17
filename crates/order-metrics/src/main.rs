use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::collections::HashMap;
use tokio::time::{Duration, interval};

#[tokio::main]
async fn main() {
    let brokers = "localhost:9092";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "order-metrics")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&["orders.created", "orders.validated", "orders.shipped"])
        .expect("Can't subscribe to topics");

    println!("Starting order metrics consumer...");

    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut tick = interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            msg = consumer.recv() => {
                match msg {
                    Ok(m) => {
                        let topic = m.topic().to_string();
                        *counts.entry(topic).or_insert(0) += 1;
                    }
                    Err(e) => {
                        eprintln!("Kafka error: {}", e);
                    }
                }
            }
            _ = tick.tick() => {
                println!("Metrics so far:");
                let mut sorted: Vec<_> = counts.iter().collect();
                sorted.sort_by_key(|(t, _)| t.as_str());
                for (topic, count) in sorted {
                    println!("  {}: {}", topic, count);
                }
            }
        }
    }
}
