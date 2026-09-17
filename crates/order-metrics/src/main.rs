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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn test_counts_increase_per_topic() {
        let mut counts: HashMap<String, u64> = HashMap::new();

        let topics = vec!["orders.created", "orders.validated", "orders.shipped"];

        for topic in &topics {
            *counts.entry(topic.to_string()).or_insert(0) += 1;
        }

        assert_eq!(counts.get("orders.created"), Some(&1));
        assert_eq!(counts.get("orders.validated"), Some(&1));
        assert_eq!(counts.get("orders.shipped"), Some(&1));

        // Simulate more messages
        *counts.entry("orders.created".to_string()).or_insert(0) += 2;

        assert_eq!(counts.get("orders.created"), Some(&3));
    }

    #[test]
    fn test_sorted_metrics_output() {
        let mut counts: HashMap<String, u64> = HashMap::new();
        counts.insert("orders.shipped".to_string(), 5);
        counts.insert("orders.created".to_string(), 10);
        counts.insert("orders.validated".to_string(), 7);

        let mut sorted: Vec<_> = counts.iter().collect();
        sorted.sort_by_key(|(t, _)| t.as_str());

        assert_eq!(sorted[0].0, "orders.created");
        assert_eq!(sorted[1].0, "orders.shipped");
        assert_eq!(sorted[2].0, "orders.validated");
    }
}
