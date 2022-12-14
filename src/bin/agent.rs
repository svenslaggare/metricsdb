use std::str::FromStr;
use std::time::SystemTime;

use fnv::FnvHashMap;

use reqwest::StatusCode;
use serde_json::json;

use metricsdb::metric::common::CountInput;
use metricsdb::metric::tags::Tag;
use metricsdb::engine::io::{AddGaugeValue, AddCountValue};

struct AgentConfig {
    base_url: String,
    sample_rate: f64
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            base_url: "http://localhost:9090".to_string(),
            sample_rate: 1.0
        }
    }
}

#[tokio::main]
async fn main() {
    let mut cpu_usage_collector = CpuUsageCollector::new();
    let mut context_switches_collector = ContextSwitchesCollector::new();
    let mut memory_usage_collector = MemoryUsageCollector::new();

    let config = AgentConfig::default();

    let arguments = std::env::args().collect::<Vec<_>>();
    let hostname = if arguments.len() >= 2 {
        arguments[1].clone()
    } else {
        gethostname::gethostname().to_str().unwrap().to_owned()
    };

    let client = reqwest::Client::new();
    loop {
        let time_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();

        let cpu_usage = cpu_usage_collector.collect().unwrap();
        if !cpu_usage.is_empty() {
            let cpu_usage_json = json!(
                cpu_usage
                    .iter()
                    .map(|(core_name, cpu_usage)|
                        AddGaugeValue::new(
                            time_now,
                            *cpu_usage,
                            vec![Tag::from_ref("host", &hostname), Tag::from_ref("core", core_name)]
                        )
                    )
                    .collect::<Vec<_>>()
            );

            send_metric_data(
                &config,
                &client,
                "cpu_usage",
                "gauge",
                &cpu_usage_json
            ).await;
        }

        let memory_usage = memory_usage_collector.collect().unwrap();

        send_metric_data(
            &config,
            &client,
            "used_memory",
            "gauge",
            &json!(vec![AddGaugeValue::new(time_now, memory_usage.1, vec![Tag::from_ref("host", &hostname)])])
        ).await;

        send_metric_data(
            &config,
            &client,
            "total_memory",
            "gauge",
            &json!(vec![AddGaugeValue::new(time_now, memory_usage.0, vec![Tag::from_ref("host", &hostname)])])
        ).await;

        if let Some(context_switches) = context_switches_collector.collect().unwrap() {
            send_metric_data(
                &config,
                &client,
                "context_switches",
                "count",
                &json!(vec![AddCountValue::new(time_now, CountInput(context_switches as u32), vec![Tag::from_ref("host", &hostname)])])
            ).await;
        }

        std::thread::sleep(std::time::Duration::from_secs_f64(1.0 / config.sample_rate));
    }
}

async fn send_metric_data(config: &AgentConfig,
                          client: &reqwest::Client,
                          name: &str,
                          metric_type: &str,
                          metric_data: &serde_json::Value) {
    match post_result(&config, &client, name, metric_type, metric_data).await {
        Ok((status, content)) => {
            if !status.is_success() {
                println!("Failed to post result due to (status code: {}): {}", status, content)
            }
        }
        Err(err) => {
            println!("Failed to post result due to: {}", err);
        }
    }
}

async fn post_result(config: &AgentConfig,
                     client: &reqwest::Client,
                     name: &str,
                     metric_type: &str,
                     metric_data: &serde_json::Value) -> reqwest::Result<(StatusCode, String)> {
    println!("{}", metric_data);
    let response = client.put(format!("{}/metrics/{}/{}", config.base_url, metric_type, name))
        .json(&metric_data)
        .send()
        .await?;

    let response_status = response.status();

    let response_data = response.bytes().await?;
    let response_data = std::str::from_utf8(response_data.as_ref()).unwrap().to_owned();

    Ok((response_status, response_data))
}

struct CpuUsageCollector {
    prev_values: FnvHashMap<String, (i32, i32)>
}

impl CpuUsageCollector {
    pub fn new() -> CpuUsageCollector {
        CpuUsageCollector {
            prev_values: FnvHashMap::default()
        }
    }

    pub fn collect(&mut self) -> std::io::Result<Vec<(String, f64)>> {
        let mut usage = Vec::new();
        for line in std::fs::read_to_string("/proc/stat")?.lines() {
            let parts = line.split(" ").collect::<Vec<_>>();

            if parts[0].starts_with("cpu") && parts[0] != "cpu" {
                let core_name = parts[0];
                let int_parts = parts.iter().skip(1).map(|x| i32::from_str(x)).flatten().collect::<Vec<_>>();
                let idle = int_parts[3];
                let total = int_parts.iter().sum::<i32>();

                if let Some((prev_total, prev_idle)) = self.prev_values.get(core_name) {
                    let diff_total = total - prev_total;
                    let diff_idle = idle - prev_idle;
                    let cpu_usage = 1.0 - diff_idle as f64 / diff_total as f64;
                    usage.push((core_name.to_owned(), cpu_usage));
                }

                self.prev_values.insert(core_name.to_owned(), (total, idle));
            }
        }

        Ok(usage)
    }
}

struct ContextSwitchesCollector {
    prev_context_switches: Option<i64>
}

impl ContextSwitchesCollector {
    pub fn new() -> ContextSwitchesCollector {
        ContextSwitchesCollector {
            prev_context_switches: None
        }
    }

    pub fn collect(&mut self) -> std::io::Result<Option<i64>> {
        for line in std::fs::read_to_string("/proc/stat")?.lines() {
            let parts = line.split(" ").collect::<Vec<_>>();

            if parts[0].starts_with("ctxt") {
                let context_switches = i64::from_str(parts[1]).unwrap();
                let mut count = None;
                if let Some(prev_context_switches) = self.prev_context_switches {
                    count = Some(context_switches - prev_context_switches);
                }

                self.prev_context_switches = Some(context_switches);
                return Ok(count);
            }
        }

        Ok(None)
    }
}

struct MemoryUsageCollector {

}

impl MemoryUsageCollector {
    pub fn new() -> MemoryUsageCollector {
        MemoryUsageCollector {

        }
    }

    pub fn collect(&mut self) -> std::io::Result<(f64, f64)> {
        let mut total_memory = 0.0;
        let mut used_memory = 0.0;
        for line in std::fs::read_to_string("/proc/meminfo")?.lines() {
            let parts = line.split(":").collect::<Vec<_>>();
            let name = parts[0];
            let value = f64::from_str(parts[1].trim().split(" ").next().unwrap()).unwrap() / 1024.0;
            match name {
                "MemTotal" => {
                    total_memory = value;
                }
                "MemAvailable" => {
                    used_memory = total_memory - value;
                }
                _ => {}
            }
        }

        Ok((total_memory, used_memory))
    }
}