use std::str::FromStr;
use std::time::SystemTime;
use fnv::FnvHashMap;
use serde_json::json;

#[tokio::main]
async fn main() {
    let mut cpu_usage_collector = CpuUsageCollector::new();

    // loop {
    //     for (core_name, cpu_usage) in cpu_usage_collector.collect().unwrap() {
    //         println!("{}: {}", core_name, 100.0 * cpu_usage);
    //     }
    //
    //     println!();
    //     std::thread::sleep(std::time::Duration::from_secs_f64(1.0));
    // }

    let client = reqwest::Client::new();
    loop {
        let cpu_usage = cpu_usage_collector.collect().unwrap();
        if !cpu_usage.is_empty() {
            let time_now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64();
            let cpu_usage_json = json!(
                cpu_usage
                    .iter()
                    .map(|(core_name, cpu_usage)| json!({ "time": time_now, "tags": vec![format!("core:{}", core_name)], "value": cpu_usage }))
                    .collect::<Vec<_>>()
            );

            println!("{}", cpu_usage_json);

            let response = client.put("http://localhost:9090/metrics/gauge/cpu_usage")
                .json(&cpu_usage_json)
                .send()
                .await
                .unwrap();
            let response_status = response.status();
            let response_data = response.bytes().await.unwrap();
            let response_data = std::str::from_utf8(response_data.as_ref()).unwrap();
            println!("{}: {}", response_status, response_data);
        }

        std::thread::sleep(std::time::Duration::from_secs_f64(1.0));
    }
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