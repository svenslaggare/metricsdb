import requests

BASE_URL = "http://localhost:9090"

def main():
    # create_metric("cpu_usage", "gauge")
    # create_metric("total_memory", "gauge")
    # create_metric("used_memory", "gauge")
    create_metric("context_switches", "count")

def create_metric(name, type):
    requests.post(
        "{}/metrics/{}".format(BASE_URL, type),
        json={
            "name": name
        }
    ).raise_for_status()

    requests.post(
        "{}/metrics/auto-primary-tag/{}".format(BASE_URL, name),
        json={
            "key": "host"
        }
    ).raise_for_status()

if __name__ == "__main__":
    main()