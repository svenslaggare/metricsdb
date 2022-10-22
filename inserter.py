import json
import requests

def main():
    with open("output.json", "r") as f:
        output = json.load(f)

    for indices in chunks(range(len(output["times"])), 1000):
        entries = [{"time": output["times"][index], "value": output["values"][index], "tags": []} for index in indices]
        response = requests.put("http://localhost:9090/metrics/gauge/cpu", json=entries)
        response.raise_for_status()

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    main()