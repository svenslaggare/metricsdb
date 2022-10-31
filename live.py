import datetime
import time

import requests
from matplotlib import pyplot as plt

def main():
    local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo

    while True:
        time_now = time.time()
        response = requests.post(
            "http://localhost:9090/metrics/query/cpu_usage",
            json={
                "operation": "Average",
                # "percentile": 50,
                "duration": 10.0,
                "start": time_now - 3.0 * 3600.0,
                "end": time_now,
                # "tags": ["core:cpu1"]
            }
        )
        response.raise_for_status()

        ts, ys = zip(*response.json()["value"])
        ts = [datetime.datetime.fromtimestamp(t, tz=local_timezone) for t in ts]
        plt.plot(ts, ys)
        plt.ylim([0.0, 1.0])
        plt.show()

if __name__ == "__main__":
    main()