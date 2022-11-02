import datetime
import time

import numpy as np
import requests
from matplotlib import pyplot as plt

def main():
    local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    # cores = ["cpu{}".format(n) for n in range(16)]
    cores = [None]

    while True:
        time_now = time.time()

        for core in cores:
            tags = None
            if core is not None:
                tags = ["core:" + core]

            response = requests.post(
                "http://localhost:9090/metrics/query/cpu_usage",
                json={
                    "operation": "Average",
                    # "percentile": 50,
                    "duration": 10.0,
                    "start": time_now - 3.0 * 3600.0,
                    "end": time_now,
                    "tags": tags
                }
            )
            response.raise_for_status()
            response_data = response.json()

            ts, ys = zip(*response_data["value"])
            ys = 100.0 * np.array(ys)
            ts = [datetime.datetime.fromtimestamp(t, tz=local_timezone) for t in ts]
            plt.plot(ts, ys, "-o", label=core)

        if len(cores) > 1:
            plt.legend()

        plt.ylim([0.0, 100.0])
        plt.show()

if __name__ == "__main__":
    main()