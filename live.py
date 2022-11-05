import datetime
import time

import numpy as np
import requests
from matplotlib import pyplot as plt

def main():
    local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo

    while True:
        time_now = time.time()
        # time_now = 1667336006.3926258

        # group_by = "core"
        group_by = "host"
        # group_by = None

        response = requests.post(
            "http://localhost:9090/metrics/query/cpu_usage",
            json={
                "operation": "Average",
                "duration": 10.0,
                "group_by": group_by,
                "start": time_now - 3.0 * 3600.0,
                "end": time_now,
            }
        )
        response.raise_for_status()
        response_data = response.json()

        groups = response_data["value"]
        if group_by is None:
            groups = [(None, response_data["value"])]

        for group, values in groups:
            ts, ys = zip(*values)
            ys = 100.0 * np.array(ys)
            ts = [datetime.datetime.fromtimestamp(t, tz=local_timezone) for t in ts]
            plt.plot(ts, ys, "-o", label=group)

        if group_by is not None:
            plt.legend()

        plt.ylim([0.0, 100.0])
        plt.show()

if __name__ == "__main__":
    main()