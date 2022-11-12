import datetime
import time

import numpy as np
import requests
from matplotlib import pyplot as plt


def main():
    local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo

    while True:
        time_now = time.time()
        # time_now = 1667652117.2578413

        metric = "cpu_usage"
        # metric = "total_memory"
        # metric = "used_memory"
        # metric = "context_switches"

        # group_by = "core"
        group_by = "host"
        # group_by = None

        response = requests.post(
            "http://localhost:9090/metrics/query/{}".format(metric),
            json={
                "operation": "Average",
                "duration": 10.0,
                "group_by": group_by,
                "start": time_now - 3.0 * 3600.0,
                "end": time_now,
                # "output_filter": {
                #     "Compare": {
                #         "operation": "GreaterThan",
                #         "left": {"Transform": "InputValue"},
                #         "right": {"Transform": {"Value": 0.1}}
                #     }
                # }
            }
        )
        response.raise_for_status()
        response_data = response.json()

        groups = response_data["value"]
        if group_by is None:
            groups = [(None, response_data["value"])]

        for group, values in groups:
            ts, ys = zip(*values)
            # ys = 100.0 * np.array(ys)
            ts = [datetime.datetime.fromtimestamp(t, tz=local_timezone) for t in ts]
            plt.plot(ts, ys, "-o", label=group)

        if group_by is not None:
            plt.legend()

        # plt.ylim([0.0, 100.0])
        plt.show()


if __name__ == "__main__":
    main()
