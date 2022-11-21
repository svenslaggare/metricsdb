import datetime
import time

import numpy as np
import requests
from matplotlib import pyplot as plt

def main():
    local_timezone = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo
    base_url = "http://localhost:9090"

    while True:
        # time_now = time.time()
        time_now = 1668874032.213049

        metric = "cpu_usage"
        # metric = "total_memory"
        # metric = "used_memory"
        # metric = "context_switches"

        # group_by = "core"
        group_by = "host"
        # group_by = None

        response = requests.post(
            "{}/metrics/query".format(base_url),
            json={
                "time_range": {
                    "start": time_now - 3.0 * 3600.0,
                    "end": time_now
                },
                "duration": 10.0,
                "expression": {
                    "Average": {
                        "metric": metric,
                        "query": {
                            "group_by": group_by,
                            # "output_filter": {
                            #     "Compare": {
                            #         "operation": "GreaterThan",
                            #         "left": {"Value": "InputValue"},
                            #         "right": {"Value": {"Value": 0.1}}
                            #     }
                            # }
                        }
                    }
                }
            }
        )

        # response = requests.post(
        #     "{}/metrics/query".format(base_url),
        #     json={
        #         "time_range": {
        #             "start": time_now - 3.0 * 3600.0,
        #             "end": time_now
        #         },
        #         "duration": 10.0,
        #         "expression": {
        #             "Arithmetic": {
        #                 "operation": "Multiply",
        #                 "left": {
        #                     "Value": 100.0
        #                 },
        #                 "right": {
        #                     "Arithmetic": {
        #                         "operation": "Divide",
        #                         "left": {
        #                             "Average": {
        #                                 "metric": "used_memory",
        #                                 "query": {}
        #                             }
        #                         },
        #                         "right": {
        #                             "Average": {
        #                                 "metric": "total_memory",
        #                                 "query": {}
        #                             }
        #                         }
        #                     }
        #                 }
        #             }
        #         }
        #     }
        # )

        response.raise_for_status()
        response_data = response.json()

        groups = response_data["value"]
        if group_by is None:
            groups = [(None, response_data["value"])]

        for group, values in groups:
            ts, ys = zip(*values)
            ts = [datetime.datetime.fromtimestamp(t, tz=local_timezone) for t in ts]
            plt.plot(ts, ys, "-o", label=group)

        if group_by is not None:
            plt.legend()

        # plt.ylim([0.0, 100.0])
        plt.show()


if __name__ == "__main__":
    main()
