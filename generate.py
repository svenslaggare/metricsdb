import datetime
import json

import numpy as np
import matplotlib.pyplot as plt

def main():
    np.random.seed(1337)

    start_time = datetime.datetime(2022, 6, 1, 12, 0, 0).timestamp()
    duration = 7.0 * 24.0 * 3600.0
    event_frequency = 20.0

    times = []
    values = []

    current_time = start_time
    while current_time < start_time + duration:
        times.append(datetime.datetime.fromtimestamp(current_time))
        value = 0.5 + 0.3 * (1.0 + np.sin(0.001 * current_time)) * 0.5
        value += np.random.normal(scale=0.005)
        values.append(np.clip(value, 0.0, 1.0))

        delta = np.random.exponential(1.0 / event_frequency)
        current_time += delta

    with open("output.json", "w") as f:
        json.dump({
            "times": [current.timestamp() for current in times],
            "values": values
        }, f)

    print(len(times), len(times) / duration)
    plt.plot(times, values)
    plt.ylim([0.0, 1.0])
    plt.show()

if __name__ == "__main__":
    main()