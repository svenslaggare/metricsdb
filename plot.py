import json

from matplotlib import pyplot as plt


def main():
    with open("window.json", "r") as f:
        windows = json.load(f)

    ts, ys = zip(*windows)
    plt.plot(ts, ys)
    plt.show()

if __name__ == "__main__":
    main()