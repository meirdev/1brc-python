import collections
import sys
from typing import TextIO


class Measurement:
    def __init__(self) -> None:
        self.count = 0
        self.sum = 0.0
        self.min = float("inf")
        self.max = float("-inf")

    def update(self, measurement: float) -> None:
        self.count += 1
        self.sum += measurement
        self.min = min(self.min, measurement)
        self.max = max(self.max, measurement)

    def __repr__(self) -> str:
        mean = round(self.sum / self.count, 1) + 0

        return f"{self.min}/{mean}/{self.max}"


def parse_measurements(file: TextIO) -> str:
    measurements = collections.defaultdict(Measurement)

    for line in file:
        station_name, measurement = line.split(";")
        measurements[station_name].update(float(measurement))

    return f"{{{', '.join(f'{i}={measurements[i]}' for i in sorted(measurements))}}}"


def main() -> None:
    with open(sys.argv[1]) as fp:
        print(parse_measurements(fp))


if __name__ == "__main__":
    main()
