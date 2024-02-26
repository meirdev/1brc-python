import collections
import sys
from typing import BinaryIO, DefaultDict


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


def parse_measurements(file: BinaryIO) -> str:
    measurements: DefaultDict[bytes, Measurement] = collections.defaultdict(Measurement)

    length = 0

    while chunk := file.read(2_000_000):  # 2 MB
        for line in chunk.splitlines(True):
            if len(line) > 1 and line.endswith(b"\n"):
                length += len(line)
                station_name, measurement = line.split(b";")
                measurements[station_name].update(float(measurement))
            else:
                file.seek(length)

    return f"{{{', '.join(f'{i.decode()}={measurements[i]}' for i in sorted(measurements))}}}"


def main() -> None:
    with open(sys.argv[1], "rb") as fp:
        print(parse_measurements(fp))


if __name__ == "__main__":
    main()
