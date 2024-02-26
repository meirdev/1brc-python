import collections
import concurrent.futures
import mmap
import multiprocessing
import sys
from typing import DefaultDict


class Measurement:
    def __init__(self) -> None:
        self.count = 0
        self.sum = 0.0
        self.min = float("inf")
        self.max = float("-inf")

    def update(self, measurement: float) -> None:
        self.count += 1
        self.sum += measurement

        if self.min > measurement:
            self.min = measurement

        if self.max < measurement:
            self.max = measurement

    def union(self, other: "Measurement") -> "Measurement":
        self.count += other.count
        self.sum += other.sum

        if self.min > other.min:
            self.min = other.min

        if self.max < other.max:
            self.max = other.max

        return self

    def __repr__(self) -> str:
        mean = round(self.sum / self.count, 1) + 0

        return f"{self.min}/{mean}/{self.max}"


def parse_measurements_chunk(
    file: str, length: int, offset: int
) -> DefaultDict[bytes, Measurement]:
    measurements: DefaultDict[bytes, Measurement] = collections.defaultdict(Measurement)

    rem = offset % mmap.ALLOCATIONGRANULARITY

    with open(file, "rb") as fp:
        with mmap.mmap(
            fp.fileno(), length + rem, access=mmap.ACCESS_READ, offset=offset - rem
        ) as mm:
            mm.seek(rem)

            while line := mm.readline():
                idx = line.find(b";")
                measurements[line[:idx]].update(float(line[idx + 1:]))

    return measurements


def get_chunks(file: str, processes: int) -> list[tuple[int, int]]:
    chunks: list[tuple[int, int]] = []

    with open(file, "rb") as fp:
        with mmap.mmap(fp.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            size = mm.size()
            chunk_size = mm.size() // processes

            while True:
                current = mm.tell()

                if current >= size - chunk_size:
                    chunks.append((current, size))
                    break

                mm.seek(chunk_size, 1)

                newline = mm.find(b"\n")

                chunks.append((current, newline))

                mm.seek(newline + 1)

    return chunks


def parse_measurements(file: str) -> str:
    measurements: DefaultDict[bytes, Measurement] = collections.defaultdict(Measurement)

    workers = multiprocessing.cpu_count()

    chunks = get_chunks(file, workers)

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
        futures = []

        for start, offset in chunks:
            futures.append(
                pool.submit(parse_measurements_chunk, file, offset - start, start)
            )

        for future in concurrent.futures.as_completed(futures):
            measurements_: DefaultDict[bytes, Measurement] = future.result()

            for station_name, measurement in measurements_.items():
                measurements[station_name].union(measurement)

    return f"{{{', '.join(f'{i.decode()}={measurements[i]}' for i in sorted(measurements))}}}"


def main() -> None:
    print(parse_measurements(sys.argv[1]))


if __name__ == "__main__":
    main()
