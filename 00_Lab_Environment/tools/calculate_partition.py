#!/usr/bin/env python3
"""Calculate Kafka MurmurHash2 partition assignment for a given key and partition count.
Matches Kafka's Java DefaultPartitioner / BuiltInPartitioner implementation.

Usage:
  python tools/calculate_partition.py KEY_A 3
"""
from __future__ import annotations

import sys


def murmur2_kafka(data: bytes, seed: int = 0x9747B28C) -> int:
    """Exact replica of Kafka's org.apache.kafka.common.utils.Utils.murmur2."""
    m = 0x5BD1E995
    r = 24
    length = len(data)
    h = seed ^ length
    length4 = length >> 2

    for i in range(length4):
        i4 = i << 2
        k = (data[i4] & 0xFF) | ((data[i4 + 1] & 0xFF) << 8) | ((data[i4 + 2] & 0xFF) << 16) | ((data[i4 + 3] & 0xFF) << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= (k >> r)
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k

    extra = length & 3
    if extra == 3:
        h ^= (data[(length & ~3) + 2] & 0xFF) << 16
    if extra >= 2:
        h ^= (data[(length & ~3) + 1] & 0xFF) << 8
    if extra >= 1:
        h ^= (data[length & ~3] & 0xFF)
        h = (h * m) & 0xFFFFFFFF

    h ^= (h >> 13)
    h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 15)
    return h & 0x7FFFFFFF


def to_positive(x: int) -> int:
    return x & 0x7FFFFFFF


def get_partition(key_str: str, num_partitions: int = 3) -> int:
    raw_hash = murmur2_kafka(key_str.encode("utf-8"))
    return to_positive(raw_hash) % num_partitions


if __name__ == "__main__":
    key = sys.argv[1] if len(sys.argv) > 1 else "KEY_A"
    num_p = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    partition = get_partition(key, num_p)
    print(f"Key: '{key}' | Num Partitions: {num_p} | Computed MurmurHash2 Partition: {partition}")
