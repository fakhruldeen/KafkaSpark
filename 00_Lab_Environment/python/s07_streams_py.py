"""S07 lab: the Kafka Streams ideas (topology, KTable, state store, changelog) in plain Python.

Kafka Streams itself is a Java library. Trainees who write Python learn the same model here, and
Java learners read the reference in lab S07 section 9, which maps each class below to its Kafka Streams API.

Pure Python. Tested offline in tests/test_s07_streams_py.py. A live version writes the changelog to a Kafka topic.
"""
from __future__ import annotations

from typing import Callable, Iterable


class KeyedAggregate:
    """A KTable-like state store: the latest aggregate per key, plus a changelog of every update."""

    def __init__(self, fold: Callable[[float, float], float], initial: float = 0.0):
        self._fold = fold
        self._initial = initial
        self.state: dict[str, float] = {}
        self.changelog: list[tuple[str, float]] = []  # (key, new value); a real app writes these to a topic

    def update(self, key: str, value: float) -> float:
        new_value = self._fold(self.state.get(key, self._initial), value)
        self.state[key] = new_value
        self.changelog.append((key, new_value))
        return new_value

    @classmethod
    def restore(cls, changelog: Iterable[tuple[str, float]], fold, initial: float = 0.0) -> "KeyedAggregate":
        """Rebuild the state store from the changelog, as Kafka Streams does after a crash."""
        store = cls(fold, initial)
        for key, value in changelog:
            store.state[key] = value
        store.changelog = list(changelog)
        return store


def run_topology(events: Iterable[dict], store: KeyedAggregate, min_amount: float = 0.0) -> int:
    """Topology: filter (drop small orders) -> map (key by customer) -> aggregate (sum of amounts).

    Returns the number of events that passed the filter.
    """
    passed = 0
    for event in events:
        if event.get("amount", 0) < min_amount:  # filter
            continue
        store.update(event["customer_id"], float(event["amount"]))  # map + aggregate
        passed += 1
    return passed
