"""Shared helpers for the labs. Pure Python, no broker needed to import or test this file."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

# Host addresses (the trainee's onsite Windows desktop, Ubuntu terminal in WSL 2). Inside containers use kafka1:9092 etc.
BOOTSTRAP = os.environ.get("BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

TOPIC_ORDERS = "orders.events.v1"          # raw order events (keyed by order_id)
TOPIC_DLT = "orders.events.v1.dlt"         # dead-letter topic for poison records
TOPIC_KPI = "orders.kpi.minute.v1"         # curated output from the capstone pipeline

VALID_TYPES = {"OrderPlaced", "PaymentAccepted", "Refunded"}


@dataclass(frozen=True)
class Verdict:
    ok: bool
    reason: str = ""


def validate_event(event: dict) -> Verdict:
    """Data-quality rules used by the producer checks (S03), the consumer (S04) and Spark (S08)."""
    required = ("event_id", "order_id", "customer_id", "event_type", "event_time")
    for field in required:
        if not event.get(field):
            return Verdict(False, f"missing {field}")
    if event["event_type"] not in VALID_TYPES:
        return Verdict(False, f"unknown event_type {event['event_type']}")
    amount = event.get("amount")
    if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
        return Verdict(False, "amount must be a positive number")
    return Verdict(True)


def dedupe(events: list[dict]) -> tuple[list[dict], int]:
    """Keep the first event for each event_id. Returns (unique events, number removed)."""
    seen: set[str] = set()
    unique: list[dict] = []
    for event in events:
        if event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        unique.append(event)
    return unique, len(events) - len(unique)


def key_bytes(order_id: str) -> bytes:
    """Kafka keys are bytes. Using order_id keeps every event of one order in one partition."""
    return order_id.encode("utf-8")


def encode_json(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def decode_json(payload: bytes) -> dict:
    return json.loads(payload.decode("utf-8"))
