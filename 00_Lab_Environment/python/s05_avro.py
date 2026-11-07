"""S05 lab: produce and consume Avro events through Schema Registry.

Subcommands:
  produce --schema schemas/order_event_v1.avsc --count 20
  consume --group contract-check --max-messages 20
  try-evolve --schema schemas/order_event_v3_breaking.avsc   (shows the registry rejecting a change)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from python.common import BOOTSTRAP, SCHEMA_REGISTRY_URL, key_bytes
from tools.events import generate

TOPIC_AVRO = "orders.avro.v1"        # a separate topic: never mix JSON and Avro on one topic
SUBJECT = f"{TOPIC_AVRO}-value"      # TopicNameStrategy: one subject per topic value


def _client() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def _to_dict(event: dict, ctx) -> dict:
    """Map a generated event onto the v1 contract. Fields not in the contract are dropped."""
    return {
        "event_id": event["event_id"], "order_id": event["order_id"],
        "customer_id": event["customer_id"], "store_id": event["store_id"],
        "event_type": event["event_type"], "amount": event["amount"],
        "event_time": event["event_time"],
    }


def produce(schema_path: str, count: int, seed: int) -> int:
    schema_str = Path(schema_path).read_text(encoding="utf-8")
    serializer = AvroSerializer(_client(), schema_str, to_dict=_to_dict)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})
    sent = 0
    for event in generate(count, seed):
        if event["amount"] is None:
            continue                          # the contract requires a value for this demo
        value = serializer(event, SerializationContext(TOPIC_AVRO, MessageField.VALUE))
        producer.produce(TOPIC_AVRO, key=key_bytes(event["order_id"]), value=value)
        sent += 1
        producer.poll(0)
    producer.flush(60)
    print(f"produced {sent} Avro events, subject {SUBJECT}")
    return 0


def consume(group: str, max_messages: int) -> int:
    client = _client()
    deserializer = AvroDeserializer(client)
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([TOPIC_AVRO])
    seen = 0
    idle = 0                                   # stop after 30 empty polls (about 30 s) instead of waiting forever
    try:
        while seen < max_messages and idle < 30:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                idle += 1
                continue
            idle = 0
            event = deserializer(msg.value(), SerializationContext(TOPIC_AVRO, MessageField.VALUE))
            seen += 1
            if seen <= 3:
                print(event)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
    print(f"consumed {seen} events")
    return 0


def try_evolve(schema_path: str) -> int:
    """Register a candidate schema. The registry checks compatibility before accepting it."""
    client = _client()
    candidate = Schema(Path(schema_path).read_text(encoding="utf-8"), "AVRO")
    try:
        schema_id = client.register_schema(SUBJECT, candidate)
        print(f"accepted, schema id {schema_id}")
        return 0
    except Exception as error:               # the registry returns HTTP 409 for incompatible schemas
        print(f"rejected by Schema Registry: {error}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S05 Avro lab")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("produce"); p.add_argument("--schema", required=True)
    p.add_argument("--count", type=int, default=20); p.add_argument("--seed", type=int, default=42)
    c = sub.add_parser("consume"); c.add_argument("--group", default="contract-check")
    c.add_argument("--max-messages", type=int, default=20)
    e = sub.add_parser("try-evolve"); e.add_argument("--schema", required=True)
    args = parser.parse_args(argv)
    if args.cmd == "produce":
        return produce(args.schema, args.count, args.seed)
    if args.cmd == "consume":
        return consume(args.group, args.max_messages)
    return try_evolve(args.schema)


if __name__ == "__main__":
    raise SystemExit(main())
