"""Print each event as 'key|json' so the console producer can send it with a key (S01)."""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    import json
    event = json.loads(line)
    print(f"{event['order_id']}|{json.dumps(event, separators=(',', ':'))}")
