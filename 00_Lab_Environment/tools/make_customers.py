"""Create data/customers.csv for the S09 lab.

Includes one deliberate problem: customer C0007 has an older row (address change).
The S09 join must handle it, or revenue is counted twice.
"""
import csv
import random
from pathlib import Path

REGIONS = ["Cairo", "Giza", "Alexandria", "Delta"]
OUT = Path("data/customers.csv")


def build(n: int = 200, seed: int = 7) -> int:
    rng = random.Random(seed)
    rows = []
    for i in range(1, n + 1):
        cid = f"C{i:04d}"
        rows.append({"customer_id": cid, "region": rng.choice(REGIONS),
                     "updated_at": f"2026-10-{rng.randint(1, 28):02d}"})
    rows.append({"customer_id": "C0007", "region": "Giza", "updated_at": "2026-09-01"})  # older row
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "region", "updated_at"])
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


if __name__ == "__main__":
    print(f"wrote {build()} customer rows to {OUT}")
