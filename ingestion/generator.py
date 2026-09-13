import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List


class ChaosGenerator:
    """Simulates realistic financial ledger transaction streams

    with controlled fault injection (chaos anomalies).
    """

    CURRENCIES = ["INR", "USD", "EUR", "GBP"]
    PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING"]
    STATUSES = ["SUCCESS", "FAILED", "PENDING"]

    def __init__(self, chaos_probability: float = 0.05, seed: int = None):
        if seed is not None:
            random.seed(seed)
        self.chaos_probability = chaos_probability

    def _generate_clean_record(self) -> Dict[str, Any]:
        """Generates a strictly valid financial transaction payload."""
        return {
            "transaction_id": str(uuid.uuid4()),
            "idempotency_key": f"idemp_{uuid.uuid4().hex[:16]}",
            "user_id": f"usr_{random.randint(1000, 9999)}",
            "merchant_id": f"merch_{random.randint(100, 499)}",
            "amount": round(random.uniform(5.0, 5000.0), 2),
            "currency": random.choice(self.CURRENCIES),
            "payment_method": random.choice(self.PAYMENT_METHODS),
            "status": random.choice(self.STATUSES),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_ip": f"192.168.1.{random.randint(2, 254)}",
        }

    def _inject_chaos(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Mutates a valid payload with an intentional real-world corruption."""
        corruption_type = random.choice(
            [
                "negative_amount",
                "invalid_currency",
                "missing_id",
                "corrupt_timestamp",
                "bad_status",
            ]
        )

        corrupted = payload.copy()

        if corruption_type == "negative_amount":
            corrupted["amount"] = -1 * abs(corrupted["amount"])
        elif corruption_type == "invalid_currency":
            corrupted["currency"] = "CRYPTO_ETH"
        elif corruption_type == "missing_id":
            corrupted.pop("transaction_id", None)
        elif corruption_type == "corrupt_timestamp":
            corrupted["event_timestamp"] = "INVALID_TIMESTAMP_FORMAT_2026"
        elif corruption_type == "bad_status":
            corrupted["status"] = "CHARGEBACK_PROCESSING"

        return corrupted

    def generate_batch(
        self, size: int = 100, duplicate_ratio: float = 0.02
    ) -> List[Dict[str, Any]]:
        """Generates a batch of transactions with realistic chaos and replays."""
        batch: List[Dict[str, Any]] = []

        for _ in range(size):
            record = self._generate_clean_record()
            if random.random() < self.chaos_probability:
                record = self._inject_chaos(record)
            batch.append(record)

        # Inject controlled transaction duplicates (replay simulation)
        num_duplicates = int(size * duplicate_ratio)
        if num_duplicates > 0 and len(batch) > 0:
            duplicates_to_clone = random.sample(
                batch, min(num_duplicates, len(batch))
            )
            for item in duplicates_to_clone:
                replayed = item.copy()
                # Same transaction_id and idempotency_key, but slightly later arrival
                replayed["event_timestamp"] = datetime.now(
                    timezone.utc
                ).isoformat()
                batch.append(replayed)

        random.shuffle(batch)
        return batch