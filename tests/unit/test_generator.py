from ingestion.generator import ChaosGenerator
from ingestion.schemas import TransactionPayload
from pydantic import ValidationError


def test_clean_generator_strictly_valid():
    # Setting chaos_probability=0.0 to ensure 100% clean records
    generator = ChaosGenerator(chaos_probability=0.0, seed=42)
    batch = generator.generate_batch(size=50, duplicate_ratio=0.0)

    assert len(batch) == 50
    for record in batch:
        # Must parse through Pydantic contract without raising ValidationError
        payload = TransactionPayload(**record)
        assert payload.amount > 0.0


def test_chaos_injection_generates_corrupted_payloads():
    # Setting chaos_probability=1.0 forces every record to be corrupted
    generator = ChaosGenerator(chaos_probability=1.0, seed=42)
    batch = generator.generate_batch(size=20, duplicate_ratio=0.0)

    assert len(batch) == 20
    failures = 0
    for record in batch:
        try:
            payload = TransactionPayload(**record)
            # Catch business logic failure: negative/zero amounts
            if payload.amount <= 0:
                failures += 1
        except (ValidationError, KeyError):
            failures += 1

    # Every single record should fail schema or contract assertions
    assert failures == 20