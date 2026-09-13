import logging
import os
import time
from ingestion.generator import ChaosGenerator
from ingestion.s3_transport import S3TransportEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] IngestionRunner: %(message)s",
)
logger = logging.getLogger("IngestionRunner")


def run_continuous_ingestion(
    batch_size: int = 100,
    interval_seconds: int = 15,
    chaos_probability: float = 0.05,
):
    """Continuously generates transaction batches and streams them to S3 Bronze."""
    logger.info("Initializing Chaos Generator and S3 Transport Engine...")
    generator = ChaosGenerator(chaos_probability=chaos_probability)
    transport = S3TransportEngine()

    logger.info(
        f"Starting ingestion loop: {batch_size} events every {interval_seconds}s "
        f"(Chaos rate: {chaos_probability * 100}%)..."
    )

    try:
        while True:
            # 1. Generate realistic data with intentional anomalies
            batch = generator.generate_batch(
                size=batch_size, duplicate_ratio=0.02
            )

            # 2. Resiliently stream batch to AWS S3 Bronze partition
            uploaded_key = transport.upload_batch(batch)
            logger.info(f"Batch committed -> s3://{transport.bucket_name}/{uploaded_key}")

            # 3. Rest before next batch emission
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Ingestion service terminated gracefully by user.")


if __name__ == "__main__":
    # Allow tweaking speeds via environment variables if desired
    BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "50"))
    INTERVAL = int(os.getenv("INGEST_INTERVAL_SECONDS", "10"))
    CHAOS_RATE = float(os.getenv("INGEST_CHAOS_RATE", "0.05"))

    run_continuous_ingestion(
        batch_size=BATCH_SIZE,
        interval_seconds=INTERVAL,
        chaos_probability=CHAOS_RATE,
    )