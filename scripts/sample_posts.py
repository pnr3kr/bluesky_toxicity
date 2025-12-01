import os
import sys
import duckdb
import logging

# Config
DB_PATH = "data/bluesky.duckdb"
LOG_PATH = "logs/sample_posts.log"

SAMPLE_SIZE = 10_000
RANDOM_SEED = 42


# Setup Logging
def setup_logging():
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        filename=LOG_PATH,
        filemode="a",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logging.getLogger().addHandler(console)


logger = logging.getLogger(__name__)


# Main function for sampling posts
def main():
    setup_logging()
    logger.info("Starting sample_posts.py...")

    if not os.path.exists(DB_PATH):
        logger.error("DuckDB database not found. Run clean_firehose.py first.")
        return

    con = duckdb.connect(DB_PATH)

    # Drop + recreate sample_posts table
    con.execute("DROP TABLE IF EXISTS sample_posts;")

    con.execute("""
        CREATE TABLE sample_posts (
            uri TEXT,
            text TEXT,
            created_at TIMESTAMP,
            toxicity DOUBLE
        );
    """)

    logger.info("Sampling 10,000 posts from clean_posts...")

    # Insert sampled posts using hash-based random sampling
    con.execute(f"""
        INSERT INTO sample_posts
        SELECT
            uri,
            text,
            created_at,
            NULL::DOUBLE AS toxicity
        FROM clean_posts
        ORDER BY hash(uri || '{RANDOM_SEED}')
        LIMIT {SAMPLE_SIZE};
    """)

    total = con.execute("SELECT COUNT(*) FROM sample_posts;").fetchone()[0]
    logger.info(f"Sample created successfully. Total sampled posts: {total:,}")

    con.close()
    logger.info("Sampling complete.")


if __name__ == "__main__":
    main()
