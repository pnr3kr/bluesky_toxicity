import os
import time
import sys
import logging
import duckdb
import requests

# Config
DB_PATH = "data/bluesky.duckdb"
LOG_PATH = "logs/score_toxicity.log"

PERSPECTIVE_API_KEY = os.getenv("PERSPECTIVE_API_KEY")

API_URL = "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze"

REQUESTS_PER_SECOND = 1
SLEEP_TIME = 1.05  # slightly above 1 second

MAX_RETRIES = 5


# Setting up logging
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
    console.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    console.setLevel(logging.INFO)
    logging.getLogger().addHandler(console)


logger = logging.getLogger(__name__)


# Score text toxicity
def score_text(text: str) -> float:
    """Send text to Perspective API and return toxicity score."""

    if text is None or len(text.strip()) == 0:
        return 0.0

    payload = {
        "comment": {"text": text},
        "languages": ["en"],
        "requestedAttributes": {"TOXICITY": {}},
        "doNotStore": True,
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                API_URL,
                params={"key": PERSPECTIVE_API_KEY},
                json=payload,
                timeout=30
            )

            # Rate limit hit
            if response.status_code == 429:
                wait = 2 ** attempt
                logger.warning(f"429 rate limit — sleeping {wait}s then retrying.")
                time.sleep(wait)
                continue

            response.raise_for_status()
            data = response.json()

            return data["attributeScores"]["TOXICITY"]["summaryScore"]["value"]

        except Exception as e:
            wait = 2 ** attempt
            logger.warning(f"Error scoring text: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    logger.error("Max retries exceeded. Returning toxicity = 0.0")
    return 0.0


# Main function for scoring toxicity
def main():
    setup_logging()

    if not PERSPECTIVE_API_KEY:
        logger.error("No PERSPECTIVE_API_KEY found in environment.")
        return

    logger.info("Starting toxicity scoring...")

    if not os.path.exists(DB_PATH):
        logger.error("DuckDB not found. Run previous stages first.")
        return

    con = duckdb.connect(DB_PATH)

    # Fetch posts that haven't been scored yet
    rows = con.execute("""
        SELECT uri, text
        FROM sample_posts
        WHERE toxicity IS NULL;
    """).fetchall()

    total_to_score = len(rows)
    logger.info(f"Found {total_to_score:,} posts to score.")

    count = 0

    for uri, text in rows:

        toxicity = score_text(text)

        # Update DuckDB
        con.execute(
            "UPDATE sample_posts SET toxicity = ? WHERE uri = ?;",
            [toxicity, uri]
        )

        count += 1

        # Logging
        if count % 100 == 0:
            logger.info(f">>> {count:,} / {total_to_score:,} scored.")

        # Respect API rate limit of 60 RPM
        time.sleep(SLEEP_TIME)

    con.close()
    logger.info("All posts scored successfully.")
    logger.info("Scoring complete.")


if __name__ == "__main__":
    main()