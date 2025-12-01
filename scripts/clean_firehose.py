import os
import re
import duckdb
import logging
import sys

# Config
DB_PATH = "data/bluesky.duckdb"
LOG_PATH = "logs/clean_firehose.log"


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
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logging.getLogger().addHandler(console)


logger = logging.getLogger(__name__)


# Text Cleaning Functions
URL_PATTERN = re.compile(r"http\S+|www\.\S+")
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002700-\U000027BF"
    "\U0001F900-\U0001F9FF"
    "\U00002600-\U000026FF"
    "\U00002B00-\U00002BFF"
    "]+",
    flags=re.UNICODE,
)
SYMBOL_PATTERN = re.compile(r"[^A-Za-z0-9\s.,!?\'\"-]")


def clean_text_py(text: str) -> str:
    if text is None:
        return ""

    text = re.sub(URL_PATTERN, " ", text)
    text = re.sub(EMOJI_PATTERN, " ", text)
    text = re.sub(SYMBOL_PATTERN, " ", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


# Main function for cleaning firehose data
def main():
    setup_logging()
    logger.info("Starting clean_firehose.py...")

    if not os.path.exists(DB_PATH):
        logger.error("DuckDB database not found. Run load_firehose.py first.")
        return

    con = duckdb.connect(DB_PATH)
    con.create_function("clean_text_py", clean_text_py)

    con.execute("DROP TABLE IF EXISTS clean_posts;")

    con.execute("""
        CREATE TABLE clean_posts (
            uri TEXT,
            cid TEXT,
            repo TEXT,
            text TEXT,
            created_at TIMESTAMP,
            is_reply BOOLEAN,
            parent_uri TEXT,
            month TEXT,
            day INTEGER,
            hour INTEGER
        );
    """)

    logger.info("Running cleaning + transformation SQL...")

    con.execute("""
        INSERT INTO clean_posts
        SELECT
            uri,
            cid,
            repo,
            cleaned AS text,
            created_at,
            (reply_parent_uri IS NOT NULL) AS is_reply,
            reply_parent_uri AS parent_uri,
            strftime('%Y-%m', created_at) AS month,
            EXTRACT(day FROM created_at) AS day,
            EXTRACT(hour FROM created_at) AS hour
        FROM (
            SELECT
                uri,
                cid,
                repo,
                clean_text_py(text) AS cleaned,
                created_at,
                langs,
                reply_root_uri,
                reply_parent_uri
            FROM raw_firehose
            WHERE
                langs IS NOT NULL
                AND array_length(langs) > 0
                AND langs[1] = 'en'
                AND text IS NOT NULL
        )
        WHERE
            length(regexp_replace(cleaned, '[^A-Za-z0-9]', '', 'g')) >= 3
            AND created_at IS NOT NULL;
    """)

    logger.info("clean_posts table created successfully.")
    logger.info(f"Total clean posts: {con.execute('SELECT COUNT(*) FROM clean_posts').fetchone()[0]:,}")

    con.close()
    logger.info("Cleaning complete.")


if __name__ == "__main__":
    main()
