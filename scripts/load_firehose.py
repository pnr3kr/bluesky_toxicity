import os
import sys
import time
import logging
import duckdb
from datetime import datetime

from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, CAR

# Configuration
DB_PATH = "data/bluesky.duckdb"
LOG_PATH = "logs/load_firehose.log"

TARGET_POSTS = 100_000
PRINT_EVERY = 1000

total_posts = 0
con = None
client = None


# Setup logging
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


# Setup DuckDB and raw_firehose table
def init_db():
    """Reset the raw_firehose table every time for clean ingestion."""
    global con

    os.makedirs("data", exist_ok=True)
    con = duckdb.connect(DB_PATH)

    con.execute("DROP TABLE IF EXISTS raw_firehose;")

    con.execute("""
        CREATE TABLE raw_firehose (
            uri TEXT,
            cid TEXT,
            repo TEXT,
            text TEXT,
            created_at TIMESTAMP,
            langs TEXT[],
            reply_root_uri TEXT,
            reply_parent_uri TEXT
        );
    """)

    logger.info("DuckDB connected — raw_firehose table reset and ready.")


# Message handler for Firehose
def on_message_handler(message):
    global total_posts, client

    try:
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return
        if not commit.blocks:
            return

        car = CAR.from_bytes(commit.blocks)

        for op in commit.ops:
            if op.action != "create" or not op.cid:
                continue

            data = car.blocks.get(op.cid)
            if not isinstance(data, dict):
                continue
            if data.get("$type") != "app.bsky.feed.post":
                continue

            uri = f"at://{commit.repo}/{op.path}"
            cid = str(op.cid)
            repo = commit.repo
            text = data.get("text", "")

            created_at = data.get("createdAt")
            if created_at:
                try:
                    created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                except:
                    created_at = None

            langs = data.get("langs") or []

            reply = data.get("reply") or {}
            reply_root_uri = (reply.get("root") or {}).get("uri")
            reply_parent_uri = (reply.get("parent") or {}).get("uri")

            # Insert directly into DuckDB
            con.execute("""
                INSERT INTO raw_firehose
                (uri, cid, repo, text, created_at, langs, reply_root_uri, reply_parent_uri)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, [uri, cid, repo, text, created_at, langs, reply_root_uri, reply_parent_uri])

            total_posts += 1

            if total_posts % PRINT_EVERY == 0:
                logger.info(f">>> {total_posts:,} posts ingested...")

            if total_posts >= TARGET_POSTS:
                logger.info(f"Reached target {TARGET_POSTS:,}. Stopping...")
                client.stop()
                return

    except Exception as e:
        logger.exception(f"Error in handler: {e}")


# Main function to start ingestion
def main():
    global client

    setup_logging()
    init_db()

    logger.info("Starting Bluesky Firehose ingestion...")

    while total_posts < TARGET_POSTS:
        try:
            client = FirehoseSubscribeReposClient()
            client.start(on_message_handler)

        except Exception as e:
            logger.error(f"Firehose disconnected: {e}")
            logger.info("Reconnecting in 2 seconds...")
            time.sleep(2)
            continue

    logger.info(f"Ingestion complete. Total posts: {total_posts:,}")


if __name__ == "__main__":
    main()
