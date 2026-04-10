import logging
from datetime import datetime, timezone, timedelta

import boto3
import matplotlib.pyplot as plt
import pandas as pd
import requests
from boto3.dynamodb.conditions import Key

import os

# =========================
# Basic configuration (using kubernetes secret)
# =========================
API_KEY = os.environ["API_KEY"]
S3_BUCKET = os.environ["S3_BUCKET"]

DDB_TABLE = "zyh4up-dp2"
AWS_REGION = "us-east-1"

# Keep this small so the plot stays readable
SECTIONS = ["world", "science", "business", "technology"]


# =========================
# Logging setup
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =========================
# AWS clients
# =========================
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DDB_TABLE)
s3 = boto3.client("s3", region_name=AWS_REGION)


def get_current_timestamp():
    """
    Return the current UTC timestamp in ISO format.

    This is used as the DynamoDB sort key so each run gets a unique time value.
    """
    try:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    except Exception as e:
        logger.error(f"Failed to create timestamp: {e}")
        raise

def get_seen_urls_for_section(section):
    """
    Read all historical rows for one section and collect every article URL
    previously stored for that section.

    Returns a set of URLs that have already been seen.
    """
    try:
        items = read_history_for_section(section)
        seen_urls = set()

        for item in items:
            urls = item.get("article_urls", [])
            if urls:
                for url in urls:
                    seen_urls.add(url)

        logger.info(f"Loaded {len(seen_urls)} seen URLs for section '{section}'")
        return seen_urls

    except Exception as e:
        logger.error(f"Failed to load seen URLs for section '{section}': {e}")
        raise

def fetch_section_count(section):
    """
    Call the NYT TimesWire API for one section and count only articles
    whose URLs have not been seen before for that section.

    This avoids the problem of the API returning a fixed-size recent batch.
    """
    try:
        url = f"https://api.nytimes.com/svc/news/v3/content/all/{section}.json"
        params = {"api-key": API_KEY}

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        current_urls = []
        for item in results:
            article_url = item.get("url")
            if article_url:
                current_urls.append(article_url)

        # remove duplicates while preserving order
        current_urls = list(dict.fromkeys(current_urls))

        seen_urls = get_seen_urls_for_section(section)
        new_urls = [u for u in current_urls if u not in seen_urls]

        record = {
            "section": section,
            "timestamp": get_current_timestamp(),
            "article_count": len(new_urls),
            "article_urls": new_urls
        }

        logger.info(
            f"Fetched {section}: {len(new_urls)} new articles "
            f"({len(current_urls)} URLs returned, {len(seen_urls)} already seen)"
        )

        return record

    except requests.RequestException as e:
        logger.error(f"Request failed for section '{section}': {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while fetching section '{section}': {e}")
        raise


def write_record(record):
    """
    Write one record into DynamoDB.

    The table uses:
    - partition key: section
    - sort key: timestamp
    """
    try:
        table.put_item(Item=record)
        logger.info(
            f"Wrote record to DynamoDB: "
            f"{record['section']} | {record['timestamp']} | {record['article_count']}"
        )
    except Exception as e:
        logger.error(f"Failed to write record to DynamoDB: {e}")
        raise


def read_history_for_section(section):
    """
    Read all saved records for a single section from DynamoDB.

    Because section is the partition key, we query one section at a time.
    """
    try:
        items = []

        response = table.query(
            KeyConditionExpression=Key("section").eq(section)
        )
        items.extend(response.get("Items", []))

        while "LastEvaluatedKey" in response:
            response = table.query(
                KeyConditionExpression=Key("section").eq(section),
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))

        logger.info(f"Loaded {len(items)} historical rows for section '{section}'")
        return items

    except Exception as e:
        logger.error(f"Failed to read history for section '{section}': {e}")
        raise


def read_all_history(sections):
    """
    Read all historical records for all tracked sections and combine them into a DataFrame.

    This historical data is needed so the plot can evolve over time instead of showing
    only the current run.
    """
    try:
        all_items = []

        for section in sections:
            section_items = read_history_for_section(section)
            all_items.extend(section_items)

        if not all_items:
            logger.warning("No historical data found")
            return pd.DataFrame(columns=["section", "timestamp", "article_count"])

        df = pd.DataFrame(all_items)

        if "article_count" in df.columns:
            df["article_count"] = pd.to_numeric(df["article_count"], errors="coerce")

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        df = df.sort_values(["section", "timestamp"]).reset_index(drop=True)

        logger.info(f"Combined historical data into DataFrame with {len(df)} rows")
        return df

    except Exception as e:
        logger.error(f"Failed to build historical DataFrame: {e}")
        raise


def make_plot(df, output_file="plot.png"):
    """
    Create a line plot of article count over time for each section.

    The same file name is reused every run so the S3 website always shows the latest
    version of the evolving plot.
    """
    try:
        plt.figure(figsize=(10, 6))

        if df.empty:
            plt.text(0.5, 0.5, "No data available", ha="center", va="center")
            plt.title("NYT Section Activity Over Time")
            plt.xlabel("Time")
            plt.ylabel("Article Count")
        else:
            for section in sorted(df["section"].unique()):
                section_df = df[df["section"] == section]
                plt.plot(
                    section_df["timestamp"],
                    section_df["article_count"],
                    marker="o",
                    label=section
                )

            plt.title("NYT Section Activity Over Time")
            plt.xlabel("Timestamp (UTC)")
            plt.ylabel("Article Count")
            plt.xticks(rotation=30, ha="right")
            plt.legend()

        plt.tight_layout()
        plt.savefig(output_file, dpi=150)
        plt.close()

        logger.info(f"Saved plot to {output_file}")

    except Exception as e:
        logger.error(f"Failed to create plot: {e}")
        raise


def save_csv(df, output_file="data.csv"):
    """
    Save the full historical dataset to a CSV file.

    This gives you the evolving data file required by the assignment.
    """
    try:
        df_to_save = df.copy()

        if "timestamp" in df_to_save.columns:
            df_to_save["timestamp"] = df_to_save["timestamp"].astype(str)

        df_to_save.to_csv(output_file, index=False)
        logger.info(f"Saved CSV to {output_file}")

    except Exception as e:
        logger.error(f"Failed to save CSV: {e}")
        raise


def upload_to_s3(local_file, s3_key, content_type):
    """
    Upload a local file to the S3 website bucket.

    This is used for both plot.png and data.csv so they stay publicly available
    at stable URLs.
    """
    try:
        with open(local_file, "rb") as f:
            s3.upload_fileobj(
                f,
                S3_BUCKET,
                s3_key,
                ExtraArgs={"ContentType": content_type}
            )

        logger.info(f"Uploaded {local_file} to s3://{S3_BUCKET}/{s3_key}")

    except Exception as e:
        logger.error(f"Failed to upload {local_file} to S3: {e}")
        raise


def main():
    """
    Run one full pipeline cycle.

    Steps:
    1. Fetch current NYT section counts
    2. Write each result to DynamoDB
    3. Read all historical records back
    4. Rebuild plot.png and data.csv
    5. Upload both files to S3
    """
    try:
        logger.info("Starting NYT pipeline run")

        for section in SECTIONS:
            record = fetch_section_count(section)
            write_record(record)

        history_df = read_all_history(SECTIONS)

        make_plot(history_df, "plot.png")
        save_csv(history_df, "data.csv")

        upload_to_s3("plot.png", "plot.png", "image/png")
        upload_to_s3("data.csv", "data.csv", "text/csv")

        logger.info("Pipeline run completed successfully")

    except Exception as e:
        logger.error(f"Pipeline run failed: {e}")
        raise


if __name__ == "__main__":
    main()