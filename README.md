# NYT Section Activity Pipeline

## Overview

This project builds a data pipeline that tracks article activity across different sections of the New York Times using the TimesWire API. The pipeline runs on a schedule, collects data over time, and produces an evolving visualization of article activity.

The system is fully automated using:
- AWS EC2 (compute)
- Kubernetes (K3S) for scheduling
- DynamoDB for persistent storage
- S3 for hosting outputs

---

## Problem

Understanding how news activity varies across different domains (e.g., world, business, technology) can provide insight into:
- when certain topics are more active
- how frequently new content is published
- differences in update patterns across sections

The challenge is that the NYT API returns a fixed batch of recent articles, rather than a stream of newly published ones. This makes it difficult to directly measure real-time activity without additional processing.

---

## Approach

Instead of counting the number of articles returned by the API, this pipeline tracks newly observed articles over time.

At each run:
1. The pipeline fetches recent articles for each section
2. Extracts article URLs
3. Compares them against previously stored URLs in DynamoDB
4. Counts only the URLs that have not been seen before
5. Stores the results and updates outputs

This allows the system to approximate:
"How many new articles appeared since the last run?"

---

## Data Pipeline Architecture

### Data Source
- New York Times TimesWire API

### Processing
- Python script containerized with Docker
- Runs as a Kubernetes CronJob

### Storage
- DynamoDB table
  - Partition key: section
  - Sort key: timestamp
  - Stores:
    - article_count
    - article_urls (used for deduplication)

### Output
- plot.png — evolving time series plot
- data.csv — full dataset

The data.csv file contains the full historical dataset with the following fields:
- section: NYT section name
- timestamp: time of data collection (UTC)
- article_count: number of newly observed articles in that run

Both are uploaded to S3 and publicly accessible.

---

## Interpreting the Plot

The plot shows the number of newly observed articles per section over time, not the total number returned by the API.

The first data point for each section typically appears as 20 articles, which corresponds to the default number of items returned by the TimesWire API. Since the database is empty at the start, all articles are considered "new" on the first run.

After this initial point, values drop significantly and fluctuate over time, reflecting only newly observed articles. This results in a more meaningful representation of article activity.

---

## Scheduling

The pipeline is executed using a Kubernetes CronJob.

- Final schedule: every 30 minutes
- Duration: approximately 72 hours
- Result: approximately 140 or more data points per section

---

## Key Challenges

### Fixed API Response Size
The API returns a consistent number of recent articles, which initially led to flat, uninformative results. This was resolved by tracking unique URLs over time.

### Deduplication
Identifying new articles required maintaining state across runs, which was handled using DynamoDB.

### Rate Limiting
Running the pipeline too frequently resulted in API rate limits (HTTP 429). The schedule was adjusted to avoid exceeding limits.

### Deployment Issues
- Image architecture mismatch (ARM vs AMD64)
- Container registry permissions
- Kubernetes image pull errors

These were resolved through rebuilding images for the correct platform and configuring access properly.

---

## Technologies Used

- Python
- Docker
- Kubernetes (K3S)
- AWS EC2
- AWS DynamoDB
- AWS S3
- NYT TimesWire API
- pandas
- matplotlib

---

## How to Run

1. Build and push Docker image
2. Create Kubernetes secret for NYT API key
3. Apply CronJob YAML
4. Monitor jobs and logs using kubectl
5. View outputs via S3

