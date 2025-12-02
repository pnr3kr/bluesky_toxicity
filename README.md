# Bluesky Toxicity Analysis

This project analyzes toxicity levels in Bluesky posts using a custom data pipeline, text processing workflows, and machine-learning models. The goal is to understand patterns of toxic language online and explore what features (such as message length or reply status) relate to toxicity.

What I Did
1. Data Ingestion

Used the Bluesky Firehose API (atproto) to stream and collect posts.

Stored 100,000 raw posts into a local DuckDB database (bluesky.duckdb).

Built a reproducible loading script to re-run ingestion safely.

2. Toxicity Scoring

Sampled 10,000 posts for analysis.

Used the Google Perspective API to assign each post a continuous toxicity score between 0 and 1.

Stored scores back into DuckDB for analysis.

3. Text Processing

Tokenized post text and built word frequency counts.

Separated:

Highly toxic posts (toxicity ≥ 0.7)

Least toxic posts (toxicity < 0.1)

Generated word clouds and bar plots showing the most common words in each group.

4. Data Analysis & Visualizations

Created multiple plots, including:

Toxicity score distribution (10k posts)

Toxic vs. non-toxic post counts

Toxicity in replies vs. non-replies

Scatterplot of message length vs. toxicity

Most common words in high-toxicity posts

Most common words in low-toxicity posts
All plots are saved to the plots/ directory.

5. Machine Learning

Built a regression model to predict toxicity from text using:

TF-IDF vectorization

Linear Regression

Evaluated using RMSE and R² (model reached ~0.55 R²).

Explored whether simple linguistic patterns can predict toxicity.
