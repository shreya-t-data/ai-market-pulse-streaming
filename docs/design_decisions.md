# Design Decisions - AI/Tech Market Pulse Streaming Pipeline

Architecture Decision Record (ADR) format: each entry states the decision, the context, and the trade-offs accepted.

## 1. Kafka over direct API polling from Spark

**Decision:** Ingest via Kafka producers, not by having Spark poll Finnhub directly.

**Why:** Decouples ingestion from processing - producers can keep running independently of whether Spark jobs are up, and Kafka retains messages so a restarted Spark job doesn't lose data mid-stream. Also allows multiple independent consumers (OHLC job, sentiment job, correlation job) to read the same data withour re-polling the API multiple times.

## 2. Separate topics for trades vs. news

**Decision:** 'stock-trades' and 'market-news' as to independent topics, not one merged topic.

**Why:** Different data shapes, wildly different volumes (trades: many/sec; news: a few/hour), and different consumers may want one without the other. Joining downstream in Spark is the standard pattern over merging upstream.

## 3. Spark Structured Streaming over a plain Kafka consumer loop

**Decision:** Use Spark's declarative streaming API rather than hand-rolled Python consumers.

**Why:** Windowed aggregation (OHLC), watermarking, and stream-stream joins would require significant management in a plain consumer loop. Spark expresses this declaratively, the same way you'd write batch SQL, while handling state and fault tolerance underneath.

## 4. VADER over an LLM/finance-tuned model for sentiment

**Decision:** Use VADER (lexicon-based) for sentiment scoring rather than an LLM API or finance-tuned model like FinBERT.

**Why:** VADER is free, runs locally with no external calls, and is fast enough for a tight streaming loop. Known limitation: VADER's lexicon isn't finance-tuned, so financial jargon ("beats estimates," "plummets") is sometimes scored as neutral when a human would read it as strongly directional. FinBERT would be the production-grade upgrade, at the cost of a heavier model dependency.

## 5. Postgres for the gold layer instead of Iceberg/Delta lake or a dedicated time-series DB

**Decision:** Use Postgres (existing DB) rather than a lakehouse table format or time-series database.

**Why:** This project runs on a single local Postgres instance with no cloud storage or multi-engine access requirement - Iceberg/Delta Lake's real advantages (time travel, safe concurrent multi-engine writes, cloud-scale file management) don't apply at this scale, and would add meaningful setup complexity (catalog service, storage layer) without a corresponding benefit here. Noted as a futute improvement if this pipeline needed to scale to multi-engine or cloud-scale use.

## 6. foreachBatch + staging table + upsert, instead of Delta/Iceberg MERGE

**Decision:** Land each micro-batch into a staging table, then run a manual 'INSERT ... ON CONFLICT' to upsert into the real table.

**Why:** Spark's native JDBC sink only supports append/overwrite, with no built-in upsert. This is the standard workaround for idempotent writes to a plain relational sink, protecting against duplicate rows if Spark reprocesses a batch after a restart/failure.

## 7. Simplified stream-stream join (temporal proximity, not price-change-vs-5-min-prior)

**Decision:** Correlate trades with news based on temporal proximity (news within 15 minutes of a trade) rather than computing "price change vs. 5 minutes prior," which is the original design considered.

**Why:** Computing a true trailing price-change comparison live would require a self-join or stateful tracking on the trades stream - meaningfully more complex for the value added in portfolio-scope project. The simplified version still demonstrates the core, rarer skill (dual-watermarked stream-stream join) without over-engineering under time constraints. Noted as future improvement.

## 8. Tow independent alert types: news-correlated vs. statistical anomaly

**Decision:** Separate alerting into two paths - a stream-stream join (news-correlated) and an independent z-score check against trailing 30-minute volatility (statistical anomaly) - rather than on merged alert rule.

**Why:** Not every real price move has a discoverable news cause. Forcing every anomaly into a single "caused by news" bucket would be dishonest; separating "we found a plausible cause" from "this is statistically unusual, cause unknown" mirrors how real monitoring/fraud systems are designed.

## What I'd change at production scale

- Schema Registry (Avro) instead of a markdown schema doc, for enforced topic contracts
- Multiple Kafka brokers with replication factor >=3, instead of single-broker/RF=1
- Delta Lake or Iceberg for the gold layer, enabling native MERGE and multi-engine access
- Real alerting via PagerDuty/Slack instead of console-only polling
- A proper "seen" tracking column (auto-increment ID) in 'market_alerts' instead of relying on Postgres's internal 'ctid'