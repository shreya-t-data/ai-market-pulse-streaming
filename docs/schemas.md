# Kafka Topic Schemas

## Topic: `stock-trades`
Partitions: 4 | Key: ticker symbol (string)

| Field | Type | Description |
|---|---|---|
| ticker | string | Stock symbol (e.g. "NVDA") |
| price | float | Trade price |
| volume | int | Trade volume |
| trade_timestamp | int (epoch ms) | Time of trade (from Finnhub) |
| received_at | int (epoch ms) | Time producer received/sent the message |

Example:
```json
{"ticker": "NVDA", "price": 194.83, "volume": 120, "trade_timestamp": 1783287600000, "received_at": 1783287600500}
```

## Topic: `market-news`
Partitions: 2 | Key: ticker symbol (string)

| Field | Type | Description |
|---|---|---|
| ticker | string | Related stock symbol |
| headline | string | Article headline |
| summary | string | Short article summary |
| source | string | Publisher (e.g. "Yahoo") |
| url | string | Article URL (used for dedup) |
| published_at | int (epoch s) | Publish time (from Finnhub) |
| received_at | int (epoch ms) | Time producer published to Kafka |

Example:
```json
{"ticker": "NVDA", "headline": "...", "summary": "...", "source": "Yahoo", "url": "https://...", "published_at": 1783287600, "received_at": 1783287600500}
```