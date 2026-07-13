
.PHONY: up down producers streaming demo logs clean

up:
	docker-compose up -d


down:
	docker-compose down


producers:
	nohup python src/producers/stock_trade_producer.py > logs/trade_producer.log 2>&1 &
	nohup python src/producers/news_producer.py > logs/news_producer.log 2>&1 &
	@echo "Producers started in background. Logs in logs/"


streaming:
	nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars ~/spark-jars/postgresql-42.7.3.jar src/streaming/read_trades.py > logs/ohlc.log 2>&1 &
	nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars ~/spark-jars/postgresql-42.7.3.jar src/streaming/news_sentiment.py > logs/sentiment.log 2>&1 &
	nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars ~/spark-jars/postgresql-42.7.3.jar src/streaming/correlate_alerts.py > logs/correlate.log 2>&1 &
	@echo "Spark streaming jobs started in background. Logs in logs/"


alerts:
	nohup python src/utils/alert_watcher.py > logs/alert_watcher.log 2>&1 &
	nohup python src/utils/anomaly_detector.py > logs/anomaly_detector.log 2>&1 &
	@echo "Alerting started in background. Logs in logs/"


demo: up
	@sleep 5
	@mkdir -p logs
	$(MAKE) producers
	@sleep 5
	$(MAKE) streaming
	@sleep 5
	$(MAKE) alerts
	@echo "Full demo running. Check logs/ for output, or docker-compose ps for containers."


logs:
	tail -f logs/*.log


clean:
	pkill -f "producers/stock_trade_producer.py" || true
	pkill -f "producers/news_producer.py" || true
	pkill -f "streaming/read_trades.py" || true
	pkill -f "streaming/news_sentiment.py" || true
	pkill -f "streaming/correlate_alerts.py" || true
	pkill -f "utils/alert_watcher.py" || true
	pkill -f "utils/anomaly_detector.py" || true
	@echo "All background processes stopped."