# spark_streaming_scala
Spark Structured Streaming examples

## Sample task
Need to detect bot traffic.
The input data is a log with requests from users each of them contains fields like ip address, url or category page, timestamp, 'click' or 'view' type and so on so forth. 

Source data format looks like that:

{"ip": "172.10.2.125", "unix_time": 1543170426, "type": "view", "category_id": 1005},

{"ip": "172.10.3.135", "unix_time": 1543170426, "type": "click", "category_id": 1007},

...

Signs of bot's activity in our case:
1. Enormous event rate, e.g. more than 59 request in 2 minutes.
2. High difference between click and view events, e.g. (clicks/views) more than 2.5. Correctly process cases when there is no views.
3. Looking for many categories during the period, e.g. more than 15 categories in 2 minutes.

**There are two options to get source streaming data:**
1. get data from json-file source (StreamFile)
2. get data from Kafka source (StreamKafka)

### Start process

1. Open console and launch Kafka and Flume (gets data from file and put to Kafka):
```
> source start_kafka_flume.sh
```

2. Open console and launch data generator.
To generate data start botgen_rt.py with default parameters that will put data every second into file (default values: 2 bots in every 2 sec, 1000 users, 10 requests in sec, duration in 300 sec, input.txt as output):
```
> python3 botgen_rt.py
```

3. Start StreamKafka.scala
```
> $SPARK_HOME/bin/spark-submit --class "com.stopfraud.structured.StreamKafka" --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 "target/scala-2.11/stopfraud_2.11-0.1.jar"
```
