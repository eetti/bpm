## Streaming App

The following spark applications ingest a series of health records spacificaly heart beats record evert 0.5 seconds over 15 minutes.

>Source: [MIT Dataset](#)

### Usage
1. We need to insert data into the kafka input topic. You would need the filename and the kafka topic name.
>`python streamer.py data/hr.7257-time.txt input `

2. Proceed to start the first spark application that would ingest this records, process them and send the reocrds to another kafka topic.
>`../spark-3.0.1-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 app.py --input input --output output`

3. The second spark app aggregates the data and displays if we should send an alert for a given time period

>`../spark-3.0.1-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 app-seq.py  --output output`