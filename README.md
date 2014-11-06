

$ bin/plugin --remove example-plugin
$ bin/plugin --url file://///home/mial/work/elasticsearch-plugin/elasticsearch-river-kafka/target/releases/elasticsearch-river-kafka-0.0.1-SNAPSHOT.zip --install example-plugin
$ bin/elasticsearch -f

DELETE /_river

PUT _river/kafka/_meta
{
"type": "kafka",
  "kafka": {
    "zookeeper.connect": "127.0.0.1",
    "zookeeper.connection.timeout.ms": 10000,
    "topic": "analytics"
  },
  "index": {
    "index": "kafka-index",
    "type": "status",
    "bulk.size": 100,
    "concurrent.requests": 1
  }
}

GET /_river/kafka/_meta

DELETE /kafka-index

GET /kafka-index/status/_search
{
   "query": {
      "match_all": {}
   }
}

Run kafka:
./zookeeper-server-start.sh -daemon ../config/zookeeper.properties
./kafka-server-start.sh -daemon ../config/server.properties

Produce Events:
$ ./kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic analytics <export-analytics-2014.07.28.json
