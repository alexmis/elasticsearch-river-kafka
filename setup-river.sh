#!/bin/sh
curl -XDELETE 'http://localhost:9200/_river'

echo "----------------------------------------------"

curl -XDELETE 'http://localhost:9200/kafka-index'

echo "----------------------------------------------"
curl -XPUT 'http://127.0.0.1:9200/_river/kafka/_meta' -d'{
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
}'

curl -XGET 'http://127.0.0.1:9200/_river/kafka/_meta'
