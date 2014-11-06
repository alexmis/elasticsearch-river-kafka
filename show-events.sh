#!/bin/sh
curl -XGET 'http://127.0.0.1:9200/kafka-index/status/_search' -d '{
    "query": {
      "match_all": {}
   }
}'