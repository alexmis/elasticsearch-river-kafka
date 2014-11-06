package org.mial.elasticsearch.plugin.river.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer {

    private final static Integer AMOUNT_OF_THREADS_PER_CONSUMER = 1;
    private final static String GROUP_ID = "elasticsearch-kafka-river";
    private final static Integer CONSUMER_TIMEOUT = 15000;

    private List<KafkaStream<byte[], byte[]>> streams;
    private ConsumerConnector consumer;

    private final ESLogger logger = ESLoggerFactory.getLogger(KafkaConsumer.class.getName());


    public KafkaConsumer(final RiverConfig riverConfig) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(riverConfig));

        final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(riverConfig.getTopic(), AMOUNT_OF_THREADS_PER_CONSUMER);

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCountMap);

        streams = consumerStreams.get(riverConfig.getTopic());

        logger.info("Started kafka consumer for topic: " + riverConfig.getTopic() + " with " + streams.size() + " partitions in it.");
    }

    private ConsumerConfig createConsumerConfig(final RiverConfig riverConfig) {
        final Properties props = new Properties();
        props.put(RiverConfig.ZOOKEEPER_CONNECT, riverConfig.getZookeeperConnect());
        props.put("zookeeper.connection.timeout.ms", String.valueOf(riverConfig.getZookeeperConnectionTimeout()));
        props.put("group.id", GROUP_ID);
        props.put("auto.commit.enable", String.valueOf(false));
        props.put("consumer.timeout.ms", String.valueOf(CONSUMER_TIMEOUT));

        return new ConsumerConfig(props);
    }


    List<KafkaStream<byte[], byte[]>> getStreams() {
        return streams;
    }

    public ConsumerConnector getConsumer() {
        return consumer;
    }
}