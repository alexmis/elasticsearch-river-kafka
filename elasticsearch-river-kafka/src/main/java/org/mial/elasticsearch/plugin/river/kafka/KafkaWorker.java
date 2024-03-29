package org.mial.elasticsearch.plugin.river.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;


/**
 * The worker thread, which does the actual job of consuming messages from kafka and passing those to
 * Elastic Search producer - {@link org.elasticsearch.river.kafka.ElasticsearchProducer} to index.
 * Behind the scenes of kafka high level API, the worker will read the messages from different kafka brokers and
 * partitions.
 */
public class KafkaWorker implements Runnable {

    private KafkaConsumer kafkaConsumer;
    private ElasticsearchProducer elasticsearchProducer;

    private volatile boolean consume = false;

    private final ESLogger logger = ESLoggerFactory.getLogger(KafkaWorker.class.getName());

    /**
     * For randomly selecting the partition of a kafka partition.
     */
    private Random random = new Random();


    public KafkaWorker(final KafkaConsumer kafkaConsumer, final ElasticsearchProducer elasticsearchProducer) {
        this.kafkaConsumer = kafkaConsumer;
        this.elasticsearchProducer = elasticsearchProducer;
    }

    @Override
    public void run() {

        logger.info("Kafka worker started...");

        if (consume) {
            logger.info("Consumer is already running, new one will not be started...");
            return;
        }

        consume = true;
        try {
            logger.info("Kafka consumer started...");

            while (consume) {
                KafkaStream stream = chooseRandomStream(kafkaConsumer.getStreams());
                Set<MessageAndMetadata> consumedMessages = consumePartitionMessages(stream);

                elasticsearchProducer.addMessagesToBulkProcessor(consumedMessages);
            }
        } finally {
            logger.info("Kafka consumer has stopped...");
            consume = false;
        }
    }

    /**
     * Consumes the messages from the partition via specified stream.
     */
    private Set<MessageAndMetadata> consumePartitionMessages(final KafkaStream stream) {
        final Set<MessageAndMetadata> messageSet = new HashSet<MessageAndMetadata>();

        try {
            // by default it waits forever for message, but there is timeout configured
            final ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();

            // Consume all the messages of the stream (partition)
            while (consumerIterator.hasNext() && consume) {

                final MessageAndMetadata messageAndMetadata = consumerIterator.next();
                logMessage(messageAndMetadata);


                messageSet.add(messageAndMetadata);
            }
        } catch (ConsumerTimeoutException ex) {
            logger.info("Nothing to be consumed for now. Consume flag is: " + consume);
        }
        return messageSet;
    }

    /**
     * Chooses a random stream to consume messages from, from the given list of all streams.
     *
     * @return randomly choosen stream
     */
    private KafkaStream chooseRandomStream(final List<KafkaStream<byte[], byte[]>> streams) {
        final int streamNumber = random.nextInt(streams.size());

//        logger.info("Selected stream " + streamNumber + " out of  " + streams.size() + " from TOPIC: " + kafkaConsumer.getRiverConfig().getTopic());

        return streams.get(streamNumber);
    }

    /**
     * Logs consumed kafka messages to the log.
     */
    private void logMessage(final MessageAndMetadata messageAndMetadata) {
        final byte[] messageBytes = (byte[]) messageAndMetadata.message();

        try {
            final String message = new String(messageBytes, "UTF-8");

            logger.info(message);
        } catch (UnsupportedEncodingException e) {
            logger.info("The UTF-8 charset is not supported for the kafka message.");
        }
    }
}