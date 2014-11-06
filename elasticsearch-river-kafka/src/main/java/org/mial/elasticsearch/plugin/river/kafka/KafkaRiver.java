package org.mial.elasticsearch.plugin.river.kafka;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

public class KafkaRiver extends AbstractRiverComponent implements River {

    private KafkaConsumer kafkaConsumer;
    private ElasticsearchProducer elasticsearchProducer;

    private Thread thread;

    @Inject
    protected KafkaRiver(RiverName riverName, RiverSettings settings, final Client client) {
        super(riverName, settings);

        final RiverConfig riverConfig = new RiverConfig(riverName, settings);
        kafkaConsumer = new KafkaConsumer(riverConfig);
        elasticsearchProducer = new ElasticsearchProducer(client, riverConfig, kafkaConsumer);
    }

    @Override
    public void start() {
        try {
            logger.info("Starting Kafka River...");
            final KafkaWorker kafkaWorker = new KafkaWorker(kafkaConsumer, elasticsearchProducer);

            thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "Kafka River Worker").newThread(kafkaWorker);
            thread.start();
        } catch (Exception ex) {
            logger.error("Unexpected Error occurred", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        logger.info("Closing kafka river...");

        elasticsearchProducer.closeBulkProcessor();

        thread.interrupt();
    }
}
