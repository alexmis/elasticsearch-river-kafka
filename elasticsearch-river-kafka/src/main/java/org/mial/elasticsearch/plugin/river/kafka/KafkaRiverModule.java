package org.mial.elasticsearch.plugin.river.kafka;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;
import org.mial.elasticsearch.plugin.handler.HelloRestHandler;

public class KafkaRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(KafkaRiver.class).asEagerSingleton();
        bind(HelloRestHandler.class).asEagerSingleton();
    }
}
