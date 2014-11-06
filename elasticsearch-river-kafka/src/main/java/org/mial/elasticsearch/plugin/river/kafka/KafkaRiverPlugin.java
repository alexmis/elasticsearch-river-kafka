package org.mial.elasticsearch.plugin.river.kafka;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

public class KafkaRiverPlugin extends AbstractPlugin {

    @Inject
    public KafkaRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-kafka";
    }

    @Override
    public String description() {
        return "River Kafka Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("kafka", KafkaRiverModule.class);
    }
}