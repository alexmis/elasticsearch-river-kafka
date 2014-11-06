package org.mial;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;

@ElasticsearchIntegrationTest.ClusterScope(
        scope = ElasticsearchIntegrationTest.Scope.SUITE, transportClientRatio = 0.0)
public class KafkaRiverTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Before
    public void createEmptyRiverIndex() {
        // We want to force _river index to use 1 shard 1 replica
        client().admin().indices().prepareCreate("_river").setSettings(ImmutableSettings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)).get();
    }

    @After
    public void deleteRiverAndWait() throws InterruptedException {
        logger.info(" --> remove all wikipedia rivers");
        client().admin().indices().prepareDelete("_river").get();
        // We just wait a few to make sure that all bulks has been processed
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                return false;
            }
        }, 2, TimeUnit.SECONDS);
    }

    @Test
    public void testKafkaRiver() throws IOException, InterruptedException {
        logger.info(" --> create wikipedia river");
        XContentBuilder source = jsonBuilder()
                .startObject().field("type", "kafka")

                .startObject("kafka")
                .field("zookeeper.connect", "127.0.0.1")
                .field("zookeeper.connection.timeout.ms", 10000)
                .field("topic", "analytics")
                .endObject()

                .startObject("index")
                .field("index", "kafka-index")
                .field("type", "status")
                .field("bulk.size", 100)
                .field("concurrent.requests", 1)
                .endObject()

                .endObject();

        index("_river", "kafka", "_meta", source);

        logger.info(" --> waiting for some documents");
        // Check that docs are indexed by the river
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                try {
                    refresh();
                    CountResponse response = client().prepareCount("_river").get();
                    logger.info("  -> got {} docs in {} index", response.getCount());
                    return response.getCount() > 0;
                } catch (IndexMissingException e) {
                    return false;
                }
            }
        }, 1, TimeUnit.MINUTES), equalTo(true));

        while (true) {
            Thread.sleep(1000);
            try {
                CountResponse response = client().prepareCount("kafka-index").setTypes("status").get();
                System.out.println(">>>  " + response.getCount());
            }catch (Exception e){
                logger.error(e.getMessage());
            }

        }
    }
}
