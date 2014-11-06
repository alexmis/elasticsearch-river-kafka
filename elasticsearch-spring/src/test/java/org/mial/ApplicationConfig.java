package org.mial;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugins.PluginsService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

@Configuration
/*
@ComponentScan(basePackages = {"com.levelup.analytics"})
@PropertySource(value = {"classpath:spark.properties"})
@ImportResource({"classpath:elastic-static-data.xml"})
@EnableElasticsearchRepositories("com.levelup.repository")
*/
public class ApplicationConfig {

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public ElasticsearchTemplate elasticsearchTemplate(){
        ImmutableSettings.Builder settings = ImmutableSettings.settingsBuilder()
//                .put("http.enabled", false)
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true);

        NodeClient client = (NodeClient) nodeBuilder().settings(settings).clusterName("testCluster1111")
                .local(true)
                .node()
                .client();

//        Client client = new TransportClient()
//                .addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
//
        return new ElasticsearchTemplate(client);
    }

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(ApplicationConfig.class);
        String ss = "{\n" +
                "  \"type\": \"kafka\",\n" +
                "  \"kafka\": {\n" +
                "    \"zookeeper.connect\": \"127.0.0.1\",\n" +
                "    \"zookeeper.connection.timeout.ms\": 10000,\n" +
                "    \"topic\": \"analytics\"\n" +
                "  },\n" +
                "  \"index\": {\n" +
                "    \"index\": \"kafka-index\",\n" +
                "    \"type\": \"status\",\n" +
                "    \"bulk.size\": 100,\n" +
                "    \"concurrent.requests\": 1\n" +
                "  }\n" +
                "}";

        ElasticsearchTemplate bean = context.getBean(ElasticsearchTemplate.class);

        try {
            bean.deleteIndex("_river");
            bean.refresh("_river", true);
        } catch (Exception e) {
            System.out.println("ffff");
        }


        IndexQuery query = new IndexQuery();
        query.setIndexName("_river");
        query.setType("kafka");
        query.setId("_meta");
        query.setSource(ss);
        bean.index(query);

        bean.refresh("_river", true);
        System.out.println();
    }
}

