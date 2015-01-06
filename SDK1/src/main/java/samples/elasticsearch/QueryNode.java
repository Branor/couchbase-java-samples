package samples.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.*;

/**
 * Created by David on 28/09/2014.
 */
public class QueryNode {
    public static void main(String [] args) {

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("transport.tcp.port", "9300-9400")
                .put("discovery.zen.ping.multicast.enabled", "false")
                .put("discovery.zen.ping.unicast.hosts", "localhost").build();

        Node node = nodeBuilder().clusterName("elasticsearch").settings(settings).client(true).node();
        Client client = node.client();

        GetResponse response = client.prepareGet("default", "couchbaseDocument", "person-21")
                .execute()
                .actionGet();

        System.out.println(response.getSourceAsString());

        node.close();
    }
}
