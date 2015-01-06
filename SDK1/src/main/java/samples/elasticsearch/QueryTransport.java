package samples.elasticsearch;

import com.couchbase.client.CouchbaseClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by David on 28/09/2014.
 */
public class QueryTransport {
    public static void main(String [] args) throws URISyntaxException, IOException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient couchbase = new CouchbaseClient(nodes, "default", "");

        Client client = new TransportClient()
                .addTransportAddress(
                        new InetSocketTransportAddress("127.0.0.1", 9300));

        SearchResponse response = client.prepareSearch("default")
                .setTypes("couchbaseDocument")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                //.setQuery(QueryBuilders.simpleQueryString("john-13"))             // Query
                .setPostFilter(FilterBuilders.rangeFilter("age").from(12).to(18))   // Filter
                .setFrom(0).setSize(60)
                //.setExplain(true)
                .execute()
                .actionGet();

        //System.out.println(response.toString());
        response.getHits().forEach(hit -> System.out.println(hit.getId()));

        List<String> hits = new ArrayList<String>();
        response.getHits().forEach(hit -> hits.add(hit.getId()));

        Map<String, Object> docs = couchbase.getBulk(hits);
        System.out.println("Loaded " + docs.size() + " full docs.");

//        response.getHits().forEach(hit -> {
//            if(hit.score() > 0.5)
//                System.out.println(couchbase.get(hit.getId()));
//        });

        couchbase.shutdown();
        client.close();
    }
}
