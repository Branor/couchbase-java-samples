package samples.views;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewRow;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class Querying {
    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseConnectionFactory connectionFactory = new CouchbaseConnectionFactoryBuilder()
                .setViewTimeout(30) // set the timeout to 30 seconds
                .setViewWorkerSize(5) // use 5 worker threads instead of one
                .setViewConnsPerNode(20) // allow 20 parallel http connections per node in the cluster
                .buildCouchbaseConnection(nodes, "default", "");
        CouchbaseClient client = new CouchbaseClient(connectionFactory);

        View view = client.getView("dev_persons", "persons_by_age");
        Query query = new Query();
        query.setLimit(10);
        query.setKey("35");
        for (ViewRow row : client.query(view, query)) {
            System.out.println(row.getId());
        }

        query = new Query();
        query.setLimit(10);
        query.setRange("30", "35");
        query.setIncludeDocs(true);
        for (ViewRow row : client.query(view, query)) {
            System.out.println(row.getDocument());
        }

        client.shutdown();
    }
}
