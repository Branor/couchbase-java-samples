package samples.crud;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class ConnectionConfig {

    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseConnectionFactoryBuilder factory = new CouchbaseConnectionFactoryBuilder();

        factory.setOpTimeout(60000);// Set operation timeout to 100 ms
        factory.setUseNagleAlgorithm(true); // Control Nagle's algorithm for ACKs

        CouchbaseConnectionFactory connectionFactory = factory.buildCouchbaseConnection(nodes, "default", "");
        CouchbaseClient client = new CouchbaseClient(connectionFactory);

        // Use client...

        client.shutdown();
    }
}
