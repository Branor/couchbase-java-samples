package samples.crud;

import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Created by David on 22/09/2014.
 */
public class Counters {

    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        IntStream.range(0,1000).parallel().forEach(i -> client.incr("_counter", 1, 1));

        System.out.println(client.get("_counter"));

        client.shutdown();
    }
}
