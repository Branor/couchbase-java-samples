package samples.crud;

import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class Expiration {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        client.set("_test", 1, "hello");
        System.out.println(client.get("_test"));
        //System.out.println(client.getAndTouch("_test", 5));
        //client.touch("_test", 5);
        Thread.sleep(2000);
        System.out.println(client.get("_test"));

        client.shutdown();
    }
}
