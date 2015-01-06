package samples.crud;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.PersistTo;
import net.spy.memcached.ReplicateTo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.time.StopWatch;

/**
 * Created by David on 22/09/2014.
 */
public class WriteConcern {

    public static void main(String[] args) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");


        StopWatch sw = new StopWatch();
        sw.start();
        Boolean result = client.set("_test", 123, PersistTo.ONE, ReplicateTo.ZERO).get();
        sw.stop();
        System.out.println(sw.getNanoTime());

        client.shutdown();
    }
}
