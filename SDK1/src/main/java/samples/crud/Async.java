package samples.crud;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.GetCompletionListener;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class Async {

    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        client.set("_test", 123).addListener(new OperationCompletionListener() {
            @Override
            public void onComplete(OperationFuture<?> operationFuture) throws Exception {
                System.out.println(operationFuture.getKey());
                System.out.println(operationFuture.getStatus());
                System.out.println(operationFuture.get());
            }
        });

        client.asyncGet("_test").addListener(new GetCompletionListener() {
            @Override
            public void onComplete(GetFuture<?> getFuture) throws Exception {
                System.out.println(getFuture.getStatus());
                System.out.println(getFuture.get());
            }
        });

        client.set("_test", 123).addListener((future) -> System.out.println(future.getCas()));

        client.shutdown();
    }
}
