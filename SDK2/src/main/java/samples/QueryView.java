package samples;

import com.couchbase.client.java.*;
import com.couchbase.client.java.view.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by David on 28/09/2014.
 */
public class QueryView {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("localhost"/*, "host2"*/);
        Bucket bucket = cluster.openBucket("default", 30, TimeUnit.SECONDS);

        // Views
        long start = System.currentTimeMillis();
        bucket.async()
        .query(ViewQuery.from("dev_test", "users_by_name").limit(100))
        .doOnNext(viewResult -> {
            if (!viewResult.success()) {
                System.err.println(viewResult.error());
            }
        })
        .flatMap(AsyncViewResult::rows)
        .flatMap(AsyncViewRow::document)
        .toBlocking();
        long end =  System.currentTimeMillis()- start;
        System.out.println(end);
        //.forEach(System.out::println);

       cluster.disconnect();
    }
}
