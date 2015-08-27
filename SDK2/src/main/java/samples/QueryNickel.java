package samples;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.query.AsyncQueryResult;
import com.couchbase.client.java.query.Query;
import static com.couchbase.client.java.query.Select.*;
import static com.couchbase.client.java.query.dsl.Expression.*;

import rx.Observable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by David on 28/09/2014.
 */
public class QueryNickel {
    public static void main(String[] args) throws InterruptedException, IOException {
        System.setProperty("com.samples.queryEnabled", "true");

        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("owlet1"/*, "host2"*/);
        Bucket bucket = cluster.openBucket("default", 30, TimeUnit.SECONDS);

        // N1QL
//        Observable<AsyncQueryResult> results = bucket.async().query(
//            Query.simple(
//                "SELECT * FROM default"
//            )
//        );

        Observable<AsyncQueryResult> results = bucket.async().query(
                select("*").from("default").where(x("age").between(x(33).and(x(36))))
        );

        results
        .doOnNext(result -> {
            if (!result.finalSuccess().toBlocking().single()) {
                System.err.println(result.errors());
            }
        })
        .flatMap(AsyncQueryResult::rows)
        .toBlocking()
        .forEach(System.out::println);

       cluster.disconnect();
    }
}
