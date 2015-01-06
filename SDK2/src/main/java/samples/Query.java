package samples;

import com.couchbase.client.java.*;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.view.*;
import rx.Observable;

import java.io.IOException;

import static com.couchbase.client.java.query.Select.*;
import static com.couchbase.client.java.query.dsl.Expression.*;

/**
 * Created by David on 28/09/2014.
 */
public class Query {
    public static void main(String[] args) throws InterruptedException, IOException {
        System.setProperty("com.samples.queryEnabled", "true");

        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("localhost"/*, "host2"*/);
        Bucket bucket = cluster.openBucket("default");

        // N1QL
//        Observable<AsyncQueryResult> results = bucket.async().query(
//                "SELECT * FROM default"
//        );

        Observable<AsyncQueryResult> results = bucket.async().query(
                select("*").from("default").where(x("age").between(x(33).and(x(36))))
        );

        results
        .doOnNext(result -> {
            if (!result.success()) {
                System.err.println(result.error());
            }
        })
        .flatMap(AsyncQueryResult::rows)
        .toBlocking()
        .forEach(System.out::println);


        // Views
        bucket.async()
        .query(ViewQuery.from("users", "all_users").startKey("pymc33").limit(4))
        .doOnNext(viewResult -> {
            if (!viewResult.success()) {
                System.err.println(viewResult.error());
            }
        })
        .flatMap(AsyncViewResult::rows)
        .flatMap(AsyncViewRow::document)
        .toBlocking()
        .forEach(System.out::println);

       cluster.disconnect();
    }
}
