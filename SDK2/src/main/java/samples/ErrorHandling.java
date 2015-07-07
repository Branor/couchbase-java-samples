package samples;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.CASMismatchException;
import rx.Observable;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by David on 28/09/2014.
 */
public class ErrorHandling {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("localhost"/*, "host2"*/);
        AsyncBucket bucket = cluster.openBucket("default").async();

        JsonObject content = JsonObject.empty()
                .put("firstname", "David")
                .put("lastname", "Ostrovsky")
                .put("age", 36)
                .put("aliases", JsonArray.from("hello", "world", "!"));
        JsonDocument doc = JsonDocument.create("_test", content);
        bucket.upsert(doc).subscribe();

        // Update the document with CAS, retry once per second if CASMismatchException is raised
        Observable
        .defer(() -> bucket.get("_test"))
        .map(document -> {
            // Modify document in the background to simulate write collisions. Do not use this in production!
            if(new Random().nextInt(10) % 5 != 0 )
                bucket.replace(document).toBlocking().last();

            // Update the retrieved document
            document.content().put("modified", new Date().getTime());
            return document;
        })
        .flatMap(bucket::replace)
        .retryWhen(attempts ->
            attempts.flatMap(n -> {
                System.out.println(n);
                if (!(n instanceof CASMismatchException)) {
                    return Observable.error(n);
                }
                return Observable.timer(1, TimeUnit.MILLISECONDS);
            })
        )
        .subscribe(result -> System.out.println("replace: " + result));
        System.in.read();

        // Get document from MASTER node, fall back to getFromReplica if TimeoutException is raised
        bucket
        .get("_test")
        .timeout(500, TimeUnit.MILLISECONDS)
        // Simulating a timeout exception due to a failed node. Do not use this in production!
        .flatMap(document -> Observable.error(new TimeoutException("Testing retry behavior.")))
        .onErrorResumeNext(throwable -> {
            if (throwable instanceof TimeoutException) {
                System.out.println("Trying to get from replica...");
                return bucket.getFromReplica("_test", ReplicaMode.ALL);
            }
            return Observable.error(throwable);
        })
        .subscribe(result -> System.out.println("get: " + result));

        System.in.read();
        cluster.disconnect();
    }
}
