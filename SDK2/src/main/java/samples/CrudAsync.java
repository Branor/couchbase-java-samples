package samples;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.commons.lang3.time.StopWatch;
import rx.Observable;
import rx.observables.BlockingObservable;

import java.io.Console;
import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by David on 28/09/2014.
 */
public class CrudAsync {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("localhost"/*, "host2"*/);
        Bucket bucket = cluster.openBucket("default");

        bucket.get("bla");
        Observable.just("_test", "_test2", "_test3")
            .flatMap(id -> bucket.async().get(id))
            .map(doc -> {
                return doc;
            })
            .timestamp()
            .map(doc -> {
                System.out.println(doc.getValue().id() + " " + doc.getTimestampMillis());
                return doc.getValue();
            })
            .map(doc -> {
                doc.content().put("date", System.currentTimeMillis());
                return doc;
            })
            .flatMap(doc -> {
                // transform
                return bucket.async().upsert(doc).timestamp();
            })
            .map(doc -> {
                System.out.println(doc.getValue().id() + " " + doc.getTimestampMillis());
                return doc.getValue();
            })
            .subscribe(
                next -> System.out.println("Next " + next),
                error -> System.out.println("Error " + error),
                () -> System.out.println("Done")
            );
            //.subscribe();
            //.toBlocking()
            //.toIterable();
            //.forEach(System.out::println);


        System.in.read();
        cluster.disconnect();
    }
}
