package samples;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.apache.commons.lang3.Range;
import com.couchbase.client.java.util.retry.RetryBuilder;
import rx.Observable;
import rx.exceptions.MissingBackpressureException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by David on 28/09/2014.
 */
public class Tests {
    private static Cluster cluster;
    private static Bucket bucket;

    public static void main(String[] args) throws InterruptedException, IOException {
        // Creating the connection
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().requestBufferSize(64*1024).responseBufferSize(64*1024).build();
        cluster = CouchbaseCluster.create(env, "localhost"/*, "host2"*/);
        bucket = cluster.openBucket("default");

        //SyncPerformance();
        AsyncPerformance();



        System.in.read();
        cluster.disconnect();
    }

    private static void SyncPerformance() {
        int items = 100;
        int count = 1000000;
        // Setup
        for(int i=0; i<items; i++)
            bucket.upsert(JsonDocument.create("test-" + i%items, JsonObject.create().put("hello", "world")));

        // Test
        long start = System.currentTimeMillis();
        for(int i=0; i < count; i++){
            bucket.get("test-" + i%items);

            if(i % 10000 == 0) {
                long elapsed = System.currentTimeMillis() - start;
                double avg = (double)(elapsed) / (double)i;
                System.out.println("Average op time: " + avg + ", ops per second: " + 1000 / avg);
            }
        }
    }

    private static void AsyncPerformance() {
        int items = 100;
        int count = 1000000;
        // Setup
        for(int i=0; i<items; i++)
            bucket.upsert(JsonDocument.create("test-" + i%items, JsonObject.create().put("hello", "world")));

        // Test
        long start = System.currentTimeMillis();
        AtomicInteger total = new AtomicInteger(0);

        Observable.range(0, count)
                .flatMap(i -> bucket.async().get("test-" + i%100))
                .retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                        .delay(Delay.exponential(TimeUnit.MILLISECONDS))
                .max(10)
                .build())
                .timestamp()
                .doOnEach(doc -> total.incrementAndGet())
                .sample(1, TimeUnit.SECONDS)
                .subscribe(
                        success -> {
                            long elapsed = success.getTimestampMillis() - start;
                            double avg = (double) (elapsed) / (double) total.get();
                            System.out.println("Average op time: " + avg + ", ops per second: " + 1000 / avg);
                        },
                        error -> {
                            System.out.println(error);
                        },
                        () -> {
                            System.out.println("Done.");
                        }
                );

    }

}
