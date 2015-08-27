import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.metrics.DefaultLatencyMetricsCollectorConfig;
import com.couchbase.client.core.metrics.DefaultMetricsCollectorConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * Created by davido on 8/27/15.
 */
public class Metrics {
    public static void main(String [] args) {
        // Optional: customize the emit frequency from every hour to every 10 seconds
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .networkLatencyMetricsCollectorConfig(DefaultLatencyMetricsCollectorConfig.builder()
                        .emitFrequency(10)
                        .emitFrequencyUnit(TimeUnit.SECONDS)
                        .build())
                .systemMetricsCollectorConfig(DefaultMetricsCollectorConfig.builder()
                        .emitFrequency(10)
                        .emitFrequencyUnit(TimeUnit.SECONDS)
                        .build())
                .build();

// Connect to the bucket
        Cluster cluster = CouchbaseCluster.create(env, "localhost");
        Bucket bucket = cluster.openBucket();

// Consume the event bus events
        env
                .eventBus()
                .get()
                .filter(event -> event.type().equals(EventType.METRIC)) // only care about metrics
                .subscribe(System.out::println); // print them to stderr as strings


// Perform some sample load to generate meaningful data
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            bucket.upsert(JsonDocument.create("doc" + i, JsonObject.empty())); // store doc
            bucket.get("doc" + i); // load doc
            bucket.remove("doc"+i); // remove doc
            //bucket.get("doc"+i); // try to load removed doc
        }

// Shut down eventually
        cluster.disconnect();
        env.shutdown();
    }
}
