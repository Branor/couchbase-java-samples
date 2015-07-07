package samples;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.util.Date;

/**
 * Created by David on 28/09/2014.
 */
public class Crud {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("localhost"/*, "host2"*/);
        Bucket bucket = cluster.openBucket("default");

        // Key/Value CRUD
        JsonObject content = JsonObject.empty()
                .put("firstname", "David")
                .put("lastname", "Ostrovsky")
                .put("age", 36)
                .put("aliases", JsonArray.from("hello", "world", "!"));
        JsonDocument doc = JsonDocument.create("_test", content);
        bucket.upsert(doc, PersistTo.MASTER, ReplicateTo.NONE);

        StopWatch sw = new StopWatch();
        String jsonContent = "{\"hello\": \"samples\", \"active\": true}";
        sw.start();
        bucket.upsert(RawJsonDocument.create("_test2", jsonContent));
        sw.stop();
        System.out.println(sw.getNanoTime());

        JsonDocument result = bucket.get("_test");
        System.out.println(result.content().toString());

       cluster.disconnect();
    }
}
