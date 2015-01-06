package samples;

import com.couchbase.client.java.*;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import java.io.IOException;

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
                .put("age", 35)
                .put("aliases", JsonArray.from("hello", "world", "!"));
        JsonDocument doc = JsonDocument.create("_test", content);
        bucket.replace(doc, PersistTo.MASTER, ReplicateTo.NONE);

        String jsonContent = "{\"hello\": \"samples\", \"active\": true}";
        bucket.upsert(RawJsonDocument.create("_test2", jsonContent));

        JsonDocument result = bucket.get("_test");
        System.out.println(result.content().toString());

       cluster.disconnect();
    }
}
