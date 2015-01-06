package samples;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by David on 28/09/2014.
 */
public class Bulk {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Creating the connection
        Cluster cluster = CouchbaseCluster.create("localhost"/*, "host2"*/);
        Bucket bucket = cluster.openBucket("default");

        // Bulk Operations
        List<String> ids = Arrays.asList("pymc0", "pymc3", "pymc9");
        System.out.println("Retrieving " + ids.size() + " documents...");
        bulkGet(ids, bucket)
                .forEach(System.out::println);

        int docsToCreate = 100;
        List<JsonDocument> documents = new ArrayList<>();
        for (int i = 0; i < docsToCreate; i++) {
            JsonObject content = JsonObject.create()
                    .put("counter", i)
                    .put("name", "Foo Bar");
            documents.add(JsonDocument.create("doc-" + i, content));
        }
        List<JsonDocument> createdDocs = bulkUpsert(documents, bucket);
        System.out.println("Created " + createdDocs.size() + " documents.");

       cluster.disconnect();
    }

    public static List<JsonDocument> bulkGet(final Collection<String> ids, Bucket bucket) {
        return Observable
                .from(ids)
                .flatMap(id -> bucket.async().get(id))
                .toList()
                .toBlocking()
                .single();
    }

    public static List<JsonDocument> bulkUpsert(List<JsonDocument> documents, Bucket bucket) {
        return Observable
                .from(documents)
                .flatMap(docToInsert -> bucket.async().upsert(docToInsert))
                .toList()
                .toBlocking()
                .single();
    }
}
