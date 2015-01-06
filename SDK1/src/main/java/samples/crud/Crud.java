package samples.crud;

import com.couchbase.client.CouchbaseClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Crud {

    public static void main(String[] args) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        Boolean success;
        Object value;
        Data data;

        success = client.set("_test", 123).get();
        System.out.println(success);

//        value = client.get("_test");
//        System.out.println(value);
//
//        Gson gson = new Gson();
//
//        data = new Data(UUID.randomUUID(), "Hello world!", 123.123, "data");
//        success = client.set("_test", gson.toJson(data)).get();
//        System.out.println(success);
//
//        value = client.get("_test");
//        System.out.println(value);
//
//        data = gson.fromJson((String) value, Data.class);
//        System.out.println(data.getId());
//
//        success = client.add("_test", 123).get();
//        System.out.println(success);
//
//        success = client.replace("_not_found", 123).get();
//        System.out.println(success);
//
//        client.delete("_test").get();
//        value = client.get("_test");
//        System.out.println(value);

        client.shutdown();
    }
}
