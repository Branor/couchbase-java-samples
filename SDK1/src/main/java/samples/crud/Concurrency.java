package samples.crud;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.Gson;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import org.apache.commons.lang3.time.StopWatch;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Created by David on 22/09/2014.
 */
public class Concurrency {

    public static void main(String[] args) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        Data data = new Data(UUID.randomUUID(), "Hello world!", 0, "data");
        client.set("_test", new Gson().toJson(data)).get();

        // Java 8
        Function<String, String> updater = json -> {
            Data d = new Gson().fromJson(json, Data.class);
            d.setNumber(d.getNumber() + 1);
            return new Gson().toJson(d);
        };

        StopWatch sw = new StopWatch();
        sw.start();
        IntStream.range(0, 1000).parallel().forEach(i -> UnsafeUpdate(client, updater));
        //IntStream.range(0, 1000).parallel().forEach(i -> UpdateWithRetry(client));
        //IntStream.range(0, 1000).parallel().forEach(i -> UpdateWithRetry(client, updater));
        //IntStream.range(0, 1000).parallel().forEach(i -> UpdateWithLock(client));

        sw.stop();
        System.out.println(client.get("_test") + " " + sw.getNanoTime() / 1000000);

        client.shutdown();
    }

    private static void UnsafeUpdate(CouchbaseClient client, Function<String, String> updater) {
        Object value = client.get("_test");
        client.set("_test", updater.apply((String) value));
    }

    private static void UpdateWithRetry(CouchbaseClient client) {
        Boolean success = false;
        int retry = 100;

        while(!success && retry-- > 0) {
            CASValue<Object> value = client.gets("_test");
            Data data = new Gson().fromJson((String) value.getValue(), Data.class);
            data.setNumber(data.getNumber() + 1);
            CASResponse res = client.cas("_test", value.getCas(), new Gson().toJson(data));
            success = res == CASResponse.OK;
        }
    }

    private static void UpdateWithRetry(CouchbaseClient client, Function<String, String> updater)  {
        Boolean success = false;
        int retry = 100;

        while(!success && retry-- > 0) {
            CASValue<Object> value = client.gets("_test");
            CASResponse res = client.cas("_test", value.getCas(), updater.apply((String) value.getValue()));
            success = res == CASResponse.OK;
        }
    }

    private static void UpdateWithLock(CouchbaseClient client) {
        Boolean success = false;
        int retry = 100;
        CASValue<Object> result = null;

        while(!success && retry-- > 0) {
            result = client.getAndLock("_test", 5);// Timeout is in seconds
            success = result.getValue() != null && result.getCas() != -1;
        }

        if(result != null && result.getValue() != null) {
            Data data = new Gson().fromJson((String) result.getValue(), Data.class);
            data.setNumber(data.getNumber() + 1);
            CASResponse res = client.cas("_test", result.getCas(), new Gson().toJson(data));
        }
    }
}