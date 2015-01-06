package samples.views;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.ComplexKey;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewRow;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class Grouping {
    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        View view = client.getView("dev_persons", "persons_grouped_by_house_number");
        Query query = new Query();
        query.setDescending(true);
        query.setGroup(true);
        for (ViewRow row : client.query(view, query)) {
            System.out.println(row.getKey() + "  " + row.getValue());
        }

        view = client.getView("dev_persons", "persons_by_date");
        query = new Query();
        query.setGroupLevel(4);
        query.setRange(ComplexKey.of(2014, 9, 23, 0), ComplexKey.of(2014, 9, 23, 15));
        for (ViewRow row : client.query(view, query)) {
            System.out.println(row.getKey() + "  " + row.getValue());
        }

        client.shutdown();
    }
}
