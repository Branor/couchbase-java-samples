package samples.views;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class Paging {
    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        View view = client.getView("dev_persons", "persons_by_age");
        Query query = new Query();
        query.setLimit(10);

        int docsPerPage = 4;
        Paginator pages = client.paginatedQuery(view, query, docsPerPage);
        while (pages.hasNext()) {
            System.out.println ("\tPage:");
            ViewResponse response = pages.next();
            for (ViewRow row : response) {
                System.out.println(row.getId());
            }
        }

        client.shutdown();
    }
}
