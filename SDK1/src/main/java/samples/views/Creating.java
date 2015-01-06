package samples.views;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.ViewDesign;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by David on 22/09/2014.
 */
public class Creating {
    public static void main(String[] args) throws IOException, URISyntaxException {
        List<URI> nodes = Arrays.asList(new URI("http://127.0.0.1:8091/pools"));
        CouchbaseClient client = new CouchbaseClient(nodes, "default", "");

        DesignDocument designDoc;
        Boolean success;

        designDoc = client.getDesignDoc("dev_persons");
        System.out.println(designDoc.toJson());

        for (ViewDesign viewDesign : designDoc.getViews()) {
            System.out.println(viewDesign.getMap());
            System.out.println(viewDesign.getReduce());
        }

        String map = "function (doc, meta) {\n" +
                "  if(meta.type == 'json' && doc.type == 'data' && doc.number)\n" +
                "  \temit(doc.number, null);\n" +
                "}";
        designDoc = new DesignDocument("dev_data"); // Remove "dev_" to create in production mode
        ViewDesign viewDesign = new ViewDesign("by_number", map /*, "_count"*/);
        designDoc.setView(viewDesign);
        success = client.createDesignDoc(designDoc);
        System.out.println(success);

        client.shutdown();
    }
}
