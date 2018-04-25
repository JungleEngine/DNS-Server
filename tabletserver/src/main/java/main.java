import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.internal.connection.ConcurrentLinkedDeque;
import org.bson.Document;
import org.bson.conversions.Bson;
import static spark.Spark.get;

public class main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;

    public static void main (String[] argv)
    {

        connectDB();


        get("/hello/:name", (request, response) -> {

            // Table used in search.
            MongoCollection<Document> collection;

            collection = database.getCollection("dns");

            collection.updateOne(Filters.eq("domain_name",  "twitter.com"),
                    Updates.set("domain_name", request.params(":name") ));
            return "Hello: " + request.params(":name");

        });

        System.out.println("abo samra");
    }

    private static void connectDB() {
        try {

            MongoClientOptions.Builder clientOptions = new MongoClientOptions.Builder();
            clientOptions.connectionsPerHost(120);

            mongo = new MongoClient(new ServerAddress("localhost",27017),clientOptions.build());
            //mongo = new MongoClient("localhost:27017?replicaSet=rs0&maxPoolSize=200", 27017);
            credential = MongoCredential.createCredential("", "tabletserver", "".toCharArray());
            database = mongo.getDatabase("tabletserver");

        }catch(Exception e){

            System.out.println("error connecting to database "+e.getMessage());

        }
    }

}
