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
        DBManager.setInitialParameters(mongo, credential, database);

        DBManager db_manager = new DBManager();
        get("/add/:name", (request, response) -> {

            String[] strings = new String[]{"192.16","195.16"};
            db_manager.addRow("facebook.com","usa", strings);
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
