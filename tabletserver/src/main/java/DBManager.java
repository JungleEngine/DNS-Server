import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.internal.connection.ConcurrentLinkedDeque;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Set;

public class DBManager {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    private MongoCollection<Document> collection;

    public DBManager()
    {

        collection = database.getCollection("dns");


    }



    public void set(String domain_name, String country_name, String IP)
    {
            /*collection.updateOne(eq("term", entry.getKey()),
                    Updates.addToSet("documents",temp_doc),
                    new UpdateOptions().upsert(true));*/

    }

    public void deleteCells(String domain_name, String country_name)
    {

    }

    public void deleteCells(String domain_name, String country_name, String IP)
    {

    }

    public void deleteRow(String domain_name)
    {

    }

    public int addRow(String domain_name, String country_name, String[] IP)
    {
        Document result = collection.find(Filters.eq("domain_name", domain_name)).first();

        if (result != null)
            return -1;

        Document document = new Document("domain_name", domain_name)
                .append("countries", Arrays.asList(new Document("country", country_name)
                        .append("IPs", Arrays.asList(IP))));

        collection.insertOne(document);
        return 1;

    }

    public void readRows(String domain_name)
    {

    }

    public static void setInitialParameters(MongoClient _mongo, MongoCredential _credential, MongoDatabase _database) {

        mongo = _mongo;
        credential = _credential;
        database = _database;

    }

}
