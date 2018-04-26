import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        Bson filter = Filters.and(

                Filters.eq("domain_name", domain_name),
                Filters.eq("countries.country", country_name)

                                );

        Bson add = Updates.addToSet("countries.$.IPs",IP);

        // Add the new IP.
        collection.updateOne(filter, add);

    }

    public void deleteCells(String domain_name, String country_name)
    {

        Bson filter =   Filters.and(

                Filters.eq("countries.country", country_name),
                Filters.eq("domain_name", domain_name)

                                     );

        Bson delete = Updates.pull("countries",   new Document("country", country_name));

        // Delete that IP.
        collection.updateOne(filter, delete);

        /* collection.updateOne(Filters.eq("dns", country_name),
                    Updates.addToSet("documents",temp_doc),
                    new UpdateOptions().upsert(true));*/

    }

    public void deleteCells(String domain_name, String country_name, String IP)
    {

        Bson filter =   Filters.and(

                Filters.eq("countries.country",   country_name),
                Filters.eq("domain_name", domain_name),
                Filters.in("countries.IPs", IP)

                );

        Bson delete = Updates.pull("countries.$.IPs",IP);

        // Delete that IP.
        collection.updateOne(filter, delete);

    }

    public void deleteRow(String domain_name)
    {

        Bson delete = Filters.eq("domain_name", domain_name);
        collection.deleteOne(delete);

    }

    // Add single domain,country with multiple IP.
    public int addRow(String domain_name, String country_name, List<String> IP)
    {

        Document result = collection.find(Filters.eq("domain_name", domain_name)).first();

        if (result != null)
            return -1;

        Document document = new Document("domain_name", domain_name)
                .append("countries", Arrays.asList(new Document("country", country_name)
                        .append("IPs", IP)));

        collection.insertOne(document);

        return 1;

    }

    // Return all countries and IPs of certain domain
    public List<Document> readRows(String domain_name)
    {

        Document result = collection.find(Filters.eq("domain_name", domain_name)).first();

        List<Document> docs = new ArrayList<>();
        docs = result.get("countries", List.class);

        return docs;

    }

    public static void setInitialParameters(MongoClient _mongo, MongoCredential _credential, MongoDatabase _database) {

        mongo = _mongo;
        credential = _credential;
        database = _database;

    }

}
