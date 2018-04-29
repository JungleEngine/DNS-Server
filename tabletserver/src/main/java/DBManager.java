import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBManager {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    private MongoCollection<Document> collection;

    public DBManager() {
        collection = database.getCollection("dns");
    }


    public void set(String domain_name, String country_name, List<String> IPs) {

        Bson filter = Filters.and(

                Filters.eq("domain_name", domain_name),
                Filters.eq("countries.country", country_name)

        );

        Bson add = Updates.addEachToSet("countries.$.IPs", IPs);

        // Add the new IP.
        collection.updateOne(filter, add);

        collection.updateOne(filter,Updates.set("dirty_bit","1"));

    }

    public void deleteCells(String domain_name, String country_name) {

        Bson filter = Filters.and(

                Filters.eq("countries.country", country_name),
                Filters.eq("domain_name", domain_name)

        );

        Bson delete = Updates.pull("countries", new Document("country", country_name));

        // Delete that IP.
        collection.updateOne(filter, delete);

        collection.updateOne(filter,Updates.set("dirty_bit","1"));

        /* collection.updateOne(Filters.eq("dns", country_name),
                    Updates.addToSet("documents",temp_doc),
                    new UpdateOptions().upsert(true));*/

    }

    public void deleteCells(String domain_name, String country_name, String IP) {

        Bson filter = Filters.and(

                Filters.eq("countries.country", country_name),
                Filters.eq("domain_name", domain_name),
                Filters.in("countries.IPs", IP)

        );

        Bson delete = Updates.pull("countries.$.IPs", IP);

        // Delete that IP.
        collection.updateOne(filter, delete);

        collection.updateOne(filter,Updates.set("dirty_bit","1"));

    }

    public void deleteRow(String domain_name) {

        Bson delete = Filters.eq("domain_name", domain_name);
        collection.deleteOne(delete);

    }

    // Add single domain name & country with multiple IPs.
    public int addRow(String domain_name, String country_name, List<String> IP) {


//        Document result = collection.find(Filters.eq("domain_name", domain_name)).first();
//
//        if (result != null)
//            return -1;

//        Document document = new Document("domain_name", domain_name)
//                .append("countries", Arrays.asList(new Document("country", country_name)
//                        .append("IPs", IP)));
        Bson updates = Updates.addToSet("countries", new Document("country", country_name)
                .append("IPs", IP));
        Bson updates2 = Updates.set("dirty_bit","1");
        Bson updates3 = Updates.combine(updates,updates2);

        // Add country with IPs and if domain doesn't exist create new  document.
        collection.updateOne(Filters.eq("domain_name", domain_name),updates3,
                new UpdateOptions().upsert(true));



        //collection.insertOne(document);

        return 1;

    }

    // Return all IPs of a certain domain & country
    public List<Document> readRow(String domain_name) {

        Document result = collection.find(Filters.eq("domain_name", domain_name)).first();

        List<Document> docs = result.get("countries", List.class);

        return docs;
    }

    public static void setInitialParameters(MongoClient _mongo, MongoCredential _credential, MongoDatabase _database) {

        mongo = _mongo;
        credential = _credential;
        database = _database;

    }

    public void fillInitialData(String initialData) {

        try {


            JSONParser JP = new JSONParser();
            JSONArray JA = (JSONArray) JP.parse(initialData);

            for (int i = 0 ; i < JA.size(); ++i)
            {
                JSONObject JO = (JSONObject)JA.get(i);
                try {
                    collection.insertOne(Document.parse(JO.toString()));
                }catch(Exception e)
                {

                }
            }

            //if (initialData != null)
            //    System.out.println("Message: " + initialData);

            System.out.println("Received data from master");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getUpdatedDocuments() {

        FindIterable<Document> docs = collection.find(Filters.eq("dirty_bit", "1"));

        String serialize = JSON.serialize(docs);

        return serialize;
    }
}