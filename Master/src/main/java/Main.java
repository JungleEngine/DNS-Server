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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import static spark.Spark.get;

public class Main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;

    public static void main (String[] argv)
    {

        connectDB();
        MongoCollection<Document> collection;
        collection = database.getCollection("dns");

        String fileName="data.txt";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            List<Document> documents = new ArrayList<Document>();
            while((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(" ");
                //domain name
                String domainParts[] = parts[0].split("\\.");
                StringBuilder domainName = new StringBuilder();
                for(int i=domainParts.length-1;i>=0;i--)
                {
                    domainName.append(domainParts[i]);
                    if(i!=0)
                        domainName.append(".");
                }
                String ip=parts[1];

                StringBuilder country= new StringBuilder(parts[2]);
                for(int i=3;i<parts.length;i++)
                {
                    country.append(" ").append(parts[i]);
                }





                Document doc1 = new Document("name", "Amarcord Pizzeria")
                        .append("contact", new Document("phone", "264-555-0193")
                                .append("email", "amarcord.pizzeria@example.net")
                                .append("location",Arrays.asList(-73.88502, 40.749556)))
                        .append("stars", 2)
                        .append("categories", Arrays.asList("Pizzeria", "Italian", "Pasta"));


                documents.add(doc1);
            }
            collection.insertMany(documents);
            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
         catch (Exception e) {
            e.printStackTrace();
        }
        //collection.updateOne(Filters.eq("domain_name",))

//        get("/connect/:domain", (request, response) -> {
//
//            // Table used in search.
//            MongoCollection<Document> collection;
//
//            collection = database.getCollection("dns");
//
//            collection.updateOne(Filters.eq("domain_name",  "twitter.com"),
//                    Updates.set("domain_name", request.params(":name") ));
//            return "Hello: " + request.params(":name");
//
//        });
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