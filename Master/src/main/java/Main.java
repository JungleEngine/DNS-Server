import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class Main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;

    public static void main(String[] argv) {

        connectDB();





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
    private static void FillDatabase(){

        String fileName = "data.txt";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line;
            ArrayList<UpdateOneModel<Document>> documents = new ArrayList<UpdateOneModel<Document>>();
            Map<String, TreeMap<String, ArrayList<String>>> mp = new TreeMap<>();

            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(" ");
                //domain name
                String domainParts[] = parts[0].split("\\.");
                StringBuilder domainName = new StringBuilder();
                for (int i = domainParts.length - 1; i >= 0; i--) {
                    domainName.append(domainParts[i]);
                    if (i != 0)
                        domainName.append(".");
                }
                String ip = parts[1];

                StringBuilder country = new StringBuilder(parts[2]);
                for (int i = 3; i < parts.length; i++) {
                    country.append(" ").append(parts[i]);
                }


                mp.computeIfAbsent(domainName.toString(), k -> new TreeMap<String, ArrayList<String>>());
                mp.get(domainName.toString()).computeIfAbsent(country.toString(), k -> new ArrayList<String>());

                mp.get(domainName.toString()).get(country.toString()).add(ip.toString());
                Document document = new Document("countries", Arrays.asList(new Document("country", country.toString())
                        .append("IPs", Arrays.asList(ip))));

                //documents.add(new UpdateOneModel<>(new Document("domain_name",domainName.toString()),
                //      new Document("$set",document)));
            }

            for (Map.Entry<String, TreeMap<String, ArrayList<String>>> Domains : mp.entrySet()) {
                Document document = new Document("domain_name", Domains.getKey());
                ArrayList<Document> countries = new ArrayList<>();
                for (Map.Entry<String, ArrayList<String>> Countries : Domains.getValue().entrySet()) {
                    countries.add(new Document("country", Countries.getKey()).append("IP", Countries.getValue()));
                }
                document.append("countries", countries);
                collection.insertOne(document);
            }


            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void connectDB() {
        try {
            collection = database.getCollection("dns");
            MongoClientOptions.Builder clientOptions = new MongoClientOptions.Builder();
            clientOptions.connectionsPerHost(120);

            mongo = new MongoClient(new ServerAddress("localhost", 27017), clientOptions.build());
            //mongo = new MongoClient("localhost:27017?replicaSet=rs0&maxPoolSize=200", 27017);
            credential = MongoCredential.createCredential("", "mp2", "".toCharArray());
            database = mongo.getDatabase("mp2");

        } catch (Exception e) {

            System.out.println("error connecting to database " + e.getMessage());

        }
    }

}