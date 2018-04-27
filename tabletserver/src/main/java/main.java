import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import spark.Filter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static spark.Spark.*;

public class main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    static Set<String> deleted_domains = new HashSet<String>();
    static String first_domain = "";
    static String last_domain = "";

    public static void main (String[] argv)
    {

        port(5678);

        connectDB();
        DBManager.setInitialParameters(mongo, credential, database);
        DBManager db_manager = new DBManager();

        after((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET,POST");
        });

 

        // Connect with the master server.
        try{
            // Open a socket with master server
            Socket s = new Socket("localhost",4040);

            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));

            // Read data from Master
            String initialData = in.readLine();

            db_manager.fillInitialData(initialData);

            TimeUnit.SECONDS.sleep(30);

            out.println(deleted_domains);
            out.println(db_manager.getUpdatedDocuments());

        }catch(Exception e){
          e.printStackTrace();
        }
        get("/3bhady",(request, response) -> {
            System.out.println("heeereee");
            response.body("hello");
            return "Hello";
        });
        post("/master/setrange", (request, response) -> {

            System.out.println("Set range");

            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String first_domain = (String)JO.get("first_domain");
            String last_domain = (String)JO.get("last_domain");

            response.body("Received range!");
            return "Hello";
        });

        // Add entire row with n columns and m columns data.
        post("/client/addrow", (request, response) -> {

            System.out.println("Add row");

            // Parser for request.body() to convert it to JSON.
            JSONParser JP = new JSONParser();

            // Get array of JSON objects.
            JSONArray JA = (JSONArray)JP.parse(request.body());

            String domain_name = "";

            // Walk through all objects in array.
            for (int i = 0; i < JA.size(); i++)
            {
                JSONObject JO = (JSONObject)JA.get(i);

                domain_name = (String)JO.get("domain_name");

                if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
                {
                    response.status(400);
                    response.body("Redirected to master");
                    return response.body();
                }

                String country = (String)JO.get("country");
                JSONArray IPs_object= (JSONArray)JO.get("IPs");

                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j < IPs_object.size(); j++)
                {
                    IPs.add((String) IPs_object.get(j));
                }

                db_manager.addRow(domain_name, country, IPs);
            }

            deleted_domains.remove(domain_name);

            response.body("Added row successfully");
            return response.body();
        });


        // Read domain info
        post("/client/readrow", (request, response) -> {

            System.out.println("Read row");

            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");

            if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
            {
                response.status(400);
                response.body("Redirected to master");
                return response.body();
            }

            List<Document> docs = db_manager.readRow(domain_name);

            String JSON = com.mongodb.util.JSON.serialize(docs);

            response.body(JSON);

            return response.body();
        });


        // Delete domain from DB
        post("/client/deleterow", (request, response) -> {

            System.out.println("Delete row");

            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");

            if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
            {
                response.status(400);
                response.body("Redirected to master");
                return response.body();
            }

            db_manager.deleteRow(domain_name);

            deleted_domains.add(domain_name);

            response.body("Deleted successfully!");
            return response.body();
        });


        // Delete cells of certain domain and country.
        post("/client/deletecells", (request, response) -> {

            System.out.println("Delete Cells");

            // Parser for request.body() to convert it to JSON.
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");

            if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
            {
                response.status(400);
                response.body("Redirected to master");
                return response.body();
            }

            String country = (String)JO.get("country");

            db_manager.deleteCells(domain_name, country);

            response.body("Deleted successfully!");
            return response.body();

        });


        // Add row with n columns and n IPs
        post("/client/set", (request, response) -> {

            System.out.println("Set");

            // Parser for request.body() to convert it t json.
            JSONParser JP = new JSONParser();

            // Get array of json objects.
            JSONArray JA = (JSONArray)JP.parse(request.body());

            // Walk through all objects in array.
            for (int i = 0; i < JA.size(); i++)
            {
                JSONObject JO = (JSONObject)JA.get(i);

                String domain_name = (String)JO.get("domain_name");

                if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
                {
                    response.status(400);
                    response.body("Redirected to master");
                    return response.body();
                }

                String country = (String)JO.get("country");
                JSONArray IPs_object= (JSONArray)JO.get("IPs");

                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j<IPs_object.size(); j++)
                {
                    IPs.add((String) IPs_object.get(j));
                }
                db_manager.set(domain_name, country, IPs);
            }

            response.body("Updated successfully!");
            return response.body();
        });

    }

    private static void connectDB() {
        try {

            MongoClientOptions.Builder clientOptions = new MongoClientOptions.Builder();
            clientOptions.connectionsPerHost(120);

            mongo = new MongoClient(new ServerAddress("localhost",27017),clientOptions.build());
            //mongo = new MongoClient("localhost:27017?replicaSet=rs0&maxPoolSize=200", 27017);
            credential = MongoCredential.createCredential("", "tabletserver", "".toCharArray());
            database = mongo.getDatabase("tabletserver");
            collection = database.getCollection("dns");

        }catch(Exception e){

            System.out.println("error connecting to database "+e.getMessage());

        }
    }

}
