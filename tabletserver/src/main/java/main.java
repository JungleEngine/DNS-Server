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
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import spark.Filter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static spark.Spark.after;
import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.route.HttpMethod.after;

public class main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;

    public static void main (String[] argv)
    {

        port(5678);

        connectDB();
        DBManager.setInitialParameters(mongo, credential, database);

        DBManager db_manager = new DBManager();

        after((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET");
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

            //s.close();  //close the server socket

        }catch(Exception e){
          e.printStackTrace();
        }


        // Add entire row with n columns and m columns data.
        post("/client/addrow", (request, response) -> {

            System.out.println("Add row");

            // Parser for request.body() to convert it to JSON.
            JSONParser JP = new JSONParser();

            // Get array of JSON objects.
            JSONArray JA = (JSONArray)JP.parse(request.body());

            // Walk through all objects in array.
            for (int i = 0; i < JA.size(); i++)
            {
                JSONObject JO = (JSONObject)JA.get(i);

                String domain_name = (String)JO.get("domain_name");
                String country = (String)JO.get("country");
                JSONArray IPs_object= (JSONArray)JO.get("IPs");

                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j<IPs_object.size(); j++)
                {
                    IPs.add((String) IPs_object.get(j));
                }

                db_manager.addRow(domain_name, country, IPs);
            }

            response.body("Added row successfully");
            return response.body();
        });


        // Read domain info
        post("/client/readrow", (request, response) -> {

            System.out.println("Read row");

            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");

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

            db_manager.deleteRow(domain_name);

            response.body("Deleted successfully!");
            return response.body();
        });


        // Delete cells of certain domain and country.
        post("/client/deletecells", (request, response) -> {

            System.out.println("Delete Cells");

            // Parser for request.body() to convert it to JSON.
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain = (String)JO.get("domain_name");
            String country = (String)JO.get("country");

            db_manager.deleteCells(domain, country);

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
