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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static spark.Spark.get;
import static spark.Spark.post;

public class main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;

    public static void main (String[] argv)
    {
        // Connect with the master server.
        try{
            // Open a socket with master server
            Socket s = new Socket("localhost",4040);

            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            BufferedReader in = new BufferedReader( new InputStreamReader(s.getInputStream()));

            // Read data from Master
            String str = in.readLine();
            if(str != null)
                System.out.println("Message: " + str);

            System.out.println("Received data from master");

            //TODO: Insert data into DB

            //s.close();  //close the server socket

        }catch(Exception e){
            System.out.println(e);
        }


        connectDB();
        DBManager.setInitialParameters(mongo, credential, database);

        DBManager db_manager = new DBManager();


//        get("/client/addrow/:domain_name/:country/:", (request, response) -> {
//
//
//            String[] strings = new String[]{"192.16","4454.3"};
//            List<Document> docs =  db_manager.readRows("facebook.com");
//            return request.queryParams("name");
//            //return "Hello: " + request.params(":name") + "\n" + docs.toString();
//
//        });

        post("/client/addrow", (request, response) -> {
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");
            String country = (String)JO.get("country");
            JSONArray IPs_object= (JSONArray)JO.get("IPs");

            List<String> IPs = new ArrayList<String>();

            for (int i = 0; i < IPs_object.size(); i++) {
                IPs.add((String) IPs_object.get(i));
            }

            db_manager.addRow(domain_name, country, IPs);

            //TODO: Check if row was not added
            response.body("Row added successfully!");
            return response.body();

        });

        post("/client/deleterow", (request, response) -> {
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());
            String domain_name = (String)JO.get("domain_name");

            db_manager.deleteRow(domain_name);

            //TODO: Check row exists?!
            response.body("Row deleted successfully!");
            return response.body();
        });

        post("/client/deletecells", (request, response) -> {
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");
            String country = (String)JO.get("country");
            JSONArray IPs_object= (JSONArray)JO.get("IPs");

            List<String> IPs = new ArrayList<String>();

            for (int i = 0; i < IPs_object.size(); i++) {
                IPs.add((String) IPs_object.get(i));
            }

            db_manager.deleteCells(domain_name);

            //TODO: Check row exists?!
            response.body("Row deleted successfully!");
            return response.body();
        });



        post("/client/readrow", (request, response) -> {
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");
            String country = (String)JO.get("country");

            List<Document> docs = db_manager.readRows(domain_name);

            //TODO: Check if row does not exist

            String json = com.mongodb.util.JSON.serialize(docs);
            response.body(json);

            return response.body();
        });


        post("/client/set", (request, response) -> {
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject)JP.parse(request.body());

            String domain_name = (String)JO.get("domain_name");
            String country = (String)JO.get("country");
            JSONArray IPs_object= (JSONArray)JO.get("IPs");

            List<String> IPs = new ArrayList<String>();

            for (int i = 0; i < IPs_object.size(); i++) {
                IPs.add((String) IPs_object.get(i));
            }

            List<Document> docs = db_manager.readRows(domain_name);

            //TODO: Check if row does not exist
            if (docs.isEmpty())
                response.body("Does not exist!");
            else {
                String json = com.mongodb.util.JSON.serialize(docs);
                response.body(json);
            }
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

        }catch(Exception e){

            System.out.println("error connecting to database "+e.getMessage());

        }
    }

}
