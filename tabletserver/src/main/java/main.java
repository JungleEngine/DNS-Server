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


        try {
            Socket socket = new Socket("192.168.1.10", 4040);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            socket.close();

        }catch (Exception e)
        {
            e.printStackTrace();
        }


        connectDB();
        DBManager.setInitialParameters(mongo, credential, database);

        DBManager db_manager = new DBManager();


        get("/client/addrow/:domain_name/:country/:", (request, response) -> {


            String[] strings = new String[]{"192.16","4454.3"};
            List<Document> docs =  db_manager.readRows("facebook.com");
            return request.queryParams("name");
            //return "Hello: " + request.params(":name") + "\n" + docs.toString();

        });

        post("/client/addrow", (request, response) -> {
            JSONParser json_parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) json_parser.parse(request.body());
            String domain_name = (String) jsonObject.get("domain_name");
            String country = (String) jsonObject.get("country");
            JSONArray IPs_object= (JSONArray) jsonObject.get("IPs");
            List<String> IPs = new ArrayList<String>();

            for (int i=0; i<IPs_object.size(); i++) {
                IPs.add((String) IPs_object.get(i));
            }

            db_manager.addRow(domain_name, country, IPs);
            System.out.println("user");
            response.body("abosamra");
            return response.body();

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
