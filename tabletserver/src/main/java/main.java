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

/*
        try {
            Socket socket = new Socket("192.168.1.10", 4040);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            // Consume the initial welcoming messages from the server
            System.out.println(in.readLine());

            BufferedReader consolereader = new BufferedReader (new InputStreamReader(System.in));
            String message = "";
            while(true)
            {
                System.out.println("Enter a message in lower case or a period '.' to quit");
                message = consolereader.readLine();
                out.println(message); //send message to server
                if(message.equals("."))
                    break;
                String response = in.readLine();
                System.out.println("Capitalized message = "+ response + "\n");
            }
            socket.close();
        }catch (Exception e)
        {
            e.printStackTrace();
        }
*/

        connectDB();
        DBManager.setInitialParameters(mongo, credential, database);

        DBManager db_manager = new DBManager();

        // Delete entire row.
        post("/client/deleterow", (request, response) -> {

            JSONParser json_parser = new JSONParser();
            JSONObject object = (JSONObject) json_parser.parse(request.body());
            String domain = (String) object.get("domain");
            db_manager.deleteRow(domain);
            return response.body();

        });

        post("/client/readrow", (request, response) -> {

            JSONParser json_parser = new JSONParser();
            JSONObject object = (JSONObject) json_parser.parse(request.body());
            String domain = (String) object.get("domain");
            List<Document> rows = db_manager.readRows(domain);
            return response.body();

        });

        // Add row with n columns and n IPs
        post("/client/set", (request, response) -> {

            // Parser for request.body() to convert it t json.
            JSONParser json_parser = new JSONParser();

            // Get array of json objects.
            JSONArray jsonArray = (JSONArray)json_parser.parse(request.body());

            // Walk through all objects in array.
            for (int i = 0; i < jsonArray.size(); i++)
            {
                JSONObject jsonObject = (JSONObject) jsonArray.get(i);

                String domain_name = (String) jsonObject.get("domain");
                String country = (String) jsonObject.get("country");
                JSONArray IPs_object= (JSONArray) jsonObject.get("IPs");
                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j<IPs_object.size(); j++)
                {

                    IPs.add((String) IPs_object.get(j));

                }


                db_manager.set(domain_name, country, IPs);
                System.out.println("set_row");
            }

            response.body("abosamra");
            return response.body();

        });

        // Delete cells of certain domain and country.
        post("/client/deletecells", (request, response) -> {

            // Parser for request.body() to convert it t json.
            JSONParser json_parser = new JSONParser();
            JSONObject object = (JSONObject) json_parser.parse(request.body());
            String domain = (String) object.get("domain");
            String country = (String) object.get("country");
            db_manager.deleteCells(domain, country);
            return response.body();

        });

        // Add entire row with n columns and m columns data.
        post("/client/addrow", (request, response) -> {

            // Parser for request.body() to convert it t json.
            JSONParser json_parser = new JSONParser();

            // Get array of json objects.
            JSONArray jsonArray = (JSONArray)json_parser.parse(request.body());

            // Walk through all objects in array.
            for (int i = 0; i < jsonArray.size(); i++)
            {
                JSONObject jsonObject = (JSONObject) jsonArray.get(i);

                String domain_name = (String) jsonObject.get("domain");
                String country = (String) jsonObject.get("country");
                JSONArray IPs_object= (JSONArray) jsonObject.get("IPs");
                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j<IPs_object.size(); j++)
                {

                    IPs.add((String) IPs_object.get(j));

                }


                db_manager.addRow(domain_name, country, IPs);
                System.out.println("user");
            }

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
