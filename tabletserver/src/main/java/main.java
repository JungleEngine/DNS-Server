import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import spark.Filter;
import spark.Request;
import spark.Response;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.concurrent.atomic.AtomicInteger;


import static java.lang.Thread.sleep;
import static spark.Spark.*;

public class main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    static Set<String> deleted_domains = new HashSet<>();
    static String first_domain = "";
    static String last_domain = "";
    static String MasterURL = "http://localhost:1234";
    static String master_IP="localhost";
    static Logger logger;
    static Boolean locked = false;
    public static void main(String[] argv) {

        port(5678);
        logger=getLogger();



        connectDB();

        DBManager.setInitialParameters(mongo, credential, database);
        DBManager db_manager = new DBManager();
        after((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET,POST");
            response.type("text/html");
            response.header("Content-Type","text/html");
        });

        before((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", null);
            response.header("Access-Control-Allow-Methods", "GET,POST");
        });

        //request initial data from master
        String initialData = sendGet(MasterURL+"/tablet/data");
        if(initialData!=null&&!initialData.equals("")) {
            System.out.println("successfully received data from master");
            logger.info("Successfully received initial from master");
            try {
                JSONParser JP = new JSONParser();
                JSONObject JO = (JSONObject) JP.parse(initialData);
                first_domain=(String)JO.get("first_domain");
                last_domain=(String)JO.get("last_domain");
                db_manager.fillInitialData((String)JO.get("data"));
            }catch (Exception e){
                System.out.println("failed to parse the response from master");
            }
        }else {
            System.out.println("got an empty response from master");
        }

        get("/3bhady",(request, response) -> {

            System.out.println("heeereee");
            response.status(200);
            response.body("ok");
            return response;
        });

        post("master/setrange", (Request request, Response response) -> {

            System.out.println("Set range");

            try {
                JSONParser JP = new JSONParser();
                JSONObject JO = (JSONObject) JP.parse(request.body());

                first_domain = (String) JO.get("first_domain");
                last_domain = (String) JO.get("last_domain");
                logger.info("Adjusted range of tablet first: "+first_domain+" last: "+last_domain);
                response.body("Received range done!");
                response.status(200);
            }catch (Exception e){
                response.body("couldn't parse data");
                response.status(500);
            }
            return response.body();
        });

        // Add entire row with n columns and m columns data.
        post("/client/addrow", (request, response) -> {

            if(locked.equals(true))
            {
                System.out.println("tablet locked");
                JSONObject obj = new JSONObject();
                obj.put("locked","true");
                response.body(obj.toJSONString());
                return response.body();
            }

            System.out.println("Add row");

            // Parser for request.body() to convert it to JSON.
            JSONParser JP = new JSONParser();

            // Get array of JSON objects.
            JSONArray JA = (JSONArray) JP.parse(request.body());

            String domain_name = "";

            // Walk through all objects in array.
            for (int i = 0; i < JA.size(); i++) {
                JSONObject JO = (JSONObject) JA.get(i);

                domain_name = (String) JO.get("domain_name");


                if(domain_name.compareTo(first_domain) < 0 || domain_name.compareTo(last_domain) > 0)
                {
                    logger.info("Requested domain is out of range for this tablet server. Sending client back to master");
                    JSONObject obj = new JSONObject();
                    obj.put("master_IP",master_IP);
                    response.body(obj.toJSONString());
                    return response.body();
                }

                String country = (String) JO.get("country");
                JSONArray IPs_object = (JSONArray) JO.get("IPs");

                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j < IPs_object.size(); j++) {
                    IPs.add((String) IPs_object.get(j));
                }

                logger.info("Added new row in tablet server: "+domain_name);

                locked = true;

                db_manager.addRow(domain_name, country, IPs);
                locked = false;
            }

            deleted_domains.remove(domain_name);

            JSONObject obj = new JSONObject();
            obj.put("row_added",1);
            response.body(obj.toJSONString());
            return response.body();
        });


        // Read domain info
        post("/client/readrow", (request, response) -> {


            if(locked.equals(true))
            {
                System.out.println("tablet locked");
                JSONObject obj = new JSONObject();
                obj.put("locked","true");
                response.body(obj.toJSONString());
                return response.body();
            }


            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject) JP.parse(request.body());

            String domain_name = (String) JO.get("domain_name");
            System.out.println("Read row with domain " + domain_name);


            if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0 )
            {
                System.out.println("out of range");
                //response.status(400);
                logger.info("Requested domain is out of range for this tablet server. Sending client back to master");
                JSONObject obj = new JSONObject();
                obj.put("master_IP",master_IP);
                response.body(obj.toJSONString());

                //response.body("Redirected to master");
                return response.body();
            }
            locked = true;
            List<Document> docs = db_manager.readRow(domain_name);

            logger.info("Read row from tablet server: "+domain_name);
            locked = false;

            String JSON = com.mongodb.util.JSON.serialize(docs);

            response.body(JSON);

            return response.body();
        });


        // Delete domain from DB
        post("/client/deleterow", (request, response) -> {

            if(locked.equals(true))
            {
                System.out.println("tablet locked");
                JSONObject obj = new JSONObject();
                obj.put("locked","true");
                response.body(obj.toJSONString());
                return response.body();
            }

            System.out.println("Delete row");

            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject) JP.parse(request.body());

            String domain_name = (String) JO.get("domain_name");


            if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
            {
                //response.status(400);
                //response.body("Redirected to master");
                logger.info("Requested domain is out of range for this tablet server. Sending client back to master");
                JSONObject obj = new JSONObject();
                obj.put("master_IP",master_IP);
                response.body(obj.toJSONString());
                return response.body();
            }
            locked = true;
            db_manager.deleteRow(domain_name);
            locked = false;
            deleted_domains.add(domain_name);

            JSONObject obj = new JSONObject();
            obj.put(" deleted row ",1);
            logger.info("Deleted Row: "+domain_name);
            response.body(obj.toJSONString());
            return response.body();
        });


        // Delete cells of certain domain and country.
        post("/client/deletecells", (request, response) -> {


            if(locked.equals(true))
            {
                System.out.println("tablet locked");
                JSONObject obj = new JSONObject();
                obj.put("locked","true");
                response.body(obj.toJSONString());
                return response.body();
            }

            System.out.println("Delete Cells");

            // Parser for request.body() to convert it to JSON.
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject) JP.parse(request.body());

            String domain_name = (String) JO.get("domain_name");


            if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
            {

//                response.status(400);
//                response.body("Redirected to master");
                logger.info("Requested domain is out of range for this tablet server. Sending client back to master");

                JSONObject obj = new JSONObject();
                obj.put("master_IP",master_IP);
                response.body(obj.toJSONString());
                return response.body();
            }

            String country = (String) JO.get("country");
            locked = true;
            db_manager.deleteCells(domain_name, country);

            logger.info("Deleted: "+country+" in: "+domain_name);

            locked = false;
            JSONObject obj = new JSONObject();
            obj.put(" delete cells ","true");
            response.body(obj.toJSONString());

            return response.body();

        });


        // Add row with n columns and n IPs
        post("/client/set","text/html", (request, response) -> {


            if(locked.equals(true))
            {
                System.out.println("tablet locked");
                JSONObject obj = new JSONObject();
                obj.put("locked","true");
                response.body(obj.toJSONString());
                return response.body();
            }

            System.out.println("Set");

            // Parser for request.body() to convert it t json.
            JSONParser JP = new JSONParser();

            // Get array of json objects.
            JSONArray JA = (JSONArray) JP.parse(request.body());

            // Walk through all objects in array.
            for (int i = 0; i < JA.size(); i++) {
                JSONObject JO = (JSONObject) JA.get(i);

                String domain_name = (String) JO.get("domain_name");


                if(domain_name.compareTo( first_domain) < 0 || domain_name.compareTo( last_domain) > 0)
                {
//                    response.status(400);
//                    response.body("Redirected to master");
                    logger.info("Requested domain is out of range for this tablet server. Sending client back to master");
                    JSONObject obj = new JSONObject();
                    obj.put("master_IP",master_IP);
                    response.body(obj.toJSONString());
                    return response.body();
                }

                String country = (String) JO.get("country");
                JSONArray IPs_object = (JSONArray) JO.get("IPs");

                List<String> IPs = new ArrayList<String>();

                for (int j = 0; j < IPs_object.size(); j++) {
                    IPs.add((String) IPs_object.get(j));
                }
                locked = true;
                db_manager.set(domain_name, country, IPs);
                locked = false;
            }

            logger.info("Set operation is successful.");


            JSONObject obj = new JSONObject();
            obj.put(" column  set ","true");
            response.body(obj.toJSONString());

            return response.body();
        });

        while(true) {
            try {
                sleep(20000);
                //TODO:send updates
                System.out.println("sending updates!");

                String docs = db_manager.getUpdatedDocuments();
                String deleted = JSON.serialize(Arrays.asList(deleted_domains.toArray()));

                if (docs == null || docs.equals(""))
                {
                    if(deleted==null||deleted.equals(""))
                    {
                        logger.info("Skipping sending updates as nothing has changed");
                        continue;
                    }
                }
                logger.info("Sending updates to Master");
                JSONObject data = new JSONObject();
                data.put("deleted", deleted);
                data.put("edited", docs);
                if (sendPost(MasterURL + "/update/tablets", data.toString()) != null) {
                    deleted_domains.clear();

                }
            } catch (Exception e) {

            }
        }
    }

    private static boolean inRange(String s){
        if(first_domain.equals(""))
            return (s.compareToIgnoreCase(last_domain)<=0);
        return (s.compareToIgnoreCase(first_domain)*s.compareToIgnoreCase(last_domain))<=0;
    }
    private static String sendGet(String url){
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("GET");
            con.setDoOutput(true);
            try {
                BufferedReader br;
                if (200 <= con.getResponseCode() && con.getResponseCode() <= 299) {
                    br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                } else {
                    br = new BufferedReader(new InputStreamReader(con.getErrorStream()));
                }
                br = new BufferedReader(new InputStreamReader((con.getInputStream())));
                StringBuilder sb = new StringBuilder();
                String output;
                while ((output = br.readLine()) != null) {
                    sb.append(output);
                }
                return sb.toString();
            } catch (Exception e) {
                System.out.println(e);
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    private static String sendPost(String url,String data){
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(data);
            wr.flush();
            wr.close();
            try {
                BufferedReader br;
                if (200 <= con.getResponseCode() && con.getResponseCode() <= 299) {
                    br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                } else {
                    br = new BufferedReader(new InputStreamReader(con.getErrorStream()));
                }
                br = new BufferedReader(new InputStreamReader((con.getInputStream())));
                StringBuilder sb = new StringBuilder();
                String output;
                while ((output = br.readLine()) != null) {
                    sb.append(output);
                }
                return sb.toString();
            } catch (Exception e) {
                System.out.println(e);
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    private static void connectDB() {
        try {

            MongoClientOptions.Builder clientOptions = new MongoClientOptions.Builder();
            clientOptions.connectionsPerHost(120);

            mongo = new MongoClient(new ServerAddress("localhost", 27017), clientOptions.build());
            //mongo = new MongoClient("localhost:27017?replicaSet=rs0&maxPoolSize=200", 27017);
            credential = MongoCredential.createCredential("", "tabletserver", "".toCharArray());
            database = mongo.getDatabase("tabletserver");
            collection = database.getCollection("dns");
            logger.info("Successfully connected to database");
        } catch (Exception e) {

            System.out.println("error connecting to database " + e.getMessage());

        }
    }
    private static Logger getLogger(){

        Logger logger = Logger.getLogger("MyLog");
        FileHandler fh;

        try {

            // This block configure the logger with handler and formatter
            fh = new FileHandler("TabletServer.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            // the following statement is used to log any messages
            logger.info("My first log");

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return logger;
    }

}
