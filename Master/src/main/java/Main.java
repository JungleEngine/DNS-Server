import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import spark.Filter;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static spark.Spark.*;

public class Main {
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    static String LastElement="";
    static String SparkPort="5678"; //port for sending to tablet servers their ranges
    static Map<String,Range>tablets= new TreeMap<>();
    static Logger logger;
    static public class Range{
        Range(String s,String e){
            start=s;
            end=e;
        }
        boolean inRange(String s) {
            /*
                System.out.println(s1.compareTo(s2));//0
                System.out.println(s1.compareTo(s3));//1(because s1>s3)
                System.out.println(s3.compareTo(s1));//-1(because s3 < s1 )
             */
            //bigger than start and smaller than the end
            System.out.println("s:"+s);
            System.out.println("start:"+start);
            System.out.println("end:"+end);
            System.out.println("s.compareToIgnoreCase(start):"+s.compareToIgnoreCase(start));
            System.out.println("s.compareToIgnoreCase(end)"+s.compareToIgnoreCase(end));
                if(start.equals(""))
                    return (s.compareToIgnoreCase(end)<=0);

            return (s.compareToIgnoreCase(start)*s.compareToIgnoreCase(end))<=0;
        }
        boolean isBefore(String s){
            //less than start or than end
            //this is used to adjust the range if a new element occurs

            if(end.equals("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"))
                return true;
            return (s.compareToIgnoreCase(start)<=0 || s.compareToIgnoreCase(end)<=0);
        }
        public String start="";
        public String end="";
    }
    public static void main(String[] argv) {
        logger = getLogger();
        connectDB();

        port(1234);

        after((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET,POST");
        });

        before((Filter) (request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET,POST");
        });

        //JSONObject data = new JSONObject();
        //            data.put("first_domain", "hey");
        //            data.put("last_domain", "hello");

        //TODO: return IP of the server regarding the request
//        get("/connect/:domain", (request, response) -> {
//
//
//            return "Hello: " + request.params(":name");
//
//        });

        post("update/tablets",((request, response) -> {
            logger.info("Updating Master Table data from tablet server "+request.ip());
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject) JP.parse(request.body());
            String deleted = (String) JO.get("deleted");
            String edited = (String) JO.get("edited");

            JSONArray deletedJSON = (JSONArray) JP.parse(deleted);
            JSONArray editedJSON = (JSONArray) JP.parse(edited);

            for (int i = 0 ; i < editedJSON.size(); ++i)
            {
                JSONObject temp = (JSONObject)editedJSON.get(i);
                try {
                    //TODO:is this over writing or not???
                    collection.insertOne(Document.parse(temp.toString()));
                }catch(Exception e)
                {

                }
            }
            for (int i = 0 ; i < deletedJSON.size(); ++i)
            {
                JSONObject temp = (JSONObject)deletedJSON.get(i);
                try {
                    collection.deleteOne(new Document("domain_name",temp.toString()));
                }catch(Exception e)
                {

                }
            }

            return "ok";
        }));
        //collection.updateOne(Filters.eq("domain_name",))
        post("connect", (request, response) -> {

            System.out.println(request.body());
            JSONParser JP = new JSONParser();
            JSONObject JO = (JSONObject) JP.parse(request.body());
            String domain = (String) JO.get("domain_name");
            System.out.println(domain);
            logger.info("Client requesting access to tablet server containing: "+domain);

            for(Map.Entry<String,Range> entry : tablets.entrySet()) {
                String ip = entry.getKey();
                Range range = entry.getValue();

                if(range.inRange(domain))
                {
                    logger.info("Returning tablet server with range matching requested domain name");
                    //send ip of this tablet server
                    response.status(200);
                    response.body(ip+":"+SparkPort);
                    return response.body();
                }
            }
            for(Map.Entry<String,Range> entry : tablets.entrySet()) {
                String ip = entry.getKey();
                Range range = entry.getValue();

                if(range.isBefore(domain))
                {
                    logger.info("Increasing the range of: "+ip+" with new range: "+domain);
                    entry.setValue(new Range(domain,range.end));
                    //adjust range of this tablet server
                    //if this doesn't make the client adjust the range of the tablet server
                    JSONObject data = new JSONObject();
                    data.put("first_domain", domain);
                    data.put("last_domain", range.end);
                    System.out.println(sendPost("http://"+ip+":"+SparkPort+"/master/setrange",data));
                    //send ip of this tablet server
                    response.status(200);
                    response.body(ip+":"+SparkPort);
                    return response.body();
                }
            }
             response.body("what is what is?");
            System.out.println("what is what is?");
            response.status(333);
            return response.body();
        });


        get("/tablet/data", (request, response) -> {

            int count =(int)collection.count();
            logger.info("Tablet server requesting initial data");
            //Bson filter = Filters.gt("domain_name","")
            //send half of data
            //TODO: send half of the words
            //TODO: send the other tablets other elements
            Bson filter = Filters.gt("domain_name", LastElement);
            FindIterable<Document> list = collection.find(filter).limit(count/2);

            String first=null;
            String last=null;
            for(Document doc : list) {
                if(first==null)
                    first = (String)doc.get("domain_name");
                else
                     last = (String)doc.get("domain_name");
            }

            if(LastElement.equals("")) {
                first = "";//to make the first tablet accept any domains before it
                LastElement=last;
            }
            else {
                //because this is only for two tablets
                LastElement = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
            }
            System.out.println(first+" "+last);

            System.out.println("ip is:"+request.ip());
            tablets.put(request.ip(),new Range(first,last));
            JSONObject data = new JSONObject();
            data.put("first_domain", first);
            data.put("last_domain", last);
            data.put("data",JSON.serialize(list));
//            JSONObject data = new JSONObject();
//            data.put("first_domain", first);
//            data.put("last_domain", last);
//
//            String url="http://0.0.0.0:"+SparkPort+"/master/setrange";
//            System.out.println(url);
//            sendPost(url,data);
            response.body(data.toString());
            response.status(200);
            return response.body();
        });

    }
    private static Logger getLogger(){

        Logger logger = Logger.getLogger("MyLog");
        FileHandler fh;

        try {

            // This block configure the logger with handler and formatter
            fh = new FileHandler("Master.log");
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
    private static String sendPost(String url,JSONObject data){
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "text/html");
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(data.toString());
            wr.flush();
            wr.close();
            try {
                BufferedReader br;
                System.out.println(con.getResponseCode());
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
    private static void FillDatabase(){

        String fileName = "data.txt";
        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            class MyDomainComp implements Comparator<String> {

                @Override
                public int compare(String s1, String s2) {

                    String temp1 = s1.replace("\\.","");
                    String temp2 = s2.replace("\\.","");
                    return temp1.compareToIgnoreCase(temp2);
                }
            }
            String line;
            ArrayList<UpdateOneModel<Document>> documents = new ArrayList<UpdateOneModel<Document>>();
            Map<String, TreeMap<String, ArrayList<String>>> mp = new TreeMap<String, TreeMap<String, ArrayList<String>>>(new MyDomainComp());

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
                document.append("countries", countries).append("dirty_bit","0");
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

            MongoClientOptions.Builder clientOptions = new MongoClientOptions.Builder();
            clientOptions.connectionsPerHost(120);

            mongo = new MongoClient(new ServerAddress("localhost", 27017), clientOptions.build());
            //mongo = new MongoClient("localhost:27017?replicaSet=rs0&maxPoolSize=200", 27017);
            credential = MongoCredential.createCredential("", "mp2", "".toCharArray());
            database = mongo.getDatabase("mp2");
            collection = database.getCollection("dns");
            logger.info("Connected to database");
        } catch (Exception e) {

            System.out.println("error connecting to database " + e.getMessage());

        }
    }

}