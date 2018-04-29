import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.json.simple.JSONObject;

import javax.print.Doc;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.Executors;
//
//import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;
//import static java.lang.Thread.sleep;

public class TabletHandler implements Runnable{
    Socket socket;
    PrintWriter out;
    BufferedReader in;
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    static String LastElement="";//last element in the last tablet to get domain names after it
    static String SparkPort="5678"; //port for sending to tablet servers their ranges
    public TabletHandler(Socket _socket,MongoClient _mongo,MongoCredential _credential,MongoDatabase _database){

        mongo=_mongo;
        credential=_credential;
        database=_database;
        socket=_socket;
        connectDB();
        try {

             out = new PrintWriter(socket.getOutputStream(), true);
            System.out.println("here here");
             in = new BufferedReader( new InputStreamReader(socket.getInputStream()));
            System.out.println("here here");
            //TODO:
        }catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
    }
static {
    //FindIterable<Document> list = collection.find();
}
    public void run() {

        int count =(int)collection.count();

        //Bson filter = Filters.gt("domain_name","")
        //send half of data
        //TODO: send half of the words
        //TODO: send the other tablets other elements
        Bson filter = Filters.gt("domain_name", LastElement);
        FindIterable<Document> list = collection.find(filter).limit(2);


        String first=null;
        String last=null;
        for(Document doc : list)
        {
            if(first==null)
            first = (String)doc.get("domain_name");
            else
            LastElement = last = (String)doc.get("domain_name");
        }
        System.out.println(first+" "+last);


        JSONObject data = new JSONObject();
        data.put("first_domain", first);
        data.put("last_domain", last);
        String url="http://0.0.0.0:"+SparkPort+"/master/setrange";
        System.out.println(url);
        SendPost(url,data.toString());
//        try {
//            sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        System.out.println("Data sent");
        out.println(JSON.serialize(list));
       // String url="http://localhost:"+SparkPort+"/3bhady";
        //String url="http://0.0.0.0:"+SparkPort+"/master/setrange";


        //TODO: send range for the tablet server on it's endpoint
//        Executors.newCachedThreadPool().execute(() -> {
//            try {
//                in.readLine();
//            }catch(Exception e){
//
//            }
//        });


        //TODO: wait for updates
        //Updates will be rows that i delete and reinsert
    }

    private static void sendMessage(String msg,ObjectOutputStream out)
    {
        try{
            out.writeUTF(msg);
            out.flush();
            System.out.println("server>" + msg);
        }
        catch(IOException ioException){
            ioException.printStackTrace();
        }
    }

    private static void SendPost(String url,String param) {
//        try {
//            //String url = "http://localhost:5678/3bhady";
//
//            URL obj = new URL(url);
//            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
//
//            // optional default is GET
//            con.setRequestMethod("POST");
//
//            //add request header
//            con.setRequestProperty("User-Agent", "Mozilla/5.0");
//
//            int responseCode = con.getResponseCode();
//            System.out.println("\nSending 'GET' request to URL : " + url);
//            System.out.println("Response Code : " + responseCode);
//
//            BufferedReader in = new BufferedReader(
//                    new InputStreamReader(con.getInputStream()));
//            String inputLine;
//            StringBuffer response = new StringBuffer();
//
//            while ((inputLine = in.readLine()) != null) {
//                response.append(inputLine);
//            }
//            in.close();
//
//            //print result
//            System.out.println(response.toString());
//        }catch (Exception e){
//            e.printStackTrace();
//        }
        try {
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "text/html");
            //con.setRequestProperty("Accept", "text/html");
            //con.addRequestProperty("Content-Type", "text/html");


            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(param);
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
                System.out.println(sb.toString());
            } catch (Exception e) {
                System.out.println(e);
            }
            // OutputStream os = con.getOutputStream();
            // os.write(param.getBytes());
            // os.flush();
            // os.close();
//            BufferedReader in = new BufferedReader(
//                    new InputStreamReader(con.getInputStream()));
//            String inputLine;
//            StringBuilder response = new StringBuilder();
//
//            while ((inputLine = in.readLine()) != null) {
//                response.append(inputLine);
//            }
//            in.close();
//            // print result
//            System.out.println(response.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    private static void connectDB() {
        try {
            collection = database.getCollection("dns");
        } catch (Exception e) {
            System.out.println("error connecting to database " + e.getMessage());
        }
    }
    @Override
    protected void finalize() throws Throwable {
        try{
            in.close();
            out.close();
            socket.close();
        }
        catch(IOException ioException){
            ioException.printStackTrace();
        }
    }


}
