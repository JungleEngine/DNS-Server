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

import javax.print.Doc;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Int;

public class TabletHandler implements Runnable{
    Socket socket;
    PrintWriter out;
    BufferedReader in;
    static MongoClient mongo;
    static MongoCredential credential;
    static MongoDatabase database;
    static MongoCollection<Document> collection;
    static String LastElement="";//last element in the last tablet to get domain names after it

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
        FindIterable<Document> list = collection.find(filter).limit(count/2);


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
        out.println(JSON.serialize(list));


        System.out.println("Data sent");
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
