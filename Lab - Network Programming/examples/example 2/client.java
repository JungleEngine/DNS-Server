import java.io.*;  
import java.net.*;

/**
 * A simple client for the capitalization server.
 */
public class client {

    public static void main(String[] args) throws Exception {
        //Connect to server
        Socket socket = new Socket("localhost", 9898);
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
            String response = in.read();
            System.out.println("Capitalized message = "+ response + "\n");
        }
        socket.close();
    }
}