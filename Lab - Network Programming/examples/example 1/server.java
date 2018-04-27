import java.io.*;  
import java.net.*;  
public class server {  

		try{  
			ServerSocket ss=new ServerSocket(6666);  //create a server socket with port 6666
			System.out.println("Server is started and waiting for clients");  
			Socket s=ss.accept();//server waits a client to send a request. when the connection request reaches, it establishes connection and returns the socket object that will be used for communication with the client
			BufferedReader in = new BufferedReader( new InputStreamReader(s.getInputStream())); //what does this line do? by now you should already know.
			String  str = in.readLine();  // The server reads a message from the client
			System.out.println("message= "+str);  
			ss.close();  //close the server socket
		}catch(Exception e){
			System.out.println(e);
		}  
	}  
}  