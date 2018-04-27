import java.io.*;  
import java.net.*;  
public class client {  
	public static void main(String[] args) {  
		try{      
			Socket s=new Socket("localhost",4040);
			PrintWriter out = new PrintWriter(s.getOutputStream(), true);
BufferedReader in = new BufferedReader( new InputStreamReader(s.getInputStream()));

while(true)
	{
String  str = in.readLine();  // The server reads a message from the client
if(str!=null)
System.out.println("message= "+str);
}

		}catch(Exception e){
			System.out.println(e);
		}  
	}  
}
