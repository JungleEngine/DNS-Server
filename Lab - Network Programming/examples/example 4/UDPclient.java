import java.io.*;
import java.net.*;

class UDPClient
{
   public static void main(String args[]) throws Exception
   {
	   //read input from user
      BufferedReader inFromUser =
         new BufferedReader(new InputStreamReader(System.in));
	  String sentence = inFromUser.readLine(); 
	  
	  //get the ipaddress of the server
	  InetAddress IPAddress = InetAddress.getByName("localhost");
	  //prepare the socket
      byte[] sendData = new byte[1024];
      byte[] receiveData = new byte[1024];
      sendData = sentence.getBytes();
	   //create packet
      DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, 9876);
     
	  //we create the socket to send the data using it,
	  // notice that we didn't give the socket a port number contrary to what we did in the server
	  // that is because we let the OS determine the port number
	  //think why didn't we do that in the server as well??
	  DatagramSocket clientSocket = new DatagramSocket();
      
	  //send the packet using the socket
      clientSocket.send(sendPacket);
	  
	  //prepare another packet container to receive the reply from the server
      DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
      
	  //wait to receive the reply
	  clientSocket.receive(receivePacket);
      
	  //print the reply
	  String modifiedSentence = new String(receivePacket.getData());
      System.out.println("FROM SERVER:" + modifiedSentence);
	  
	  //close client socket
      clientSocket.close();
   }
}
