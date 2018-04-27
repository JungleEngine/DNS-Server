//source https://systembash.com/a-simple-java-udp-server-and-udp-client/
 /**
     * Application method to run the server runs in an infinite loop
     * listening on port 9876.  The server waits to receive a message
	 * then it converts it to capital letters then send it back to the client
	 * 
	 * This example is a connection-less example, that's mean that each time 
	 * we send a packet, we need to specify the destination IPAddress and port number,
	 * whether we are sending from the client to server or the opposite
     */

import java.io.*;
import java.net.*;

class UDPServer
{
   public static void main(String args[]) throws Exception
      {
		  //create a server socket with port 9876, so it could listen on it
         DatagramSocket serverSocket = new DatagramSocket(9876);
            byte[] receiveData = new byte[1024];
            byte[] sendData = new byte[1024];
            while(true)
               {
				  //create a container to receive the data in it.
                  DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                  //wait till it receives the data
				  serverSocket.receive(receivePacket);
				  //get the data and print it
                  String sentence = new String( receivePacket.getData());
                  System.out.println("RECEIVED: " + sentence);
                  
				  //this is a connection-less approach so in order to reply back to the 
				  //client, we need to get its IPaddress and port number
				  InetAddress IPAddress = receivePacket.getAddress();
                  int port = receivePacket.getPort();
				  
				  //prepare data
                  String capitalizedSentence = sentence.toUpperCase();
                  sendData = capitalizedSentence.getBytes();
                  DatagramPacket sendPacket =
                  new DatagramPacket(sendData, sendData.length, IPAddress, port);
                  
				  //send data
				  serverSocket.send(sendPacket);
               }
      }
}

