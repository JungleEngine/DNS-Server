	//source http://www.javatpoint.com/serialization-in-java#serializable
	//This example change a student object to a stream of bytes and write it in a file
		import java.io.*;

		import java.io.Serializable;

		//implements the serializable interface
		class Student implements Serializable{
		//all objects needs to be serializable as well
		//all primitive types are serializable by default
		 int id;
		 String name;
		 public Student(int id, String name) {
		  this.id = id;
		  this.name = name;
		 }
		}	
		
		//test serialization
		class SerializeTest{  
		 public static void main(String args[])throws Exception{  
		  //create object
		  Student s1 =new Student(211,"Movy");  
		  //create file which we will write the stream of bytes in it
		  FileOutputStream fout=new FileOutputStream("f.txt");  
		  //create an object outstream to write the stream in the file
		  ObjectOutputStream out=new ObjectOutputStream(fout);  
		  // write the object
		  out.writeObject(s1);  
		  //flush to make sure it is written
		  out.flush();  
		  out.close();
		  System.out.println("success");  
		 }  
		}  