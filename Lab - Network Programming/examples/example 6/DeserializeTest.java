//test reading the stream again   
   import java.io.*;  
    class DeserializeTest{  
     public static void main(String args[])throws Exception{  
        
      ObjectInputStream in=new ObjectInputStream(new FileInputStream("f.txt"));  
      //we cast the bytes back to its original typpe
	  Student s=(Student)in.readObject();  
      System.out.println(s.id+" "+s.name);  
      
      in.close();  
     }  
    }  