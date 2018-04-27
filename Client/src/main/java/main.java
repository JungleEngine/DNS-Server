import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class main {
    private static String master_IP = "192.21.543.34:1234";
    private static String tablet_IP = "192.21.543.32:4567";
    private static String request_IP;

    public static void main(String[] args) {

        request_IP = tablet_IP;

        while (true) {


            try {

                URL url = new URL("http://"+ request_IP + "/client/addrow");
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("POST");

                ArrayList<String> IPs = new ArrayList<>();
                IPs.add("124.1328.452.3");
                IPs.add("124.1328.452.2");

                JSONObject data = new JSONObject();
                data.put("domain_name", "123.com");
                data.put("country", "Egypt");
                data.put("IPs", IPs);

                con.setDoOutput(true);
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(data.toString());
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
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
