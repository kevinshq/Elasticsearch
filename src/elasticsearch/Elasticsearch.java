package elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.json.JSONObject;

public class Elasticsearch {
	private String m_address;
	
	public Elasticsearch(String address) {
		m_address = address;
	}
	
	public boolean index(String index, String doc_type, JSONObject body, String id) {
		try {
			if (null == id || id.isEmpty()) {
				m_address = m_address+"/"+index+"/"+doc_type;
			} else {
				m_address = m_address+"/"+index+"/"+doc_type+"/"+id;
			}
			URL url;
			if (m_address.startsWith("http")) {
				url = new URL(m_address);
			} else {
				url = new URL("http://" + m_address);
			}
	        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	        connection.setRequestMethod("POST");
	        connection.setDoOutput(true);
	        connection.setRequestProperty("content-type", "application/json; charset=UTF-8");
	        connection.getOutputStream().write(body.toString().getBytes("UTF-8"));
	        int code = connection.getResponseCode();
	        switch (code) {
	        case HttpURLConnection.HTTP_OK:
	        case HttpURLConnection.HTTP_CREATED:
	            InputStream inputStream = connection.getInputStream();
	            ByteArrayOutputStream result = new ByteArrayOutputStream();
	            byte[] buffer = new byte[1024];
	            int length;
	            while ((length = inputStream.read(buffer)) != -1) {
	                result.write(buffer, 0, length);
	            }
//	            String content = result.toString("UTF-8");
//	    		System.out.println(content);
	            return true;
	        default:
	    		System.out.println("Response code: "+code);
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public boolean bulk(String index, String doc_type, List<JSONObject> actions) {
		try {
			String path = "/";
			if (index != null && index.length()>0) {
				path += index;
				path += "/";
				if (doc_type != null && doc_type.length()>0) {
					path += doc_type;
					path += "/";
				}
			}
			path += "_bulk";
			URL url;
			if (m_address.startsWith("http")) {
				url = new URL(m_address + path);
			} else {
				url = new URL("http://" + m_address + path);
			}
			System.out.println(url);
	        String body = "";
	        for (JSONObject action : actions) {
	        	body += action.toString() + "\n";
	        }
	        System.out.print(body);
	        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	        connection.setRequestMethod("POST");
	        connection.setDoOutput(true);
	        connection.setRequestProperty("content-type", "application/x-ndjson");
	        connection.getOutputStream().write(body.getBytes("UTF-8"));
	        int code = connection.getResponseCode();
	        switch (code) {
	        case HttpURLConnection.HTTP_OK:
	        case HttpURLConnection.HTTP_CREATED:
	            InputStream inputStream = connection.getInputStream();
	            ByteArrayOutputStream result = new ByteArrayOutputStream();
	            byte[] buffer = new byte[1024];
	            int length;
	            while ((length = inputStream.read(buffer)) != -1) {
	                result.write(buffer, 0, length);
	            }
	            String content = result.toString("UTF-8");
	    		System.out.println(content);
	            return true;
	        default:
	    		System.out.println("Response code: "+code);
	        }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
}
