package bigdatacourse.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JSONExample {

	public static void main(String[] args) throws IOException{
		FileReader fs = new FileReader("C://Users//User//OneDrive//Desktop//cs classes//big_data//hw2-src (1)//hw2-src//src//bigdatacourse//example//data//meta_Office_Products.json");
		BufferedReader br = new BufferedReader(fs);
		try{
			String line = br.readLine();
			JSONTokener tokener = new JSONTokener(line);
			JSONObject json = new JSONObject(tokener);
			System.out.println(json.getString("asin"));
		}catch(IOException e){
			System.out.println(e.getMessage());
		}
		fs.close();
		

		
		
		

		/*  you will find here a few examples to handle JSON.Org
		System.out.println("you will find here a few examples to handle JSON.org");
		 
		creating object examples
		JSONObject json		=	new JSONObject();								// initialize empty object
		json				=	new JSONObject("{\"phone\":\"05212345678\"}");	// initialize from string
		
		adding attributes
		json.put("street", "Einstein");
		json.put("number", 3);
		json.put("city", "Tel Aviv");
		System.out.println(json);					// prints single line
		System.out.println(json.toString(4));		// prints "easy reading"	
		
		adding inner objects
		JSONObject main = new JSONObject();
		main.put("address", json);
		main.put("name", "Rubi Boim");
		System.out.println(main.toString(4));
		
		adding array (1)
		JSONArray views = new JSONArray();
		views.put(1);
		views.put(2);
		views.put(3);
		main.put("views-simple", views);
		
		// adding array (2)
		JSONArray viewsExtend = new JSONArray();
		viewsExtend.put(new JSONObject().put("movieName", "American Pie").put("viewPercentage", 72));
		viewsExtend.put(new JSONObject().put("movieName", "Top Gun").put("viewPercentage", 100));
		viewsExtend.put(new JSONObject().put("movieName", "Bad Boys").put("viewPercentage", 87));
		main.put("views-extend", viewsExtend);
		
		System.out.println(main);
		System.out.println(main.toString(4));
		 */ 
		
	}

}
