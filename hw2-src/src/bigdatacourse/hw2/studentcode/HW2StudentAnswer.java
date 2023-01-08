package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API{
	
	// general consts
	public static final String		NOT_AVAILABLE_VALUE 	=		"na";
	public static final int         NOT_AVALIBLE_RATING		=		  -1;
	private final ReentrantLock lock = new ReentrantLock();
	// CQL stuff
	//TODO: add here create table and query designs 
	
	private static final String		TABLE_ITEM_BY_ASIN = "item_by_asin";
	private static final String		TABLE_ITEMREVIEWS_BY_ASIN = "itemReviews_by_asin";
	private static final String		TABLE_USER_REVIEWS_BY_REVIEWER_ID = "userReviews_by_reviewerID";
	
		
	private static final String		CQL_CREATE_ITEM_BY_ASIN = 
			"CREATE TABLE " + TABLE_ITEM_BY_ASIN  +"(" 		+ 
				"asin text,"				+
				"title text,"				+
				"image text,"				+
				"catagories set<text>,"		+
				"description text,"			+
				"PRIMARY KEY (asin)"		+
			")";				
			
	private static final String		CQL_CREATE_USER_REVIEWS_BY_REVIEWER_ID =
			"CREATE TABLE " + TABLE_USER_REVIEWS_BY_REVIEWER_ID +"(" 		+ 
				"reviewerID text,"				+
				"ts timestamp,"					+
				"asin text,"					+
				"reviewerName text,"			+
				"rating int,"					+
				"summary text,"					+
				"reviewText text,"				+
				"PRIMARY KEY ((reviewerID), ts, asin)"		+
			") "							+
			"WITH CLUSTERING ORDER BY (ts DESC, asin DESC)";


	private static final String		CQL_CREATE_ITEMREVIEWS_BY_ASIN =
			"CREATE TABLE " + TABLE_ITEMREVIEWS_BY_ASIN 	+"(" 		+ 
				"reviewerID text,"				+
				"ts timestamp,"					+
				"asin text,"					+
				"reviewerName text,"			+
				"rating int,"					+
				"summary text,"					+
				"reviewText text,"				+
				"PRIMARY KEY ((asin), ts , reviewerID)"		+
			") "							+
			"WITH CLUSTERING ORDER BY (ts DESC, reviewerID DESC)";
	

	
	private static final String		CQL_ITEM_BY_ASIN_SELECT = 
		"SELECT * FROM " + TABLE_ITEM_BY_ASIN + " WHERE asin = ?";
	
	private static final String		CQL_REVIEWS_BY_REVIEWERID_SELECT = 
		"SELECT * FROM " + TABLE_USER_REVIEWS_BY_REVIEWER_ID + " WHERE reviewerID = ?";
	
	private static final String		CQL_REVIEWS_BY_ASIN_SELECT = 
		"SELECT * FROM " + TABLE_ITEMREVIEWS_BY_ASIN + " WHERE asin = ?";
	
	private static final String		CQL_ITEM_BY_ASIN_INSERT = 
		"INSERT INTO " + TABLE_ITEM_BY_ASIN + "(asin, title, image, catagories, description) VALUES(?,?,?,?,?)";
					
	private static final String		CQL_REVIEWS_BY_REVIEWERID_INSERT = 
		"INSERT INTO " + TABLE_USER_REVIEWS_BY_REVIEWER_ID + "(reviewerID, ts, asin, reviewerName, rating, summary, reviewText) VALUES(?,?,?,?,?,?,?)";
	
	private static final String		CQL_REVIEWS_BY_ASIN_INSERT = 
		"INSERT INTO " + TABLE_ITEMREVIEWS_BY_ASIN + "(reviewerID, ts, asin, reviewerName, rating, summary, reviewText) VALUES(?,?,?,?,?,?,?)";
	
	
			
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	private PreparedStatement psAddItemAsin;  
	private PreparedStatement psAddReviewsReviewerID;
	private PreparedStatement psAddReviewsAsin;
	private PreparedStatement psSelectItemAsin;
	private PreparedStatement psSelectReviewsReviewerID;
	private PreparedStatement psSelectReviewsAsin;
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	

	
	@Override
	public void createTables() {
		try{
			session.execute(CQL_CREATE_ITEM_BY_ASIN);
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		System.out.println("created table: " + TABLE_ITEM_BY_ASIN);
		try{
			session.execute(CQL_CREATE_USER_REVIEWS_BY_REVIEWER_ID);
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		try{
			session.execute(CQL_CREATE_ITEMREVIEWS_BY_ASIN);
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		System.out.println("created table: " + TABLE_ITEMREVIEWS_BY_ASIN);
		
	
	}

	@Override
	public void initialize() {
		psAddItemAsin = session.prepare(CQL_ITEM_BY_ASIN_INSERT);
		psAddReviewsAsin = session.prepare(CQL_REVIEWS_BY_ASIN_INSERT);
		psAddReviewsReviewerID = session.prepare(CQL_REVIEWS_BY_REVIEWERID_INSERT);
		
		psSelectItemAsin = session.prepare(CQL_ITEM_BY_ASIN_SELECT);
		psSelectReviewsAsin = session.prepare(CQL_REVIEWS_BY_ASIN_SELECT);
		psSelectReviewsReviewerID = session.prepare(CQL_REVIEWS_BY_REVIEWERID_SELECT);
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		loadFileWithTreads(pathItemsFile, "items", "items");
		System.out.println("done insert data to + " + TABLE_ITEM_BY_ASIN);
		
	}


	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		loadFileWithTreads(pathReviewsFile, "reviews", "review by reviewer");
		System.out.println("done insert data to " + TABLE_USER_REVIEWS_BY_REVIEWER_ID);
		
		loadFileWithTreads(pathReviewsFile, "reviews", "review by asim");
		System.out.println("done insert data to " + TABLE_ITEMREVIEWS_BY_ASIN);
	}

	@Override
	public void item(String asin) {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		// required format - example for asin B005QB09TU
		System.out.println("asin: " 		+ "B005QB09TU");
		System.out.println("title: " 		+ "Circa Action Method Notebook");
		System.out.println("image: " 		+ "http://ecx.images-amazon.com/images/I/41ZxT4Opx3L._SY300_.jpg");
		System.out.println("categories: " 	+ new TreeSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper")));
		System.out.println("description: " 	+ "Circa + Behance = Productivity. The minute-to-minute flexibility of Circa note-taking meets the organizational power of the Action Method by Behance. The result is enhanced productivity, so you'll formulate strategies and achieve objectives even more efficiently with this Circa notebook and project planner. Read Steve's blog on the Behance/Levenger partnership Customize with your logo. Corporate pricing available. Please call 800-357-9991.");;
		
		// required format - if the asin does not exists return this value
		System.out.println("not exists");
	}
	
	
	@Override
	public void userReviews(String reviewerID) {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		
		// required format - example for asin A17OJCRPMYWXWV
		System.out.println(	
				"time: " 			+ Instant.ofEpochSecond(1362614400) + 
				", asin: " 			+ "B005QDG2AI" 	+
				", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
				", reviewerName: " 	+ "Old Flour Child"	+
				", rating: " 		+ 5 	+ 
				", summary: " 		+ "excellent quality"	+
				", reviewText: " 	+ "These cartridges are excellent .  I purchased them for the office where I work and they perform  like a dream.  They are a fraction of the price of the brand name cartridges.  I will order them again!");

		System.out.println(	
				"time: " 			+ Instant.ofEpochSecond(1360108800) + 
				", asin: " 			+ "B003I89O6W" 	+
				", reviewerID: " 	+ "A17OJCRPMYWXWV" 	+
				", reviewerName: " 	+ "Old Flour Child"	+
				", rating: " 		+ 5 	+ 
				", summary: " 		+ "Checkbook Cover"	+
				", reviewText: " 	+ "Purchased this for the owner of a small automotive repair business I work for.  The old one was being held together with duct tape.  When I saw this one on Amazon (where I look for almost everything first) and looked at the price, I knew this was the one.  Really nice and very sturdy.");

		System.out.println("total reviews: " + 2);
	}

	@Override
	public void itemReviews(String asin) {
		//TODO: implement this function
		System.out.println("TODO: implement this function...");
		
		
		// required format - example for asin B005QDQXGQ
		System.out.println(	
				"time: " 			+ Instant.ofEpochSecond(1391299200) + 
				", asin: " 			+ "B005QDQXGQ" 	+
				", reviewerID: " 	+ "A1I5J5RUJ5JB4B" 	+
				", reviewerName: " 	+ "T. Taylor \"jediwife3\""	+
				", rating: " 		+ 5 	+ 
				", summary: " 		+ "Play and Learn"	+
				", reviewText: " 	+ "The kids had a great time doing hot potato and then having to answer a question if they got stuck with the &#34;potato&#34;. The younger kids all just sat around turnin it to read it.");

		System.out.println(	
				"time: " 			+ Instant.ofEpochSecond(1390694400) + 
				", asin: " 			+ "B005QDQXGQ" 	+
				", reviewerID: " 	+ "AF2CSZ8IP8IPU" 	+
				", reviewerName: " 	+ "Corey Valentine \"sue\""	+
				", rating: " 		+ 1 	+ 
				", summary: " 		+ "Not good"	+
				", reviewText: " 	+ "This Was not worth 8 dollars would not recommend to others to buy for kids at that price do not buy");

		System.out.println(	
				"time: "			+ Instant.ofEpochSecond(1391299200) + 
				", asin: " 			+ "B005QDQXGQ" 	+
				", reviewerID: " 	+ "A27W10NHSXI625" 	+
				", reviewerName: " 	+ "Beth"	+
				", rating: " 		+ 2 	+ 
				", summary: " 		+ "Way overpriced for a beach ball"	+
				", reviewText: " 	+ "It was my own fault, I guess, for not thoroughly reading the description, but this is just a blow-up beach ball.  For that, I think it was very overpriced.  I thought at least I was getting one of those pre-inflated kickball-type balls that you find in the giant bins in the chain stores.  This did have a page of instructions for a few different games kids can play.  Still, I think kids know what to do when handed a ball, and there's a lot less you can do with a beach ball than a regular kickball, anyway.");

		System.out.println("total reviews: " + 3);
	}

	private void readJSONFile(BufferedReader br, String line, String dataFormatName , String table) throws Exception {
			while(true){
				lock.lock();
				line = br.readLine();
				if(line == null) {
					lock.unlock();
					break;
				}
				JSONTokener tokener = new JSONTokener(line);
				JSONObject json = new JSONObject(tokener);

				switch (dataFormatName) {
					case "items":
						
						String asin;
						String title;
						String image;
						TreeSet<String> categories;
						String description;
					
					
						try{
							asin = json.getString("asin");
						}
						catch(JSONException e){
							asin = NOT_AVAILABLE_VALUE; 
						}
						try{
							title = json.getString("title");
						}
						catch(JSONException e){
							title = NOT_AVAILABLE_VALUE; 
						}
						try{
							image = json.getString("imUrl");
						}
						catch(JSONException e){
							image = NOT_AVAILABLE_VALUE; 
						}
						try{
							Iterator<Object> iterator = json.getJSONArray("categories").getJSONArray(0)   .iterator();
							categories = new TreeSet<>();
							while (iterator.hasNext()) {
								categories.add((String) iterator.next());
							}

							
						}
						catch(JSONException e){
							categories = new TreeSet<>();
						} 
						try{
							description = json.getString("description");
						}
						catch(JSONException e){
							description = NOT_AVAILABLE_VALUE; 
						}	
						
						insertItemByAsin(asin, title, image, categories, description);
						
						break;
					
					case "reviews":
					Instant time;
					String reviewerID;
					int rating;
					String summary;
					String reviewText;
					String reviewerName;	
					try{
						time = Instant.ofEpochSecond(json.getLong("unixReviewTime"));
					}
					catch(JSONException e){
						time = null; 
					}
					try{
						asin = json.getString("asin");
					}
					catch(JSONException e){
						asin = NOT_AVAILABLE_VALUE; 
					}
					try{
						reviewerID = json.getString("reviewerID");
					}
					catch(JSONException e){
						reviewerID = NOT_AVAILABLE_VALUE; 
					}
					try{
						rating = json.getInt("overall");	
					}
					catch(JSONException e){
						rating = NOT_AVALIBLE_RATING;
					} 
					try{
						summary = json.getString("summary");
					}
					catch(JSONException e){
						summary = NOT_AVAILABLE_VALUE; 
					}
					try{
						reviewText = json.getString("reviewText");
					}
					catch(JSONException e){
						reviewText = NOT_AVAILABLE_VALUE; 
					}
					try{
						reviewerName = json.getString("reviewerName");
					}catch(JSONException e){
						reviewerName = NOT_AVAILABLE_VALUE;
					}
						switch(table){
							case "review by reviewer":
								insertReviewByReviwer(asin, time, reviewerID, rating, summary, reviewText, reviewerName);
								break;
							case "review by asim":
								insertReviewByAsin(asin, time, reviewerID, rating, summary, reviewText, reviewerName);
								break;	
						}
						break;
				}
				
			}
		 
	}
	private void insertReviewByAsin(String asin, Instant time, String reviewerID, int rating, String summary,
			String reviewText, String reviewerName) {	
			
			BoundStatement bsAddReviewsAsin = psAddReviewsAsin.bind()
				.setString(0, reviewerID)
				.setInstant(1, time)
				.setString(2, asin)
				.setString(3, reviewerName)
				.setInt(4, rating)
				.setString(5, summary)
				.setString(6, reviewText);

			lock.unlock();
			session.execute(bsAddReviewsAsin);
	}


	private void insertReviewByReviwer(String asin, Instant time, String reviewerID, int rating, String summary,
			String reviewText, String reviewerName) {
			
			BoundStatement bsAddReviewsReviewerID = psAddReviewsReviewerID.bind()
				.setString(0, reviewerID)
				.setInstant(1, time)
				.setString(2, asin)
				.setString(3, reviewerName)
				.setInt(4, rating)
				.setString(5, summary)
				.setString(6, reviewText);

			lock.unlock();
			session.execute(bsAddReviewsReviewerID);
	}


	private void insertItemByAsin(String asin, String title,String image, TreeSet<String> categories,String description){
		BoundStatement bsAddItemAsin = psAddItemAsin.bind()
			.setString(0, asin)
			.setString(1, title)
			.setString(2, image)
			.setSet(3, categories, String.class)
			.setString(4, description);

			lock.unlock();
			session.execute(bsAddItemAsin);
	}

	private void loadFileWithTreads(String dataFile, String fileName, String table) throws Exception {
		int maxThreads = 100;
		int count = 200;
		FileReader fr = new FileReader(dataFile);
		BufferedReader br = new BufferedReader(fr);
		String line = null;
		
		
		// creating the thread factors
		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		for(int i = 0;i < count; i++){
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try{
						readJSONFile(br, line, fileName, table);			
					}catch(Exception e){
						System.out.println(e.getMessage());
						return;
					}
				}
			});
		}
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);

		br.close();
		fr.close();
	}



}
