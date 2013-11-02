package edu.ucla.cs.cs144;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;

public class Indexer {
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }
 
    public void rebuildIndexes() {

        Connection conn = null;

        // create a connection to the database to retrieve Items from MySQL
	try {
	    conn = DbManager.getConnection(true);
	} catch (SQLException ex) {
	    System.out.println(ex);
	}


	/*
	 * Add your code here to retrieve Items using the connection
	 * and add corresponding entries to your Lucene inverted indexes.
         *
         * You will have to use JDBC API to retrieve MySQL data from Java.
         * Read our tutorial on JDBC if you do not know how to use JDBC.
         *
         * You will also have to use Lucene IndexWriter and Document
         * classes to create an index and populate it with Items data.
         * Read our tutorial on Lucene as well if you don't know how.
         *
         * As part of this development, you may want to add 
         * new methods and create additional Java classes. 
         * If you create new classes, make sure that
         * the classes become part of "edu.ucla.cs.cs144" package
         * and place your class source files at src/edu/ucla/cs/cs144/.
	 * 
	 */
	
	// Create the index
	IndexWriter indexWriter = new IndexWriter("index-directory", new StandardAnalyzer(), true);

	// Create a statement for our queries
	Statement stmt = conn.createStatement();
	
	// Create variables for our index creation
	String bar, beer;
	float price;
	
	float id;
	String name;
	String description;
	float buyPrice;
	Date ends;
	
	String seller;
	String categories;
	
	String content;

	// Execute the query
	ResultSet rs = stmt.executeQuery("SELECT id, name, description, buy_price, ends FROM Item");

	// Process each result
	while (rs.next()) {

		// Reset the concatenated 'categories' string
		categories = "";
		
		// Set up sub statement and result set variables
		Statement subStmt = conn.createStatement();
		ResultSet sellerRs, categoryRs;
		
		// Grab the ID for use in sub queries and other fields
		id = rs.getFloat("id");
		name = rs.getString("name");
		description = rs.getString("description");
		buyPrice = rs.getFloat("buy_price");
		ends = rs.getDate("ends");
		
		// For each item, the seller
		sellerRs = subStmt.executeQuery("SELECT uid FROM ItemSeller WHERE iid = " + id);
		while(sellerRs.next())
		{
			seller = sellerRs.getString("uid");
		}
		
		// For each item, we need all the categories
		categoryRs = subStmt.executeQuery("SELECT category FROM ItemCategory WHERE iid = " + id);
		while(categoryRs.next())
		{
			categories += categoryRs.getString("category");
		}
		
		content = name + categories + description;
		
		//item name, category, seller, buy price, bidder, ending time, and description, content
		
		// Create a document for this item
		Document doc = new Document();
		
		// Add fields to the document
		doc.add(new Field("id", id, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("name", name, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("description", description, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("buyPrice", buyPrice, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("ends", ends, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("categories", categories, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("seller", seller, Field.Store.YES, Field.Index.TOKENIZED));
		doc.add(new Field("content", content, Field.Store.YES, Field.Index.TOKENIZED));
		
		
		// Write the document
		indexWriter.addDocument(doc);
	}
	
	// Close the index
	indexWriter.close();

    // close the database connection
	try {
	    conn.close();
	} catch (SQLException ex) {
	    System.out.println(ex);
	}
    }    

    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
