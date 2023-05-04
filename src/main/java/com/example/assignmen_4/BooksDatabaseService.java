package com.example.assignmen_4;/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <YOUR STUDENT ID HERE>
 *
 */

import java.io.*;
//import java.io.OutputStreamWriter;

import java.net.Socket;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
    //Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.



public class BooksDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private CachedRowSet crs = null;
    private String[] requestStr  = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome   = null;

    private InputStream inputStream = null;

    private DataInputStream dataInputStream = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public BooksDatabaseService(Socket aSocket){

        serviceSocket = aSocket;
        
		//TO BE COMPLETED
		
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library
		
		String tmp;
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(serviceSocket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            tmp = bufferedReader.readLine();
            tmp = tmp.substring(0, tmp.length()-1);
            String[] splitter = tmp.split(";");
            if(splitter.length == 2){
                this.requestStr[0] = splitter[0];
                this.requestStr[1] = splitter[1];
            }
         }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);

        }
        return this.requestStr;
    }

    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;
		String sql = " SELECT title, genre, publisher, rrp, COUNT(bookcopy.copyid) AS copies FROM author INNER JOIN book ON author.authorid = book.authorid INNER JOIN bookcopy ON book.bookid = bookcopy.bookid INNER JOIN library ON bookcopy.libraryid = library.libraryid WHERE familyname = ? AND city = ? GROUP BY publisher, rrp, genre, title;"; //TO BE COMPLETED- Update this line as needed.
		
		
		try {
			//Connet to the database
			//TO BE COMPLETE

            Connection conn = DriverManager.getConnection(Credentials.URL, Credentials.USERNAME, Credentials.PASSWORD);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, this.requestStr[0]);
            stmt.setString(2, this.requestStr[1]);
            ResultSet rs = stmt.executeQuery();
			//Make the query
			//TO BE COMPLETED
            RowSetFactory aFactory = RowSetProvider.newFactory();
            crs = aFactory.createCachedRowSet();

            crs.populate(rs);
//            while(crs.next()){
//                System.out.println(crs.getString(2));
//            };
            this.outcome = crs;


            System.out.println();
            stmt.close();

			//Process query
			//TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.

			//Clean up
			//TO BE COMPLETED
			
		} catch (Exception e)
		{ System.out.println(e); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            OutputStream outputStream = serviceSocket.getOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(this.outcome);

            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);

        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }

        //Terminating connection of the service socket
			//TO BE COMPLETED

    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
