package KPI_3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class dataReducer extends Reducer<Text,Text,Text,Text>{
	
	
	// here we are getting input from  ***movieDataMapper*** and ***ratingDataMapper***
	
	
	@Override
	public void reduce(Text key, Iterable<Text>values,Context context)throws IOException,InterruptedException
	{ 
		
	 //key                   value
	// movie-id           [ Adventure|comedy_movies , 23:1_ratings .... ]

		String genre = null;
		
		//for a given movie_id there can be only one genre and multiple users so
		//using list to store => age:occupation
		ArrayList<String> arr = new ArrayList<String>();
		
		
		for(Text val:values)
		{
			String []tokens = val.toString().split("_");
			
			if(tokens[1].equals("movie"))    //from movieDataMapper
			{
				genre = tokens[0];      //we must know the genre first to write the data
				
			 }
		
			else if(tokens[1].equals("ratings"))  //from ratingDataMapper
			{
				arr.add(tokens[0]);
				
			
			}
		
			
		}
		
		for(String val:arr)
		{
			
			String []splitAgain = val.split(":");
			String user_id = splitAgain[0];
			String rating = splitAgain[1];
			
			context.write(new Text(user_id), new Text(genre+"::"+rating));
		}
		
		
		
	
		
		
		
		
	

}
}
