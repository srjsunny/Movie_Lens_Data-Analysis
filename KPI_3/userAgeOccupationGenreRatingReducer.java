package KPI_3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// data from ***userGenreRatingMapper  and ***userDataMapper



public class userAgeOccupationGenreRatingReducer extends Reducer<Text,Text,Text,Text> {

	//key              values
	//user_id         [ genre::rating_file2   ,  age::occupation_file1 .....]
	
	
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{
		String age = null;
		String occupation = null;
		
		
		//for a user_id , there can be only one age::occupation and multiple genres::rating
		//ArrayList to store => genre::rating
		ArrayList<String> arr2 = new ArrayList<String>();
		
		for(Text val:values)
		{
			String []tokens = val.toString().split("_");
			String file = tokens[1];
			
			
			
			if(file.equals("file1"))  //means data from userDataMapper
			{
				String []splitAgain = tokens[0].split("::");
				age = splitAgain[0];       
				occupation = splitAgain[1];
				
			}
			
			else if(file.equals("file2"))  //means data from userGenreRatingMapper
			{
				
				arr2.add(tokens[0]);
				
			}
			
			
		}
		
		
		
		for(String val:arr2)
		{   
			String []splitAgain2 = val.toString().split("::");
			String genre = splitAgain2[0];
			String rating = splitAgain2[1];
		
			
			context.write(new Text(age+"::"+occupation+"::"+genre), new Text(rating));
		
		}
		
		
		
		
	}
	
	
	
	
	
}
