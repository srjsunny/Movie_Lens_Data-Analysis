package KPI_2;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class dataReducer extends Reducer<LongWritable,Text,Text,DoubleWritable>{
	
	
	// here we are getting input from  ***movieDataMapper*** and ***ratingDataMapper***
	
	
	@Override
	public void reduce(LongWritable key, Iterable<Text>values,Context context)throws IOException,InterruptedException
	{ 
		
		//key(movie_id)          values
		//234                  [  Avengers_movie, 5_ratings , 4_ratings ........]
		
		double sum = 0.0;
		long count = 0;
		double avg = 0.0;
		
		String movie_name = null;
		
		for(Text val:values)
		{
			String []token = val.toString().split("_");
			
			//token[0] = Avengers or 5,4,....
			//token[1] = movie or ratings
			
			
			
			if(token[1].equals("ratings"))   //means data from ratingDataMapper
			{
				sum+= Long.parseLong(token[0]);
				count++;
			}
			
			else if(token[1].equals("movie"))
			{
				movie_name = token[0];   //means data from movieDataMapper;
			}
			
			
		}
		
		// if viewed by more than 40 people
		if(count >= 40)  
		{
			avg = sum/count;
			
			context.write(new Text(movie_name), new DoubleWritable(avg));
						// movie_name               average_rating
		}
	
	
	

}
}
