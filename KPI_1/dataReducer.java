package KPI_1;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class dataReducer extends Reducer<LongWritable,Text,Text,LongWritable>{
	
	
	// here we are getting input from  ***movieDataMapper*** and ***userDataMapper***
	
	
	
	@Override
	public void reduce(LongWritable key, Iterable<Text>values,Context context)throws IOException,InterruptedException
	{ 
		
		//key(movie_id)          values
		//234                  [ 1, ToyStory,1,1,1,1......]
		long count = 0;
		String movie_name = null;
		for(Text val:values)
		{
			String token = val.toString();
			
			if(token.equals("1"))   //means data from userDataMapper
			{
				count++;
				
			}
			
			else
			{
				movie_name = token;   //means data from movieDataMapper;
			}
			
			
		}
		
		context.write(new Text(movie_name), new LongWritable(count));
		
		
		
	}
	
	
	

}
