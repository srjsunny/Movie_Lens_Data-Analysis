package KPI_3;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// input data from *****dataReducer*****


public class userGenreRatingMapper extends Mapper<Object,Text,Text,Text> {

	//data format => user_id     genre::rating    (tab delimited)
	
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		
		String []tokens = value.toString().split("\t");    
		String user_id  = tokens[0];
		
		
		context.write(new Text(user_id), new Text(tokens[1]+"_file2"));
		              //user_id             genre::rating      
		
		
		
		
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
}
