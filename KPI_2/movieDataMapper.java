package KPI_2;

import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class movieDataMapper extends Mapper <Object,Text,LongWritable,Text>{
	
	//data format => MovieID::Title::Genres
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split("::");
		
		long movie_id = Long.parseLong(tokens[0]);
		
		String name = tokens[1];
		
		//appending  "_movie" with name to distinguish later in reducer
		name +="_movie";                 
		
		context.write(new LongWritable(movie_id), new Text(name));
		              //movie_id                  name
	}
	
	
	
	
	
	
	

}
