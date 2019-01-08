package KPI_3;

import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class movieDataMapper extends Mapper <Object,Text,Text,Text>{
	
	//data from movie.dat
	
	//data format => MovieID::Title::Genres
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split("::");
		
		String movie_id = tokens[0];
		
		String genre = tokens[2].trim();
		
		context.write(new Text(movie_id), new Text(genre+"_movie"));
		
		
		
		
		
	}
	
	
	
	
	
	
	

}
